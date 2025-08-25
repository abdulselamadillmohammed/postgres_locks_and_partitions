# postgres_locks_and_partitions

A small laboratory to **reproduce and observe PostgreSQL lock-manager behavior** on highly partitioned tables with many indexes.  
Inspired by:

- Blog (Kyle Hailey): **“Postgres Partition Pains – LockManager Waits”**  
  https://www.kylehailey.com/post/postgres-partition-pains-lockmanager-waits
- Video (Hussein Nasser): **“They Enabled Postgres Partitioning and their Backend fell apart”**  
   https://www.youtube.com/watch?v=YPorP8BsF_c

> **Purpose.** Make it easy to see how **partition count × index count × query shape** multiplies relation locks, pushes sessions beyond the **16 fast-path relation-lock slots**, and produces visible **LockManager** contention—and how behavior differs between **PostgreSQL 13** and **PostgreSQL 14+**.

---

## 1) Background and key ideas

- Every query in PostgreSQL takes **relation locks** on the parent table, on the **partitions it may touch**, and on the **indexes of those partitions**.
- With many partitions and ~20+ indexes per partition, a single query can hold **hundreds of locks**. Once a backend needs **more than 16** relation locks, it spills past the fast-path into the central lock manager, where **LWLock: LockManager** contention can appear under load.
- **PostgreSQL 13 behavior.** Even if a query was logically constrained to a single partition, it often still acquired locks on many partitions and their indexes, inflating lock counts dramatically (as described in the article/video).
- **PostgreSQL 14+ behavior.** Partition pruning and locking are improved; queries more often lock **only the partition(s) actually touched**. Contention can still be reproduced by using **unbounded scans** (no partition predicate), **a large number of partitions/indexes**, **high concurrency**, and/or **DDL** executed during load.

**Approximate lock fan-out (simplified)**

```

locks_per_query = partitions_touched × (1 table + N indexes) + 1 parent_table

```

Example: 10 partitions × (1 + 22) = **230** relation locks.

---

## 2) Requirements

- PostgreSQL **14+ recommended** (the lab still runs on 13; behavior will be more extreme there).
- Python 3.11+ and the packages in `requirements.txt`.
- A database where you can create tables and extensions.

**Check server version**

```bash
psql "$DATABASE_URL" -XAtc "show server_version; show server_version_num;"
# or
psql "postgresql://USER:PASS@HOST:5432/DB" -c "select version();"
```

---

## 3) Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create `.env` at the repository root (adjust as needed):

```env
# Connection (psycopg2 driver)
DATABASE_URL=postgresql+psycopg2://pi:strong_password@127.0.0.1:5432/pg_partition_lab

# Schema
SCHEMA=public

# Data window (END_DATE is exclusive)
START_DATE=2025-01-01
END_DATE=2025-09-01

# Data volume
ROWS=100000
BATCH_SIZE=2000

# Partitioning
PARTITION_GRAIN=day      # day | week
DUMMY_INDEXES=0          # additional partial indexes per partition to inflate lock counts

# Workload (tuned for small hardware; increase gradually)
POOL_SIZE=6
MAX_OVERFLOW=6
CONCURRENCY=8
READ_RATIO=0.6
UPDATE_RATIO=0.3
INSERT_RATIO=0.1

# Monitoring
KILL_BLOCKERS=false
```

---

## 4) Create schema, load data, and run

```bash
# 1) Create parent and partitions
python src/schema_partitioned.py --init --create-partitions

# 2) Load synthetic data
python src/generate_data.py

# 3) Ensure UUID helper used by inserts exists
psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"

# 4) Drive a workload (mixed reads/updates/inserts for 30 seconds)
python src/run_workload.py --mode mixed --seconds 30

# 5) Observe locks in another terminal
python src/monitor_locks.py
```

**Other workload modes**

```bash
python src/run_workload.py --mode unbounded-range --seconds 45   # scans without partition pruning
python src/run_workload.py --mode pruned-range    --seconds 45   # queries constrained by order_time
python src/run_workload.py --mode lookup          --seconds 20
python src/run_workload.py --mode update          --seconds 20
python src/run_workload.py --mode insert          --seconds 20
```

---

## 5) Observability queries

**Fast-path vs regular locks per PID**

```sql
SELECT count(*) AS cnt, pid, mode, fastpath
FROM pg_locks
WHERE fastpath IS NOT NULL
GROUP BY pid, mode, fastpath
ORDER BY pid, mode, fastpath;
```

**Blockers and waiters (who is blocking whom)**

```sql
WITH waits AS (
  SELECT w.pid AS waiter_pid, b.pid AS blocker_pid, w.locktype, w.mode,
         w.relation::regclass AS rel, w.transactionid
  FROM pg_locks w
  JOIN pg_locks b
    ON b.locktype = w.locktype
   AND b.database  = w.database
   AND COALESCE(b.relation,w.relation) IS NOT DISTINCT FROM w.relation
   AND COALESCE(b.page,w.page) IS NOT DISTINCT FROM w.page
   AND COALESCE(b.tuple,w.tuple) IS NOT DISTINCT FROM w.tuple
   AND COALESCE(b.virtualxid,w.virtualxid) IS NOT DISTINCT FROM w.virtualxid
   AND COALESCE(b.transactionid,w.transactionid) IS NOT DISTINCT FROM w.transactionid
   AND COALESCE(b.classid,w.classid) IS NOT DISTINCT FROM w.classid
   AND COALESCE(b.objid,w.objid) IS NOT DISTINCT FROM w.objid
   AND COALESCE(b.objsubid,w.objsubid) IS NOT DISTINCT FROM w.objsubid
  WHERE NOT w.granted AND b.granted
)
SELECT w.waiter_pid, w.blocker_pid, w.rel, w.transactionid,
       now() - sa_blocker.xact_start AS blocker_xact_age,
       left(sa_blocker.query,120) AS blocker_query
FROM waits w
JOIN pg_stat_activity sa_blocker ON sa_blocker.pid = w.blocker_pid
ORDER BY blocker_xact_age DESC
LIMIT 10;
```

**Partition tree and bounds**

```sql
SELECT t.relid::regclass AS partition,
       pg_get_expr(c.relpartbound, c.oid) AS bounds
FROM pg_partition_tree('public.orders') t
JOIN pg_class c ON c.oid = t.relid
WHERE t.isleaf
ORDER BY 1
LIMIT 20;
```

**Data window sanity**

```sql
SELECT min(order_time) AS min_order_time, max(order_time) AS max_order_time
FROM public.orders;
```

---

## 6) Reproducing significant lock-manager pressure

To emulate the article’s scenario (large lock counts per query):

1. **Increase partitions and indexes.**

   - Extend the date window (e.g., `END_DATE=2026-01-01`) to create many daily partitions.
   - Increase `DUMMY_INDEXES` (e.g., `8` or more) to add per-partition partial indexes.
   - Re-run: `python src/schema_partitioned.py --create-partitions`

2. **Favor unpruned scans.**

   - `python src/run_workload.py --mode unbounded-range --seconds 45`
   - This maximizes the number of partitions and indexes locked per statement.

3. **Optionally execute DDL during load** (laboratory only).

   - Example: `CREATE INDEX CONCURRENTLY` on the parent; even “concurrent” DDL adds pressure and catalog invalidations.

4. **Partition detach tests (PG14+).**

   - `ALTER TABLE public.orders DETACH PARTITION CONCURRENTLY public.orders_YYYY_MM_DD;`
   - The `CONCURRENTLY` variant reduces the severity of parent-table locking compared to a plain `DETACH`.

---

## 7) Cleanup

```bash
psql "$DATABASE_URL" -c "DROP TABLE IF EXISTS public.orders CASCADE;"
```

---

## 8) References

- Kyle Hailey — **Postgres Partition Pains – LockManager Waits**
  [https://www.kylehailey.com/post/postgres-partition-pains-lockmanager-waits](https://www.kylehailey.com/post/postgres-partition-pains-lockmanager-waits)
- Hussein Nasser — **Postgres Partition Pains – LockManager Waits Explained**
  [https://www.youtube.com/watch?v=YPorP8BsF_c](https://www.youtube.com/watch?v=YPorP8BsF_c)
