# monitor_locks.py

import os
import time
import argparse

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

def env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name, str(default)).strip().lower()
    return val in ("1","true","yes","y" )

def get_env():
    load_dotenv()
    return dict(
        DATABASE_URL=os.environ.get("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/pg_partition_lab"),
        KILL_BLOCKERS=env_bool("KILL_BLOCKERS", False),
    )

def mk_engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True, future=True)

def print_lock_snapshot(engine: Engine):
    sql = text("""
        SELECT locktype, mode, COUNT(*) AS cnt
        FROM pg_locks
        GROUP BY locktype, mode
        ORDER BY locktype, mode
    """)
    with engine.connect() as con:
        rows = con.execute(sql).all()
    print("Locks (by type/mode):")
    for lt, mode, cnt in rows:
        print(f"  {lt:>12} | {mode:<20} | {cnt}")

def print_fastpath(engine: Engine):
    sql = text("""
        SELECT count(*) AS cnt, pid, mode, fastpath
        FROM pg_locks
        WHERE fastpath IS NOT NULL
        GROUP BY fastpath, mode, pid
        ORDER BY pid, mode
    """)
    with engine.connect() as con:
        rows = con.execute(sql).all()
    print("Fast-path vs regular locks (per PID):")
    for cnt, pid, mode, fast in rows[:20]:  # print top 20 to avoid noise
        print(f"  pid={pid} mode={mode:<20} fastpath={fast} cnt={cnt}")

def list_blockers(engine: Engine):
    sql = text("""
      SELECT
        now() - a.query_start AS query_age,
        now() - a.xact_start  AS xact_age,
        a.pid,
        pg_blocking_pids(a.pid) AS blocking_pids,
        a.wait_event,
        left(a.query, 120) AS query_snippet
      FROM pg_stat_activity a
      WHERE cardinality(pg_blocking_pids(a.pid)) > 0
      ORDER BY query_age DESC
    """)
    with engine.connect() as con:
        return con.execute(sql).all()

def kill_blockers(engine: Engine):
    """Terminate all blocker PIDs (use only in a lab!)."""
    sql = text("""
      WITH blockers AS (
        SELECT DISTINCT unnest(pg_blocking_pids(a.pid)) AS bpid
        FROM pg_stat_activity a
        WHERE cardinality(pg_blocking_pids(a.pid)) > 0
      )
      SELECT pg_terminate_backend(bpid) FROM blockers
    """)
    with engine.begin() as con:
        con.execute(sql)

def main():
    cfg = get_env()
    parser = argparse.ArgumentParser(description="Monitor Postgres locks/blockers.")
    parser.add_argument("--every", default="2s", help="Refresh interval, e.g. 2s, 500ms.")
    args = parser.parse_args()

    # Parse interval
    interval = 2.0
    s = args.every.strip().lower()
    if s.endswith("ms"):
        interval = max(0.1, float(s[:-2]) / 1000.0)
    elif s.endswith("s"):
        interval = max(0.1, float(s[:-1]))
    else:
        interval = max(0.1, float(s))

    engine = mk_engine(cfg["DATABASE_URL"])

    while True:
        print("\n=== Lock snapshot ===")
        print_lock_snapshot(engine)
        print("\n=== Fast-path (sample) ===")
        print_fastpath(engine)
        blockers = list_blockers(engine)
        print(f"\n=== Blockers/Waiters ({len(blockers)}) ===")
        for row in blockers[:10]:
            qage, xage, pid, bpids, wait, snippet = row
            print(f"  pid={pid} wait={wait} qage={qage} xage={xage} bpids={bpids} :: {snippet}")

        if cfg["KILL_BLOCKERS"] and blockers:
            print("Attempting to terminate blockers ...")
            try:
                kill_blockers(engine)
                print("Blockers terminated.")
            except Exception as e:
                print("Kill failed:", repr(e))

        time.sleep(interval)

if __name__ == "__main__":
    main()
