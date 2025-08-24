-- queries.sql

-- Count of locks by type/mode (snapshot)
SELECT locktype, mode, COUNT(*) 
FROM pg_locks 
GROUP BY locktype, mode 
ORDER BY locktype, mode;

-- Fast-path vs regular locks per PID (from AWS RDS docs)
SELECT count(*) AS cnt, pid, mode, fastpath
FROM pg_locks
WHERE fastpath IS NOT NULL
GROUP BY fastpath, mode, pid
ORDER BY pid, mode;

-- Who is blocking whom (and for how long)?
SELECT
  now() - a.query_start AS query_age,
  now() - a.xact_start  AS xact_age,
  a.pid,
  pg_blocking_pids(a.pid) AS blocking_pids,
  a.wait_event,
  left(a.query, 120) AS query_snippet
FROM pg_stat_activity a
WHERE cardinality(pg_blocking_pids(a.pid)) > 0
ORDER BY query_age DESC;

-- Terminate all blockers (DANGEROUS; tested only in lab)
-- UPDATE: Call via monitor_locks.py to avoid accidents.
-- SELECT pg_terminate_backend(bpid)
-- FROM (
--   SELECT unnest(pg_blocking_pids(a.pid)) AS bpid
--   FROM pg_stat_activity a
-- ) s;
