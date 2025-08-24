# NOTE: This file is intentionally *verbose*. It's meant for learning.
# Every function has a docstring that explains *why* we're doing something,
# and inline comments explain *what each line does*.
# You can trim comments later when you're comfortable with the flow.

import os
import sys
import argparse
from datetime import datetime, timedelta
from typing import Iterable, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

def env_bool(name: str, default: bool) -> bool:
    """Helper to read booleans from environment like 'true/false/1/0'."""
    val = os.getenv(name, str(default)).strip().lower()
    return val in ("1", "true", "yes", "y")

def get_env() -> dict:
    """
    Load .env (if present) and return relevant settings as a dict.
    """
    load_dotenv()
    return dict(
        DATABASE_URL=os.environ.get("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/pg_partition_lab"),
        SCHEMA=os.environ.get("SCHEMA", "public"),
        START_DATE=os.environ.get("START_DATE", "2025-01-01"),
        END_DATE=os.environ.get("END_DATE", "2025-02-01"),
        PARTITION_GRAIN=os.environ.get("PARTITION_GRAIN", "day").lower(),
        DUMMY_INDEXES=int(os.environ.get("DUMMY_INDEXES", "0")),
    )

def mk_engine(url: str) -> Engine:
    """Create a SQLAlchemy Engine for Postgres; pool defaults are fine here."""
    # The engine manages DBAPI connections for us.
    return create_engine(url, pool_pre_ping=True, future=True)

def parse_dates(start: str, end: str, grain: str) -> Iterable[Tuple[datetime, datetime, str]]:
    """Yield (start, end, suffix) windows for partitions of given grain (day/week).
    'suffix' is used in child table names, like orders_2025_01_07.
    """
    dt_start = datetime.fromisoformat(start)
    dt_end = datetime.fromisoformat(end)
    step = timedelta(days=1) if grain == "day" else timedelta(weeks=1)
    cur = dt_start
    while cur < dt_end:
        nxt = min(cur + step, dt_end)
        suffix = cur.strftime("%Y_%m_%d") if grain == "day" else f"wk_{cur.strftime('%Y_%m_%d')}"
        yield cur, nxt, suffix
        cur = nxt

def run_sql(engine: Engine, sql: str, **params) -> None:
    """Execute a single SQL statement with optional parameters."""
    with engine.begin() as con:  # 'begin' opens a tx and commits on success/close
        con.execute(text(sql), params)

def create_parent(engine: Engine, schema: str) -> None:
    """
    Create the PARTITIONED parent table. Drops any existing 
    parent (lab safety).
    """
    run_sql(engine, f"""
    SET lock_timeout = '5s';
    CREATE SCHEMA IF NOT EXISTS {schema};
    DROP TABLE IF EXISTS {schema}.orders CASCADE;
    CREATE TABLE {schema}.orders (
        order_id   UUID PRIMARY KEY,
        customer_id INT NOT NULL,
        store_id    INT NOT NULL,
        status      TEXT NOT NULL,
        amount      NUMERIC(12,2) NOT NULL,
        order_time  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        updated_at  TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()
    ) PARTITION BY RANGE (order_time);
    """)

def create_child_partition(engine: Engine, 
                           schema: str, pstart: datetime, 
                           pend: datetime, suffix: str) -> None:
    """Create one child partition in [pstart, pend)."""
    run_sql(engine, f"""
    CREATE TABLE IF NOT EXISTS {schema}.orders_{suffix}
        PARTITION OF {schema}.orders
        FOR VALUES FROM (:pstart) TO (:pend);
    """, pstart=pstart, pend=pend)

def create_indexes_on_child(engine: Engine, schema: str, suffix: str, dummy_indexes: int) -> None:
    """Create a realistic set of indexes on a child partition.
    Also create N dummy partial indexes (to inflate lock counts) if requested.
    """
    # Index per-child ensures fewer index bloat per child and better pruning.
    idx_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_orders_{suffix}_order_time ON {schema}.orders_{suffix} (order_time);
    CREATE INDEX IF NOT EXISTS idx_orders_{suffix}_customer   ON {schema}.orders_{suffix} (customer_id);
    CREATE INDEX IF NOT EXISTS idx_orders_{suffix}_store      ON {schema}.orders_{suffix} (store_id);
    -- Partial index often used for queues (only 'new' work)
    CREATE INDEX IF NOT EXISTS idx_orders_{suffix}_status_new ON {schema}.orders_{suffix} (order_time)
        WHERE status = 'new';
    """
    run_sql(engine, idx_sql)

    # Optional dummy partial indexes: these are *intentionally redundant* for the lab.
    for i in range(dummy_indexes):
        run_sql(engine, f"""
        CREATE INDEX IF NOT EXISTS idx_orders_{suffix}_dummy_{i} ON {schema}.orders_{suffix} ((EXTRACT(EPOCH FROM order_time)))
        WHERE (EXTRACT(EPOCH FROM order_time)::BIGINT % :modulus) = :remainder;
        """, modulus=max(1, dummy_indexes), remainder=i % max(1, dummy_indexes))

def create_partitions(engine: Engine, schema: str, 
                      start: str, end: str, grain: str, 
                      dummy_indexes: int) -> None:
    """Create many partitions with indexes. This is where lock count can 
    explode later."""
    for pstart, pend, suffix in parse_dates(start, end, grain):
        create_child_partition(engine, schema, pstart, pend, suffix)
        create_indexes_on_child(engine, schema, suffix, dummy_indexes)

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Create partitioned orders table and child partitions.")
    parser.add_argument("--init", action="store_true", help="Drop & recreate parent table.")
    parser.add_argument("--create-partitions", action="store_true", help="Create partitions and indexes.")
    args = parser.parse_args(argv)

    cfg = get_env()
    engine = mk_engine(cfg["DATABASE_URL"])

    if args.init:
        print("Creating parent partitioned table ...")
        create_parent(engine, cfg["SCHEMA"])

    if args.create_partitions:
        print(f"Creating partitions ({cfg['PARTITION_GRAIN']}) from {cfg['START_DATE']} to {cfg['END_DATE']} ...")
        create_partitions(engine, cfg["SCHEMA"], cfg["START_DATE"], cfg["END_DATE"], cfg["PARTITION_GRAIN"], cfg["DUMMY_INDEXES"])
        print("Done.")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
