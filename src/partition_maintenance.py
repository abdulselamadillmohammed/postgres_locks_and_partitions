# partition_maintenance.py
import os
import argparse
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

def get_env():
    load_dotenv()
    return dict(
        DATABASE_URL=os.environ.get("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/pg_partition_lab"),
        SCHEMA=os.environ.get("SCHEMA", "public"),
    )

def mk_engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True, future=True)

def week_range(start: datetime):
    """Return (inclusive, exclusive) timestamps covering the 7-day window starting at 'start'."""
    lo = start
    hi = start + timedelta(days=7)
    return lo, hi

def create_week_from_days(engine: Engine, schema: str, week_start: datetime, commit: bool):
    """Build a weekly partition by copying rows from seven daily partitions.
    The *safe* flow is:
      1) CREATE TABLE week LIKE day INCLUDING ALL  (so constraints/replication defs carry over)
      2) DROP the day constraint on the copy; add a week-range CHECK constraint
      3) INSERT ... SELECT from the 7 day partitions
      4) VALIDATE the new CHECK constraint
      5) ATTACH PARTITION the new week table to the parent
      6) (Optionally) DETACH the 7 day partitions

    This is an *offline-ish* flow—on busy systems each step can contend on locks.
    In this lab we keep it simple and transparent.
    """
    lo, hi = week_range(week_start)
    week_suffix = f"wk_{week_start.strftime('%Y_%m_%d')}"

    # Find one existing daily child to clone *including* replication/constraints
    probe_suffix = week_start.strftime("%Y_%m_%d")
    create_like = text(f"""
        CREATE TABLE IF NOT EXISTS {schema}.orders_{week_suffix}
        (LIKE {schema}.orders_{probe_suffix} INCLUDING ALL);
    """)

    # Drop the day CHECK on the copy (unknown name; search pg_constraint) and add week CHECK
    drop_fmt = f"""
    DO $$
    DECLARE
        cname text;
    BEGIN
        SELECT c.conname INTO cname
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = '{schema}' AND t.relname = 'orders_{week_suffix}' AND c.contype='c';
        IF cname IS NOT NULL THEN
            EXECUTE format('ALTER TABLE {schema}.orders_{week_suffix} DROP CONSTRAINT %I', cname);
        END IF;
    END $$;
    ALTER TABLE {schema}.orders_{week_suffix}
      ADD CONSTRAINT orders_{week_suffix}_constraint
      CHECK (order_time >= :lo AND order_time < :hi) NOT VALID;
    """

    # Insert from 7 days
    inserts = []
    for i in range(7):
        day = (week_start + timedelta(days=i)).strftime("%Y_%m_%d")
        inserts.append(f"INSERT INTO {schema}.orders_{week_suffix} SELECT * FROM {schema}.orders_{day};")
    insert_sql = "\n".join(inserts)

    validate_attach = text(f"""
        ALTER TABLE {schema}.orders_{week_suffix} VALIDATE CONSTRAINT orders_{week_suffix}_constraint;
        ALTER TABLE {schema}.orders ATTACH PARTITION {schema}.orders_{week_suffix}
          FOR VALUES FROM (:lo) TO (:hi);
    """)

    with engine.begin() as con:
        con.execute(create_like)
        con.execute(text(drop_fmt), {"lo": lo, "hi": hi})
        con.execute(text(insert_sql))
        con.execute(validate_attach)

    if commit:
        print(f"Created and attached week partition orders_{week_suffix} covering {lo}..{hi}.")
        print("You may now DETACH the 7 day partitions if desired (see functions below)." )
    else:
        print("(Dry run finished — but note we executed DDL/INSERT to make the example realistic.)")

def detach_partition(engine: Engine, schema: str, child_name: str):
    with engine.begin() as con:
        con.execute(text(f"ALTER TABLE {schema}.orders DETACH PARTITION {schema}.{child_name};"))

def attach_partition(engine: Engine, schema: str, child_name: str, lo: datetime, hi: datetime):
    with engine.begin() as con:
        con.execute(text(f"ALTER TABLE {schema}.orders ATTACH PARTITION {schema}.{child_name} FOR VALUES FROM (:lo) TO (:hi);"), {"lo": lo, "hi": hi})

def main():
    cfg = get_env()
    parser = argparse.ArgumentParser(description="Maintenance helpers: build week partition from daily ones, attach/detach.")
    parser.add_argument("--week-start", help="ISO date for the week start (YYYY-MM-DD)")
    parser.add_argument("--commit", action="store_true", help="Run with 'commit' semantics (this script already executes DDL).")
    args = parser.parse_args()

    engine = mk_engine(cfg["DATABASE_URL"])

    if args.week_start:
        ws = datetime.fromisoformat(args.week_start)
        create_week_from_days(engine, cfg["SCHEMA"], ws, args.commit)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
