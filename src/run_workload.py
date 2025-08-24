import os
import random
import time
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv


def get_env():
    load_dotenv()
    return dict(
        DATABASE_URL=os.environ.get("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/pg_partition_lab"),
        SCHEMA=os.environ.get("SCHEMA", "public"),
        POOL_SIZE=int(os.environ.get("POOL_SIZE", "20")),
        MAX_OVERFLOW=int(os.environ.get("MAX_OVERFLOW", "20")),
        CONCURRENCY=int(os.environ.get("CONCURRENCY", "40")),
        READ_RATIO=float(os.environ.get("READ_RATIO", "0.6")),
        UPDATE_RATIO=float(os.environ.get("UPDATE_RATIO", "0.3")),
        INSERT_RATIO=float(os.environ.get("INSERT_RATIO", "0.1")),
        START_DATE=os.environ.get("START_DATE", "2025-01-01"),
        END_DATE=os.environ.get("END_DATE", "2025-02-01"),
    )

def mk_engine(url: str, pool_size: int, max_overflow:int) -> Engine: 
    return create_engine(url, pool_size=pool_size, max_overflow=max_overflow, pool_pre_ping=True, future=True)

def rand_time(start: str, end: str) -> tuple[str, str]:
    """
    Picks a small random window within the global [start,end)
    for range queries. 
    """
    s = datetime.fromisoformat(start)
    e = datetime.fromisoformat(end)
    span = (e - s).days
    base = s + timedelta(days=random.randint(0, max(0, span-1)))
    lo = base
    hi = base + timedelta(hours=6) # narrow window (fits one day partition)
    return lo.isoformat(sep=' '), hi.isoformat(sep=' ')

def do_unbounded_range(engine: Engine, schema: str, start: str, end: str):
    """
    This performs a range query which doesn't use an order_time filter.
    - This will likely create many locks 
    """

    sql = text(f"SELECT count(*) FROM {schema}.orders WHERE amount > :min_amount")
    with engine.begin() as con:
        con.execute(sql, {"min_amount": 50})

def do_pruned_range(engine: Engine, schema: str, start: str, end: str):
    """
    This targters a single partition via order_time
    """
    lo, hi = rand_time(start, end)
    sql = text(f"""
        SELECT count(*)
        FROM {schema}.orders
        WHERE amount > :min_amount
          AND order_time >= :lo AND order_time < :hi
    """)
    with engine.begin() as con:
        con.execute(sql, {"min_amount": 50, "lo": lo, "hi": hi})

def do_lookup(engine: Engine, schema: str):
    """
    Lookup the most recent order, then fetch its full row.
    """
    sql = text(f"""
        SELECT order_id FROM {schema}.orders
        WHERE order_time > now() - interval '365 days'
        ORDER BY order_time DESC
        LIMIT 1
    """)
    with engine.begin() as con:
        row = con.execute(sql).first()
        # If we found an order_id, fetch its full row
        if row:
            oid = row[0]
            con.execute(text(f"SELECT * FROM {schema}.orders WHERE order_id = :oid"), {"oid": str(oid)})

def do_update(engine: Engine, schema: str): 
    """
    Flip some 'new' work to 'in_progress' or 'done'. Use LIMIT via
    ctid (customer id) to avoid full-table update
    """
    sql = text(f"""
        UPDATE {schema}.orders o
        SET status = CASE status WHEN 'new' THEN 'in_progress' ELSE 'done' END,
            updated_at = now()
        WHERE ctid = (
            SELECT ctid FROM {schema}.orders
            WHERE status IN ('new','in_progress')
            ORDER BY order_time DESC
            LIMIT 1
        )
    """)
    with engine.begin() as con:
        con.execute(sql)

def do_insert(engine: Engine, schema: str):
    """
    Insert a tiny new order in the current time (routes to the newest partition).
    """
    sql = text(f"""
        INSERT INTO {schema}.orders(order_id, customer_id, store_id, status, amount, order_time, updated_at)
        VALUES (gen_random_uuid(), floor(random()*100000)::int, floor(random()*500)::int, 'new', 42.00, now(), now())
    """)
    with engine.begin() as con:
        con.execute(sql)

