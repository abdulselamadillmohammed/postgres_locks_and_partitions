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


