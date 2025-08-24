import os
#import sys
import argparse

from datetime import datetime, timedelta
from typing import Iterable, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# to run: python3 src/schema_partitioned.py --init --create-partitions


load_dotenv() 

def env_bool(name: str, default: bool) -> bool:
    """
    Helper function in order to read booleans from the environment 
    like 'True/False.'
    """
    val = os.getenv(name, str(default)).strip().lower()
    print(val)
    return val in ("1", "true", "yes", "y")    

#print(env_bool("KILL_BLOCKERS", False))

def get_env() -> dict:
    """
    Returns database related env variables are a dict.
    """
    return dict(
        DATABASE_URL=os.environ.get("DATABASE_URL"),
        SCHEMA=os.environ.get("SCHEMA"),
        START_DATE=os.environ.get("START_DATE"),
        END_DATE=os.environ.get("END_DATE"),
        PARTITION_GRAIN=os.environ.get("PARTITION_GRAIN").lower(),
        DUMMY_INDEXES=int(os.environ.get("DUMMY_INDEXES")),
    )

def mk_engine(url: str) -> Engine:
    """
    Creates SQLAlchemy engine for postgres
    """
    #pool_pre_ping - avoids “connection already closed” erros
    # future = formatting in sqlalchemy 2
    return create_engine(url, pool_pre_ping=True, future=True)

# partition range creator
# grain: range as in week or day... (cause date time partition)
def parse_dates(start: str, end: str, grain: str) -> Iterable[Tuple[datetime, datetime, str]]:
    """
    Yield (for memory optimization) [start, end, suffix] windows for partions 
    for a given range/grain. 
    suffix is used for child table names
    """
    dt_start = datetime.fromisoformat(start)
    dt_end = datetime.fromisoformat(end)
    step = timedelta(days=1) if grain == "day" else timedelta(weeks=1)
    curr = dt_start
    while curr < dt_end:
        next = min(curr + step, dt_end)
        suffix = curr.strftime("%Y_%m_%d") if grain == "day" else f"wk_{curr.strftime("%Y_%m_%d")}"
        yield curr, next, suffix
        curr = next

def run_sql(engine: Engine, sql: str, **params) -> None:
    """
    Executes single line SQL statements
    """
    with engine.begin() as con:
        con.execute(text(sql), params)

def create_parent(engine: Engine, schema: str) -> None:
    """
    Creates a partitioned parent table.
    """
    run_sql(engine, f"""
    SET lock_timeout = '5s';
    CREATE SCEMA IF NOT EXISTS {schema};
    DROP TABLE IF EXISTS {schema}.orders CASCADE;
    CREATE TABLE {schema}.orders (
        order_id   UUID PRIMARY KEY,
        customer_id   INT NOT NULL,
        store_id      INT NOT NULL,
        status    TEXT NOT NULL,
        amount    NUMERIC(12,2) NOT NULL,
        orer_time  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        updated_at  TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now()
    ) PARTITION BY RANGE (order_time)
    """)

def create_child_partition(engine: Engine, schema: str, pstart:datetime, pend: datetime, suffix: str) -> None:
    """
    Creates a child partition in the range [pstart, pend) } end is exclusive
    """
    run_sql(engine, f"""
    CREATE TABLE IF NOT EXISTS {schema}.orders_{suffix}
        PARTITION OF {schema}.orders
        FOR VALUES FROM (:pstart) TO (:pend);
    """, pstart, pend)

def create_indexes_on_child(engine: Engine, schema: str, suffix:str, dummy_indexes: int) -> None:
    """
    Create a realistic set of indexes on a child partition.
    Also create N dummy partial indexes (to inflate lock counts) if requested.
    """
    idx_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_orders_{suffix}_order_time ON {schema}.orders_{suffix} (order_time);
    CREATE INDEX IF NOT EXISTS idx_orders_{suffix}_customer   ON {schema}.orders_{suffix} (customer_id);
    -- Partial index
    CREATE INDEX IF NOT EXISTS idx_order_{suffix}_status_new ON {schema}.orders_{suffix} (order_time)
        WHERE status = 'new';
    """
    run_sql(engine, idx_sql)

    # Create artificial locks to force on lock manager waits which are visible 
    for i in range(dummy_indexes):
        run_sql(engine, f"""
        CREATE INDEX IF NOT EXISTS idx_orders_{suffix}_dummy_{i} ON {schema}.orders_{suffix} ((EXTRACT(EPOCH FROM order_time)))
        WHERE (EXTRACT(EPOCH FROM order_time)::BIGINT % :modulus) = :remainder;
        """, modulus=max(1, dummy_indexes), remainder=i % max(1, dummy_indexes))

def create_partitions(engine: Engine, schema: str,
                      start: str, end: str, grain: str, 
                      dummy_indexes: int) -> None:
    """
    Creates multiple partitions with indexes in order to inflate lock count. 
    """
    for pstart, pend, suffix in parse_dates(start, end, grain):
        create_child_partition(engine, schema, pstart, pend, suffix)
        create_indexes_on_child(engine, schema, suffix, dummy_indexes)