import os
import math
from uuid import uuid4
from datetime import datetime, timedelta
from typing import Iterator, List, Tuple

import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from tqdm import tqdm

from .schema_partitioned import mk_engine

fake = Faker()

def get_env():
    load_dotenv()
    return dict(
        DATABASE_URL=os.environ.get("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/pg_partition_lab"),
        SCHEMA=os.environ.get("SCHEMA", "public"),
        ROWS=int(os.environ.get("ROWS", "100000")),
        BATCH_SIZE=int(os.environ.get("BATCH_SIZE", "5000")),
        START_DATE=os.environ.get("START_DATE", "2025-01-01"),
        END_DATE=os.environ.get("END_DATE", "2025-02-01"),
    )

def synth_row(day: datetime) -> Tuple:
    """
    Create one synthetic order row for a given day. 
    Returns a tuple matching table columns.
    """
    order_id = uuid4()
    customer_id = Faker().random_int(min=1, max=100_000)
    store_id = Faker().random_int(min=1, max=500)
    status = Faker().random_element(elements=("new","in_progress", "done", "failed"))
    amount = round(Faker().pyfloat(left_digits=4, right=2, positive=True, min_value=5, max_value=500), 2)
    # Random time within the day
    order_time = day + timedelta(seconds=Faker().random_int(min=0, max=86399))
    updated_at = order_time
    return (str(order_id), customer_id, store_id, status, amount, order_time, updated_at)

def batch_insert(engine: Engine, schema: str, rows: List[Tuple]):
    """
    Insert a batch using an executemany against the parent table (Postgres routes to child).
    """
    sql = text(f"""
        INSERT INTO {schema}.orders(order_id, customer_id, store_id, status, amount, order_time, updated_at)
        VALUES (:order_id, :customer_id, :store_id, :status, :amount, :order_time, :updated_at)
    """)
    payload = [dict(order_id=r[0], customer_id=r[1], store_id=r[2], status=r[3], amount=r[4], order_time=r[5], updated_at=r[6]) for r in rows]
    with engine.begin() as con:
        con.execute(sql, payload)

