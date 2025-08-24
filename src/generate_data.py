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