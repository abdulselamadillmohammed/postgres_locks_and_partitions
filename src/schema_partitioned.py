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