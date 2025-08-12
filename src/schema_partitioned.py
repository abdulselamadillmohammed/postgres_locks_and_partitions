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


print(env_bool("KILL_BLOCKERS", False))