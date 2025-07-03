import pandas as pd
import psycopg2
import json
import pymssql
from sqlalchemy import create_engine, text
import ast

import os
from dotenv import load_dotenv
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent / "service"))
from database import DatabaseService

script_dir = Path(os.getcwd())
env_path = script_dir / '.env'
load_dotenv(env_path)