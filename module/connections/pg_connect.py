import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

PG_DB_USER = os.getenv('PG_DB_USER')
PG_DB_PASS = os.getenv('PG_DB_PASS')
SSMS_DB_PASS = os.getenv('SSMS_DB_PASS')

def pg_db_connect():
    try:
        conn = psycopg2.connect(
            f"dbname=const_manager_new user={PG_DB_USER} password={PG_DB_PASS}")

        cur = conn.cursor()
        cur_dict = conn.cursor(cursor_factory=RealDictCursor)

        return conn, cur, cur_dict
    except Exception as e:
        print(f"Error connecting to postgres: {e}")