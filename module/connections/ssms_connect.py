import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

SSMS_DB_SERVER = os.getenv('SSMS_DB_SERVER')
SSMS_DB_DATABASE = os.getenv('SSMS_DB_DATABASE')
SSMS_DB_USERNAME = os.getenv('SSMS_DB_USERNAME')
SSMS_DB_PASS = os.getenv('SSMS_DB_PASS')


def ssms_connect():
    SERVER = 'PQVP-sql.viewpointdata.cloud,4167'
    DATABASE = 'Viewpoint'
    USERNAME = 'brianb.sql'
    PASSWORD = 'vfQ*3A7fRdV'

    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SSMS_DB_SERVER};DATABASE={SSMS_DB_DATABASE};UID={SSMS_DB_USERNAME};PWD={SSMS_DB_PASS}'

    ssms_conn = pyodbc.connect(connection_string)

    ssms_cur = ssms_conn.cursor()

    return ssms_conn, ssms_cur


def ssms_close_cursor(conn, cursor):
    cursor.close()


def ssms_close_connect(conn):
    conn.close()
