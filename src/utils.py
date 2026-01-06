import os
from dotenv import load_dotenv
import pyodbc

load_dotenv()

def get_conn_strings():
    oltp = {
        "server": os.getenv("DB_SERVER_OLTP"),
        "db": os.getenv("DB_BBDD_OLTP"),
    }
    dw = {
        "server": os.getenv("DB_SERVER_OLAP"),
        "db": os.getenv("DB_BBDD_OLAP"),
    }
    return oltp, dw

def odbc_conn_str(server, db):
    return (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};DATABASE={db};Trusted_Connection=yes;"
    )

def get_connection(conn_dict):
    return pyodbc.connect(
        odbc_conn_str(conn_dict["server"], conn_dict["db"])
    )