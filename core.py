import os
import pyodbc
import pandas as pd
import polars as pl
from functools import lru_cache


def create_connection():
    """
    Crée et retourne une connexion ODBC à SQL Server.
    Les paramètres DB_DRIVER, DB_SERVER, DB_NAME sont lus depuis l'environnement.
    """
    driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
    server = os.getenv('DB_SERVER', 'XW12LBIZ0013')
    database = os.getenv('DB_NAME', 'AIDB')
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)