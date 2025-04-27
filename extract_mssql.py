import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def extract_from_mssql(config, logger, chunk_size=100000):
    MSSQL_SERVER = os.getenv('MSSQL_SERVER')
    MSSQL_DATABASE = os.getenv('MSSQL_DATABASE')
    MSSQL_USER = os.getenv('MSSQL_USER')
    MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD')

    logger.info(f"Connecting to MSSQL Server for {config['source_table']}...")
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={MSSQL_SERVER};'
        f'DATABASE={MSSQL_DATABASE};'
        f'UID={MSSQL_USER};'
        f'PWD={MSSQL_PASSWORD};'
    )
    conn = pyodbc.connect(conn_str)
    query = config["query"]
    csv_file_path = config["csv_file"]

    logger.info(f"Extracting data...")
    df = pd.read_sql(query, conn)
    df.to_csv(csv_file_path, index=False)
    conn.close()
    logger.info(f"Saved data to CSV at {os.path.abspath(csv_file_path)}.")

    return csv_file_path, len(df)
