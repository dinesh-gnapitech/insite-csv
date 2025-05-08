import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

def extract_from_mssql(config, logger):
    # Load DB credentials
    MSSQL_SERVER = os.getenv('MSSQL_SERVER')
    MSSQL_DATABASE = os.getenv('MSSQL_DATABASE')
    MSSQL_USER = os.getenv('MSSQL_USER')
    MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD')

    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={MSSQL_SERVER};'
        f'DATABASE={MSSQL_DATABASE};'
        f'UID={MSSQL_USER};'
        f'PWD={MSSQL_PASSWORD};'
    )

    source_table = config["source_table"]
    table_name = source_table.split('.')[-1].lower()
    output_dir = os.path.join("output", table_name)
    os.makedirs(output_dir, exist_ok=True)

    # Read query from file
    if "query_file" in config:
        with open(config["query_file"], 'r') as f:
            query = f.read()
        logger.info(f"[INFO] Loaded query from: {config['query_file']}")
    else:
        query = config["query"]
        logger.info("[INFO] Loaded query directly from config")

    datetime_cols = [col.lower() for col in config.get("datetime_columns", [])]
    chunk_size = config.get("csv_chunk_size", 100000)

    # Start chunked read
    conn = pyodbc.connect(conn_str)
    chunks = pd.read_sql(query, conn, chunksize=chunk_size)

    total_rows = 0
    written_files = []

    for i, chunk in enumerate(chunks, start=1):
        chunk.columns = [col.lower() for col in chunk.columns]

        for col in datetime_cols:
            if col in chunk.columns:
                try:
                    chunk[col] = pd.to_datetime(chunk[col], errors='coerce')
                    non_null_times = chunk[col].dropna().dt.time
                    if all(t == pd.Timestamp.min.time() for t in non_null_times):
                        chunk[col] = chunk[col].dt.strftime('%Y-%m-%d')
                        logger.info(f"[PARSED] '{col}' as DATE in chunk {i}")
                    else:
                        chunk[col] = chunk[col].dt.strftime('%Y-%m-%dT%H:%M:%S')
                        logger.info(f"[PARSED] '{col}' as DATETIME in chunk {i}")
                except Exception as e:
                    logger.warning(f"[WARNING] Failed to format datetime column '{col}' in chunk {i}: {e}")

        csv_path = os.path.join(output_dir, f"{table_name}.{i}.csv")
        chunk.to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"[CHUNK] Written: {csv_path} ({len(chunk)} rows)")
        written_files.append(csv_path)
        total_rows += len(chunk)

    conn.close()
    logger.info(f"[DONE] {total_rows} rows extracted across {len(written_files)} files.")
    return written_files, total_rows
