import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(override=True)

def load_to_postgres(csv_file_path, config, logger):
    PG_HOST = os.getenv('PG_HOST')
    PG_PORT = os.getenv('PG_PORT')
    PG_DATABASE = os.getenv('PG_DATABASE')
    PG_USER = os.getenv('PG_USER')
    PG_PASSWORD = os.getenv('PG_PASSWORD')

    target_table = config["target_table"]
    print("Target DB:",PG_DATABASE)

    logger.info(f"Connecting to PostgreSQL for {target_table}...")
    conn = psycopg2.connect(
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )
    cur = conn.cursor()

    with open(csv_file_path, 'r', encoding='utf-8') as f:
        cur.copy_expert(f"""
            COPY {target_table} FROM STDIN WITH CSV HEADER DELIMITER ',';
        """, f)
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"CSV data loaded into PostgreSQL target table: {target_table}.")
