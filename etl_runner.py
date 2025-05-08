import json
import os
import traceback
from utils.logger_setup import setup_logger
from extract_mssql import extract_from_mssql

os.makedirs("logs", exist_ok=True)

with open('config.json', 'r') as f:
    config = json.load(f)

master_log = 'logs/etl_master.log'
global_logger = setup_logger(master_log, 'MasterLogger')
global_logger.info("ETL process started.")

success_tables = []
failed_tables = []

try:
    for table_config in config["tables"]:
        source = table_config['source_table']
        table_name = source.split('.')[-1].lower()
        log_file = f"logs/etl_{table_name}.log"
        table_logger = setup_logger(log_file, f"Logger_{table_name}")

        try:
            table_logger.info(f"Starting export for: {source}")
            csv_files, row_count = extract_from_mssql(table_config, table_logger)

            for file in csv_files:
                table_logger.info(f"[SUCCESS] Chunk file written: {file}")
            table_logger.info(f"[SUMMARY] Total rows: {row_count}")
            success_tables.append(source)

        except Exception as e:
            table_logger.error(f"[FAILED] Export failed for: {source}")
            table_logger.error(traceback.format_exc())
            failed_tables.append(source)

except Exception as e:
    global_logger.critical("[CRITICAL] Fatal ETL failure")
    global_logger.critical(traceback.format_exc())

finally:
    global_logger.info("ETL process complete.")
    global_logger.info(f"[SUCCESS] Tables: {success_tables}")
    global_logger.info(f"[FAILED] Tables: {failed_tables}")
