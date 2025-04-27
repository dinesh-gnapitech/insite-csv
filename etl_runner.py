import json
import traceback
import os
from utils.logger_setup import setup_logger, setup_deleted_file_logger
from extract_mssql import extract_from_mssql
from load_postgres import load_to_postgres

# Load config.json
with open('config.json', 'r') as f:
    config = json.load(f)

# Initialize loggers
global_logger = setup_logger('logs/etl_master.log', 'MasterLogger')
deleted_file_logger = setup_deleted_file_logger()

global_logger.info("Multi-table ETL process started.")

success_tables = []
failed_tables = []

try:
    for table_config in config["tables"]:
        source = table_config['source_table']
        target = table_config['target_table']

        log_file_name = f"logs/etl_{source.replace('.', '_')}.log"
        table_logger = setup_logger(log_file_name, f"Logger_{source.replace('.', '_')}")

        try:
            table_logger.info(f"🚀 Starting ETL for: {source} -> {target}")
            csv_file, extracted_count = extract_from_mssql(table_config, table_logger)
            load_to_postgres(csv_file, table_config, table_logger)

            # Delete CSV file and log
            if os.path.exists(csv_file):
                os.remove(csv_file)
                table_logger.info(f"🗑️ Deleted CSV file after load: {csv_file}")
                deleted_file_logger.info(f"Deleted: {csv_file} (Source: {source} -> Target: {target})")
            else:
                table_logger.warning(f"CSV file not found for deletion: {csv_file}")

            table_logger.info(f"✅ Completed ETL for: {source} -> {target}")
            success_tables.append(f"{source} -> {target}")

        except Exception as table_err:
            table_logger.error(f"❌ ETL failed for: {source} -> {target}")
            table_logger.error(f"Error: {table_err}")
            table_logger.error(traceback.format_exc())
            failed_tables.append(f"{source} -> {target} (ETL Failed)")

except Exception as e:
    global_logger.critical(f"🔥 Critical error in ETL process: {e}")
    global_logger.critical(traceback.format_exc())

finally:
    global_logger.info("Multi-table ETL process completed.")
    global_logger.info(f"Tables processed successfully: {success_tables}")
    global_logger.info(f"Tables failed: {failed_tables}")
