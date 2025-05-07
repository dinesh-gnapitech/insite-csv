import json
import traceback
import os
from utils.logger_setup import setup_logger, setup_deleted_file_logger
from extract_mssql import extract_from_mssql
from load_postgres import load_to_postgres

# Load config.json
with open('config.json', 'r', encoding='utf-8') as f:
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
            table_logger.info("🚀 Starting ETL for: %s -> %s", source, target)
            csv_file, extracted_count = extract_from_mssql(table_config, table_logger)
            load_to_postgres(csv_file, table_config, table_logger)

            # Delete CSV file and log
            if os.path.exists(csv_file):
                os.remove(csv_file)
                table_logger.info("🗑️ Deleted CSV file after load: %s", csv_file)
                deleted_file_logger.info("Deleted: %s (Source: %s -> Target: %s)", csv_file, source, target)
            else:
                table_logger.warning("CSV file not found for deletion: %s", csv_file)

            table_logger.info("✅ Completed ETL for: %s -> %s", source, target)
            success_tables.append(f"{source} -> {target}")

        except (FileNotFoundError, ValueError, RuntimeError) as table_err:  # Replace with specific exceptions
            table_logger.error("❌ ETL failed for: %s -> %s", source, target)
            table_logger.error("Error: %s", table_err)
            table_logger.error(traceback.format_exc())
            failed_tables.append(f"{source} -> {target} (ETL Failed)")

except (KeyError, ValueError, OSError) as e:  # Replace with specific exceptions relevant to your code
    global_logger.critical("🔥 Critical error in ETL process: %s", e)
    global_logger.critical(traceback.format_exc())

finally:
    global_logger.info("Multi-table ETL process completed.")
    global_logger.info("Tables processed successfully: %s", success_tables)
    if len(failed_tables) == 0:
        failed_tables=0
    global_logger.info("Tables failed: %s", failed_tables)
