import pyodbc
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Build MSSQL classic style connection string
connection_string = (
    f"DRIVER={{{os.getenv('MSSQL_DRIVER')}}};"
    f"SERVER={os.getenv('MSSQL_SERVER')};"
    f"DATABASE={os.getenv('MSSQL_DATABASE')};"
    f"UID={os.getenv('MSSQL_USER')};"
    f"PWD={os.getenv('MSSQL_PASSWORD')}"
)
print(f"Connection String: {connection_string}")

# List of SQL files to execute
sql_files = [
    "1_pre_load_operations_insite_workorder_history.sql",
    "2_pre_load_operations_ latest_structure_and_switch_data.sql",
    "3_pre_load_operations_inspection_union.sql",
    "4_pre_load_operations_inspection_insite_history.sql",
    "5_pre_load_operations_inspection_insite_history_component.sql"
]

# Initialize connection and cursor to None
conn = None
cursor = None

try:
    print("Connecting to SQL Server...")
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    for sql_filename in sql_files:
        sql_file_path = os.path.join(os.path.dirname(__file__), sql_filename)

        if not os.path.exists(sql_file_path):
            print(f"⚠️  SQL file not found, skipping: {sql_file_path}")
            continue

        print(f"Executing SQL file: {sql_filename}")

        # Read and execute SQL script
        with open(sql_file_path, 'r') as file:
            sql_script = file.read()

        try:
            cursor.execute(sql_script)
            conn.commit()
            print(f"✅ Successfully executed: {sql_filename}")
        except Exception as e:
            print(f"❌ Error executing {sql_filename}: {e}")

except Exception as e:
    print(f"❌ Error during database connection or execution: {e}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Database connection closed.")
 