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
print(connection_string)
# SQL file path
sql_file_path = os.path.join(os.path.dirname(__file__), "1_pre_load_operations_insite_workorder_history.sql")

if not os.path.exists(sql_file_path):
    raise FileNotFoundError(f"SQL file not found at: {sql_file_path}")

# Read SQL file content
with open(sql_file_path, 'r') as file:
    sql_script = file.read()



# Initialize connection and cursor to None
conn = None
cursor = None

# Connect and execute SQL
try:
    print("Connecting to SQL Server...")
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    print("Executing SQL script...")
    cursor.execute(sql_script)
    conn.commit()

    print("SQL script executed successfully.")

except Exception as e:
    print(f"Error executing SQL script: {e}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Database connection closed.")
 