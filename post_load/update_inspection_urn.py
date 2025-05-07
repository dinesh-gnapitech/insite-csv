import os
import psycopg2
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv(override=True)

# Set up database config from environment
db_config = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "dbname": os.getenv("PG_DATABASE"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD")
}
print("Db Name:",os.getenv("PG_DATABASE"))
# SQL update query (inspection_urn, no {})
update_query = """
UPDATE data.inspection_insite_history_component AS comp
SET inspection_urn = subq.urns
FROM (
    SELECT 
        i.asset_id,
        i.wo_number,
        STRING_AGG('inspection_insite_history/' || i.id, ';') AS urns
    FROM data.inspection_insite_history i
    GROUP BY i.asset_id, i.wo_number
) AS subq
WHERE comp.asset_id = subq.asset_id
  AND comp.wo_number = subq.wo_number;
"""

def run_update():
    try:
        print("Connecting to PostgreSQL...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        print("Running update query...")
        cursor.execute(update_query)
        conn.commit()

        print("✅ Update completed successfully.")

    except Exception as e:
        print("❌ Error occurred:", str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    run_update()
 