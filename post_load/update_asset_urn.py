import os
import psycopg2
from dotenv import load_dotenv

# Load DB credentials from .env file
load_dotenv(override=True)

db_config = {
   "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "dbname": os.getenv("PG_DATABASE"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD")
}

# Only table names (strings used in URN)
structure_tables = [
    "eun_e_fos",
    "eun_e_lattice",
    "eun_e_lattice_assembly",
    "eun_e_pole",
    "eun_e_pole_assembly",
    "eun_e_support_pole",
    "eun_e_unknown_structurejunction"
]

def run_asset_urn_update():
    try:
        print("🔗 Connecting to the database...")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Step 1: Create temp tables per source table
        for table in structure_tables:
            print(f"📦 Aggregating from: data.{table}")
            create_temp = f"""
            CREATE TEMP TABLE tmp_urn_{table} AS
            SELECT structureguid AS asset_id,
                   STRING_AGG('{table}/' || id, ';') AS urn
            FROM data.{table}
            GROUP BY structureguid;
            """
            cursor.execute(create_temp)

        # Step 2: Combine all temp urn tables into one
        print("🔗 Combining all URNs...")
        union_all_query = " UNION ALL ".join([
            f"SELECT * FROM tmp_urn_{table}" for table in structure_tables
        ])
        cursor.execute(f"""
            CREATE TEMP TABLE tmp_combined_urns AS
            {union_all_query};
        """)

        # Step 3: Aggregate by asset_id
        print("🔄 Aggregating combined URNs per asset_id...")
        cursor.execute("""
            CREATE TEMP TABLE tmp_final_urns AS
            SELECT asset_id, STRING_AGG(urn, ';') AS full_urn
            FROM tmp_combined_urns
            GROUP BY asset_id;
        """)

        # Step 4: Update the target table
        print("📝 Updating asset_urn in data.inspection_insite_history...")
        cursor.execute("""
            UPDATE data.inspection_insite_history iih
            SET asset_urn = tmp.full_urn
            FROM tmp_final_urns tmp
            WHERE iih.asset_id = tmp.asset_id;
        """)

        conn.commit()
        print("✅ Update completed and committed successfully.")

    except Exception as e:
        print("❌ Error:", str(e))
        conn.rollback()
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == "__main__":
    run_asset_urn_update()
 