import duckdb
import os
from dotenv import load_dotenv

# Load environment variables (to get the token)
load_dotenv()

LOCAL_DB_PATH = "data/trading_data.duckdb"
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")


def migrate():
    if not os.path.exists(LOCAL_DB_PATH):
        print(f"Error: Local database file not found at {LOCAL_DB_PATH}")
        return

    if not MOTHERDUCK_TOKEN:
        print("Error: MOTHERDUCK_TOKEN not found in .env file.")
        return

    print("--- Starting Migration to MotherDuck ---")

    try:
        # 1. Connect to MotherDuck (Default Context)
        # We do NOT specify 'ibkr_dashboard' in the connection string yet
        print("Connecting to MotherDuck...")
        md_conn = duckdb.connect(f"md:?motherduck_token={MOTHERDUCK_TOKEN}")

        # 2. Create the Database if it doesn't exist
        print("Creating cloud database 'ibkr_dashboard'...")
        md_conn.execute("CREATE DATABASE IF NOT EXISTS ibkr_dashboard")

        # 3. Switch context to use that database
        md_conn.execute("USE ibkr_dashboard")

        # 4. Attach Local Database
        print(f"Attaching local database: {LOCAL_DB_PATH}...")
        md_conn.execute(f"ATTACH '{LOCAL_DB_PATH}' AS local_db")

        # 5. Create Tables and Copy Data
        # Note: We check if tables exist in local_db first to avoid errors
        tables = ['trades', 'transactions', 'market_data', 'app_metadata']

        for table in tables:
            print(f"Migrating table: {table}...")
            try:
                # Check if table exists in local DB
                table_exists = md_conn.execute(
                    f"SELECT count(*) FROM information_schema.tables WHERE table_schema='local_db' AND table_name='{table}'"
                ).fetchone()[0] > 0

                if not table_exists:
                    print(f" -> Skipping {table} (not found in local DB).")
                    continue

                # Create schema in MotherDuck
                md_conn.execute(
                    f"CREATE TABLE IF NOT EXISTS ibkr_dashboard.{table} AS SELECT * FROM local_db.{table} WHERE 1=0")

                # Copy Data
                md_conn.execute(f"INSERT OR IGNORE INTO ibkr_dashboard.{table} SELECT * FROM local_db.{table}")

                # Verify
                count = md_conn.execute(f"SELECT COUNT(*) FROM ibkr_dashboard.{table}").fetchone()[0]
                print(f" -> Done. Total rows in Cloud {table}: {count}")

            except Exception as e:
                print(f" -> Error migrating {table}: {e}")

        print("--- Migration Complete ---")
        md_conn.close()

    except Exception as e:
        print(f"Migration Failed: {e}")


if __name__ == "__main__":
    migrate()