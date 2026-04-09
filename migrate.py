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
        # 1. Connect to MotherDuck
        print("Connecting to MotherDuck...")
        md_conn = duckdb.connect(f"md:?motherduck_token={MOTHERDUCK_TOKEN}")

        # 2. Create the Database & Switch Context
        print("Creating cloud database 'ibkr_dashboard'...")
        md_conn.execute("CREATE DATABASE IF NOT EXISTS ibkr_dashboard")
        md_conn.execute("USE ibkr_dashboard")

        # 3. Create Tables Explicitly (PRESERVES PRIMARY KEYS)
        print("Initializing cloud table schemas...")
        md_conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id VARCHAR PRIMARY KEY,
                    symbol VARCHAR,
                    description VARCHAR,
                    asset_class VARCHAR,
                    trade_date TIMESTAMP,
                    quantity DOUBLE,
                    price DOUBLE,
                    commission DOUBLE,
                    realized_pnl DOUBLE,
                    currency VARCHAR,
                    flex_query_run_id VARCHAR,
                    buy_sell VARCHAR,
                    open_close VARCHAR,
                    close_price DOUBLE,
                    underlying VARCHAR,
                    strike DOUBLE,
                    expiry VARCHAR,
                    put_call VARCHAR,
                    multiplier DOUBLE,
                    code VARCHAR
                )
            """)
        md_conn.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id VARCHAR PRIMARY KEY,
                    type VARCHAR,
                    asset_class VARCHAR,
                    symbol VARCHAR,
                    amount DOUBLE,
                    date TIMESTAMP,
                    description VARCHAR,
                    currency VARCHAR
                )
            """)
        md_conn.execute("""
                CREATE TABLE IF NOT EXISTS market_data (
                    symbol VARCHAR,
                    date TIMESTAMP,
                    close DOUBLE,
                    PRIMARY KEY (symbol, date)
                )
            """)
        md_conn.execute("""
                CREATE TABLE IF NOT EXISTS app_metadata (
                    key VARCHAR PRIMARY KEY,
                    value VARCHAR
                )
            """)

        # 4. Attach Local Database
        print(f"Attaching local database: {LOCAL_DB_PATH}...")
        md_conn.execute(f"ATTACH '{LOCAL_DB_PATH}' AS local_db")

        # 5. Copy Data
        tables = ['trades', 'transactions', 'market_data', 'app_metadata']

        for table in tables:
            print(f"Migrating table: {table}...")
            try:
                # Check if table exists in the attached local DB
                query = f"SELECT count(*) FROM information_schema.tables WHERE table_catalog='local_db' AND table_name='{table}'"
                table_exists = md_conn.execute(query).fetchone()[0] > 0

                if not table_exists:
                    print(f" -> Skipping {table} (not found in local DB).")
                    continue

                # Copy Data (INSERT OR IGNORE now works because PKs exist!)
                md_conn.execute(f"INSERT OR IGNORE INTO {table} SELECT * FROM local_db.{table}")

                # Verify
                count = md_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f" -> Done. Total rows in Cloud {table}: {count}")

            except Exception as e:
                print(f" -> Error migrating {table}: {e}")

        print("--- Migration Complete ---")
        md_conn.close()

    except Exception as e:
        print(f"Migration Failed: {e}")

if __name__ == "__main__":
    migrate()