import duckdb
import pandas as pd
import logging
from config import settings
from pathlib import Path

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Handles interactions with the DuckDB database.
    Supports local file-based storage and can be extended for remote Postgres.
    """

    def __init__(self):
        self.db_path = settings.DATABASE_URL
        # Ensure the data directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = None

    def get_connection(self):
        """Returns a connection to the DuckDB database."""
        if self.conn is None:
            # We use 'read_only=False' to allow writes
            self.conn = duckdb.connect(self.db_path)
            self._initialize_tables()
        return self.conn

    def _initialize_tables(self):
        """Creates tables if they don't exist."""
        conn = self.conn

        # Trades Table: Updated with Option & Execution fields
        conn.execute("""
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

                -- New Fields for Execution/Options --
                buy_sell VARCHAR,
                open_close VARCHAR,
                close_price DOUBLE,
                underlying VARCHAR,
                strike DOUBLE,
                expiry VARCHAR,
                put_call VARCHAR
            )
        """)

        # Transactions Table: Captures dividends, interest, and transfers
        conn.execute("""
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
        logger.info("Database tables initialized.")

    def save_dataframe(self, table_name: str, df: pd.DataFrame):
        """
        Saves a Pandas DataFrame to the specified table.
        Uses an 'INSERT OR IGNORE' pattern based on primary keys to avoid duplicates.
        """
        if df.empty:
            return

        # --- FIX: Convert IBKR Timestamp Format ---
        # IBKR sends 'YYYYMMDD;HHMMSS' (e.g., '20251218;093547')
        # DuckDB requires standard ISO format or Python datetime objects
        date_cols = ['trade_date', 'date']
        for col in date_cols:
            if col in df.columns and df[col].dtype == 'object':
                try:
                    # Coerce errors to NaT (Not a Time) to prevent crashing on bad data
                    df[col] = pd.to_datetime(df[col], format='%Y%m%d;%H%M%S', errors='coerce')
                except Exception as e:
                    logger.warning(f"Could not convert timestamp for column {col}: {e}")
        # ------------------------------------------

        conn = self.get_connection()
        # Register the dataframe as a temporary view in DuckDB
        conn.register('df_view', df)

        # Use DuckDB's native insertion logic
        try:
            conn.execute(f"INSERT OR IGNORE INTO {table_name} SELECT * FROM df_view")
            logger.info(f"Successfully synced {len(df)} rows to {table_name}.")
        except Exception as e:
            logger.error(f"Failed to save to {table_name}: {e}")
        finally:
            conn.unregister('df_view')

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None