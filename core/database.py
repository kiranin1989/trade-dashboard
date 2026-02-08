import duckdb
import pandas as pd
import logging
from config import settings
from pathlib import Path

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Handles interactions with the DuckDB database.
    """

    def __init__(self):
        self.db_path = settings.DATABASE_URL
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = None

    def get_connection(self):
        if self.conn is None:
            self.conn = duckdb.connect(self.db_path)
            self._initialize_tables()
        return self.conn

    def _initialize_tables(self):
        conn = self.conn

        # Trades Table: Added 'multiplier' explicitly
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

                -- Options / Execution Fields
                buy_sell VARCHAR,
                open_close VARCHAR,
                close_price DOUBLE,
                underlying VARCHAR,
                strike DOUBLE,
                expiry VARCHAR,
                put_call VARCHAR,
                multiplier DOUBLE  -- <--- NEW: Dedicated Column
            )
        """)

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
        if df.empty:
            return

        # Date Fix
        date_cols = ['trade_date', 'date']
        for col in date_cols:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = pd.to_datetime(df[col], format='%Y%m%d;%H%M%S', errors='coerce')

        conn = self.get_connection()
        conn.register('df_view', df)
        try:
            # We use INSERT OR REPLACE to ensure we update old rows with new schema if needed
            # But for safety in DuckDB, we'll stick to INSERT OR IGNORE and rely on DB reset
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