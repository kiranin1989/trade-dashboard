import duckdb
import pandas as pd
import logging
from config import settings
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabaseManager:
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
                buy_sell VARCHAR,
                open_close VARCHAR,
                close_price DOUBLE,
                underlying VARCHAR,
                strike DOUBLE,
                expiry VARCHAR,
                put_call VARCHAR,
                multiplier DOUBLE
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                symbol VARCHAR,
                date TIMESTAMP,
                close DOUBLE,
                PRIMARY KEY (symbol, date)
            )
        """)
        # --- NEW: Metadata Table ---
        conn.execute("""
            CREATE TABLE IF NOT EXISTS app_metadata (
                key VARCHAR PRIMARY KEY,
                value VARCHAR
            )
        """)

    def save_dataframe(self, table_name: str, df: pd.DataFrame):
        if df.empty: return
        date_cols = ['trade_date', 'date']
        for col in date_cols:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = pd.to_datetime(df[col], format='%Y%m%d;%H%M%S', errors='coerce')
        conn = self.get_connection()
        conn.register('df_view', df)
        try:
            conn.execute(f"INSERT OR IGNORE INTO {table_name} SELECT * FROM df_view")
            logger.info(f"Successfully synced {len(df)} rows to {table_name}.")
        except Exception as e:
            logger.error(f"Failed to save to {table_name}: {e}")
        finally:
            conn.unregister('df_view')

    def record_sync_time(self):
        """Updates the last_sync timestamp."""
        conn = self.get_connection()
        now_str = datetime.now().isoformat()
        conn.execute("INSERT OR REPLACE INTO app_metadata (key, value) VALUES ('last_sync', ?)", [now_str])

    def get_last_sync_time(self):
        """Returns the last sync timestamp as a formatted string."""
        conn = self.get_connection()
        try:
            res = conn.execute("SELECT value FROM app_metadata WHERE key = 'last_sync'").fetchone()
            if res:
                dt = datetime.fromisoformat(res[0])
                return dt.strftime("%Y-%m-%d %H:%M")
            return "Never"
        except Exception:
            return "Unknown"

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None