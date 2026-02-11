import duckdb
import pandas as pd
import logging
from config import settings
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo  # Standard in Python 3.9+

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Handles interactions with the DuckDB database (Local or MotherDuck).
    """

    def __init__(self):
        # Check if MotherDuck token is present
        self.use_motherduck = settings.MOTHERDUCK_TOKEN is not None and len(settings.MOTHERDUCK_TOKEN) > 0

        if self.use_motherduck:
            self.db_path = f"md:ibkr_dashboard?motherduck_token={settings.MOTHERDUCK_TOKEN}"
            logger.info("Configured for MotherDuck Cloud Database.")
        else:
            self.db_path = settings.DATABASE_URL
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Configured for Local Database: {self.db_path}")

        self.conn = None

    def get_connection(self):
        if self.conn is None:
            self.conn = duckdb.connect(self.db_path)

            if self.use_motherduck:
                try:
                    self.conn.execute("USE ibkr_dashboard")
                except Exception as e:
                    logger.warning(f"Could not switch context to ibkr_dashboard: {e}")

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
                multiplier DOUBLE,
                code VARCHAR
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
        """Updates the last_sync timestamp in UTC."""
        conn = self.get_connection()
        # Store as UTC-aware datetime string
        now_utc = datetime.now(timezone.utc)
        conn.execute("INSERT OR REPLACE INTO app_metadata (key, value) VALUES ('last_sync', ?)", [now_utc.isoformat()])

    def get_last_sync_time(self):
        """Returns the last sync timestamp converted to NY Time."""
        conn = self.get_connection()
        try:
            res = conn.execute("SELECT value FROM app_metadata WHERE key = 'last_sync'").fetchone()
            if res:
                # Parse the ISO string
                dt = datetime.fromisoformat(res[0])

                # If the stored time has no timezone (old data), assume it was UTC (Streamlit Cloud default)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)

                # Convert to New York time
                ny_tz = ZoneInfo("America/New_York")
                dt_ny = dt.astimezone(ny_tz)

                # Format: YYYY-MM-DD HH:MM EDT/EST
                return dt_ny.strftime("%Y-%m-%d %H:%M %Z")
            return "Never"
        except Exception as e:
            logger.error(f"Time parsing error: {e}")
            return "Unknown"

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None