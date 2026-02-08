import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from core.database import DatabaseManager
from core.logic import PnLEngine
import logging
import sys

# Configure logging to ensure output appears in Streamlit console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class DataService:
    def __init__(self):
        self.db = DatabaseManager()

    def get_processed_data(self):
        conn = self.db.get_connection()
        try:
            raw_trades_df = conn.execute(
                "SELECT * FROM trades WHERE asset_class NOT IN ('CASH') AND symbol NOT LIKE '%.%'").df()
            raw_cash_df = conn.execute("SELECT * FROM transactions").df()
            if raw_trades_df.empty: return pd.DataFrame(), pd.DataFrame()
            closed_df, open_df = PnLEngine.calculate_fifo_pnl(raw_trades_df, raw_cash_df)
            if not closed_df.empty:
                closed_df['close_date'] = pd.to_datetime(closed_df['close_date'])
                if 'entry_date' in closed_df.columns:
                    closed_df['entry_date'] = pd.to_datetime(closed_df['entry_date'])
            return closed_df, open_df
        except Exception as e:
            logger.error(f"Data Service failed: {e}")
            return pd.DataFrame(), pd.DataFrame()
        finally:
            self.db.close()

    def get_benchmark_data(self, symbol="^GSPC", start_date=None):
        """
        Fetches benchmark data. Handles yfinance MultiIndex column issues.
        """
        conn = self.db.get_connection()
        try:
            # 1. Check DB for last date
            try:
                # Ensure table exists (in case DB wasn't updated)
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS market_data (symbol VARCHAR, date TIMESTAMP, close DOUBLE, PRIMARY KEY (symbol, date))")
                res = conn.execute("SELECT MAX(date) FROM market_data WHERE symbol = ?", [symbol]).fetchone()
                last_db_date = res[0] if res and res[0] else None
            except Exception as e:
                logger.error(f"DB Check failed: {e}")
                last_db_date = None

            today = datetime.now().date()
            fetch_start = None

            if last_db_date is None:
                fetch_start = start_date if start_date else datetime(2020, 1, 1).date()
            elif last_db_date.date() < today:
                fetch_start = last_db_date.date() + timedelta(days=1)

            # 2. Update from Web if needed
            if fetch_start and fetch_start <= today:
                msg = f"Updating benchmark {symbol} from {fetch_start}..."
                logger.info(msg)
                print(f"DEBUG: {msg}")  # Direct print to force visibility in Streamlit console

                try:
                    df_yf = yf.download(symbol, start=fetch_start, progress=False)

                    if df_yf.empty:
                        logger.warning(
                            f"YFinance download returned empty dataframe for {symbol}. Check internet connection or symbol.")
                        print(f"DEBUG: YFinance download empty for {symbol}")
                    else:
                        # --- CRITICAL FIX: Flatten MultiIndex Columns ---
                        # yfinance often returns columns like ('Close', '^GSPC')
                        if isinstance(df_yf.columns, pd.MultiIndex):
                            df_yf.columns = df_yf.columns.get_level_values(0)
                        # -----------------------------------------------

                        # Reset index to make Date a column
                        df_to_save = df_yf.reset_index()

                        # Normalize columns to lowercase for safer matching
                        df_to_save.columns = [c.lower() for c in df_to_save.columns]

                        logger.info(f"Downloaded columns: {df_to_save.columns.tolist()}")

                        # Ensure we have the columns we expect
                        if 'date' in df_to_save.columns and 'close' in df_to_save.columns:
                            # CRITICAL FIX: Reorder columns to match DB Schema (symbol, date, close)
                            # DuckDB inserts by position, not name.
                            df_to_save['symbol'] = symbol

                            # Clean Date format
                            df_to_save['date'] = pd.to_datetime(df_to_save['date']).dt.tz_localize(None)

                            # Select and Order columns explicitly: symbol(1), date(2), close(3)
                            df_to_save = df_to_save[['symbol', 'date', 'close']]

                            self.db.save_dataframe('market_data', df_to_save)
                            logger.info(f"Saved {len(df_to_save)} rows to market_data.")
                        else:
                            logger.error(f"YFinance returned unexpected columns: {df_to_save.columns}")
                            print(f"DEBUG: Unexpected columns {df_to_save.columns}")

                except Exception as e:
                    logger.error(f"Web fetch failed: {e}")
                    import traceback
                    traceback.print_exc()

            # 3. Return Data
            query = "SELECT date, close FROM market_data WHERE symbol = ? ORDER BY date"
            params = [symbol]
            if start_date:
                query = "SELECT date, close FROM market_data WHERE symbol = ? AND date >= ? ORDER BY date"
                params = [symbol, start_date]

            df_db = conn.execute(query, params).df()
            if not df_db.empty:
                df_db['date'] = pd.to_datetime(df_db['date'])
                df_db = df_db.set_index('date')

            return df_db

        except Exception as e:
            logger.error(f"Benchmark error: {e}")
            return pd.DataFrame()
        finally:
            self.db.close()

    @staticmethod
    def apply_filters(df, symbols=None, date_range=None):
        if df.empty: return df
        filtered_df = df.copy()
        if symbols: filtered_df = filtered_df[filtered_df['root_symbol'].isin(symbols)]
        if date_range and len(date_range) == 2:
            start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
            filtered_df = filtered_df[(filtered_df['close_date'].dt.date >= start_date.date()) & (
                        filtered_df['close_date'].dt.date <= end_date.date())]
        return filtered_df