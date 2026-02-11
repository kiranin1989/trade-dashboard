import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from core.database import DatabaseManager
from core.logic import PnLEngine
from core.strategy_engine import StrategyEngine  # NEW
from core.campaign_engine import CampaignEngine  # NEW
from core.ibkr_client import IBKRFlexClient
from core.parser import parse_ibkr_xml
from config import settings
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

    def sync_ibkr_data(self):
        """
        Connects to IBKR, downloads the report, and saves to DB.
        """
        logger.info("Starting manual IBKR Sync...")
        try:
            client = IBKRFlexClient(token=settings.IBKR_TOKEN, query_id=settings.IBKR_QUERY_ID)

            result = client.request_report()
            if not result:
                return False, "Failed to initiate report request."

            ref_code, url = result

            xml_content = client.download_report(ref_code, url)
            if not xml_content:
                return False, "Download failed (empty content)."

            data_map = parse_ibkr_xml(xml_content)
            trades_df = data_map.get('trades')
            cash_df = data_map.get('transactions')

            count_t = len(trades_df)
            count_c = len(cash_df)

            if not trades_df.empty:
                self.db.save_dataframe('trades', trades_df)
            if not cash_df.empty:
                self.db.save_dataframe('transactions', cash_df)

            self.db.record_sync_time()

            return True, f"Synced {count_t} trades & {count_c} transactions."

        except Exception as e:
            logger.error(f"Sync Error: {e}")
            return False, str(e)

    def get_last_sync(self):
        return self.db.get_last_sync_time()

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

    # --- NEW ANALYTICS METHODS ---
    def get_strategy_data(self, closed_df):
        """Groups trades into Strategies (Verticals, Condors)."""
        if closed_df.empty: return pd.DataFrame()
        grouped = StrategyEngine.group_executions_into_strategies(closed_df)
        return StrategyEngine.aggregate_strategy_pnl(grouped)

    def get_campaign_data(self, closed_df):
        """Groups trades into Wheel Campaigns."""
        if closed_df.empty: return pd.DataFrame()
        grouped = CampaignEngine.identify_campaigns(closed_df)
        return CampaignEngine.aggregate_campaign_stats(grouped)

    # -----------------------------

    def get_benchmark_data(self, symbol="^GSPC", start_date=None):
        conn = self.db.get_connection()
        try:
            try:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS market_data (symbol VARCHAR, date TIMESTAMP, close DOUBLE, PRIMARY KEY (symbol, date))")
                res = conn.execute("SELECT MAX(date) FROM market_data WHERE symbol = ?", [symbol]).fetchone()
                last_db_date = res[0] if res and res[0] else None
            except Exception:
                last_db_date = None

            today = datetime.now().date()
            fetch_start = None

            if last_db_date is None:
                fetch_start = start_date if start_date else datetime(2020, 1, 1).date()
            elif last_db_date.date() < today:
                fetch_start = last_db_date.date() + timedelta(days=1)

            if fetch_start and fetch_start <= today:
                msg = f"Updating benchmark {symbol} from {fetch_start}..."
                logger.info(msg)
                try:
                    df_yf = yf.download(symbol, start=fetch_start, progress=False)
                    if not df_yf.empty:
                        if isinstance(df_yf.columns, pd.MultiIndex):
                            df_yf.columns = df_yf.columns.get_level_values(0)
                        df_to_save = df_yf.reset_index()
                        df_to_save.columns = [c.lower() for c in df_to_save.columns]
                        if 'date' in df_to_save.columns and 'close' in df_to_save.columns:
                            df_to_save['symbol'] = symbol
                            df_to_save['date'] = pd.to_datetime(df_to_save['date']).dt.tz_localize(None)
                            df_to_save = df_to_save[['symbol', 'date', 'close']]
                            self.db.save_dataframe('market_data', df_to_save)
                except Exception as e:
                    logger.error(f"Web fetch failed: {e}")

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
        except Exception:
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