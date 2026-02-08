import pandas as pd
from core.database import DatabaseManager
from core.logic import PnLEngine
import logging

logger = logging.getLogger(__name__)


class DataService:
    """
    Acts as a bridge between the Database/Logic and the Streamlit UI.
    """

    def __init__(self):
        self.db = DatabaseManager()

    def get_processed_data(self):
        conn = self.db.get_connection()
        try:
            # Load raw executions
            raw_trades_df = conn.execute("""
                SELECT * FROM trades 
                WHERE asset_class NOT IN ('CASH')
                AND symbol NOT LIKE '%.%'
            """).df()

            if raw_trades_df.empty:
                return pd.DataFrame(), pd.DataFrame()

            # Run Logic Engine
            closed_df, open_df = PnLEngine.calculate_fifo_pnl(raw_trades_df)

            # Ensure date formats are proper for UI
            if not closed_df.empty:
                closed_df['close_date'] = pd.to_datetime(closed_df['close_date'])
                if 'entry_date' in closed_df.columns:
                    closed_df['entry_date'] = pd.to_datetime(closed_df['entry_date'])

            return closed_df, open_df
        except Exception as e:
            logger.error(f"Data Service failed to fetch data: {e}")
            return pd.DataFrame(), pd.DataFrame()
        finally:
            self.db.close()

    @staticmethod
    def apply_filters(df, symbols=None, date_range=None, asset_types=None):
        if df.empty:
            return df
        filtered_df = df.copy()

        # NOTE: Using 'root_symbol' for filtering now
        if symbols:
            filtered_df = filtered_df[filtered_df['root_symbol'].isin(symbols)]

        if date_range and len(date_range) == 2:
            start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
            filtered_df = filtered_df[(filtered_df['close_date'].dt.date >= start_date.date()) &
                                      (filtered_df['close_date'].dt.date <= end_date.date())]

        return filtered_df