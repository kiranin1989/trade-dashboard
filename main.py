import logging
import sys
import pandas as pd
from config import settings
from core.ibkr_client import IBKRFlexClient
from core.database import DatabaseManager
from core.parser import parse_ibkr_xml
from core.logic import PnLEngine  # <--- New Import

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def run_pipeline(fetch_new=False):
    """
    Executes the pipeline.
    Args:
        fetch_new (bool): If True, downloads from IBKR. If False, just runs Logic on DB.
    """
    db = DatabaseManager()

    if fetch_new:
        logger.info("--- Starting IBKR Data Download ---")
        try:
            client = IBKRFlexClient(token=settings.IBKR_TOKEN, query_id=settings.IBKR_QUERY_ID)
            result = client.request_report()
            if result:
                ref_code, url = result
                xml_content = client.download_report(ref_code, url)
                if xml_content:
                    data_map = parse_ibkr_xml(xml_content)
                    db.save_dataframe('trades', data_map.get('trades'))
                    db.save_dataframe('transactions', data_map.get('transactions'))
        except Exception as e:
            logger.error(f"Download failed: {e}")

    # --- LOGIC ENGINE STEP ---
    logger.info("--- Starting P&L Calculation ---")

    # 1. Load Executions from DB
    conn = db.get_connection()
    try:
        raw_trades_df = conn.execute("SELECT * FROM trades").df()

        if raw_trades_df.empty:
            logger.warning("No trades found in database.")
            return

        logger.info(f"Loaded {len(raw_trades_df)} raw executions.")

        # 2. Run FIFO Engine
        pnl_df = PnLEngine.calculate_fifo_pnl(raw_trades_df)

        if not pnl_df.empty:
            total_realized = pnl_df['net_pnl'].sum()
            total_trades = len(pnl_df)

            logger.info("--- RESULTS ---")
            logger.info(f"Total Closed Trades: {total_trades}")
            logger.info(f"Total Realized P&L: ${total_realized:,.2f}")

            # Show top winning trade
            best_trade = pnl_df.loc[pnl_df['net_pnl'].idxmax()]
            logger.info(f"Best Trade: {best_trade['symbol']} (${best_trade['net_pnl']:,.2f})")

            # Show top losing trade
            worst_trade = pnl_df.loc[pnl_df['net_pnl'].idxmin()]
            logger.info(f"Worst Trade: {worst_trade['symbol']} (${worst_trade['net_pnl']:,.2f})")

            # Optional: Save this processed data back to DB?
            # db.save_dataframe('closed_trades', pnl_df)
        else:
            logger.info("No closed trades generated (perhaps only open positions exists).")

    except Exception as e:
        logger.error(f"Logic Engine failed: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    # Set fetch_new=False to test logic on existing data without spamming IBKR
    run_pipeline(fetch_new=False)