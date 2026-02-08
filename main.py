import logging
import sys
import pandas as pd
from config import settings
from core.ibkr_client import IBKRFlexClient
from core.database import DatabaseManager
from core.parser import parse_ibkr_xml
from core.logic import PnLEngine

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

    conn = db.get_connection()
    try:
        # Load executions, excluding Cash/Forex pairs like USD.CAD
        raw_trades_df = conn.execute("""
            SELECT * FROM trades 
            WHERE asset_class NOT IN ('CASH')
            AND symbol NOT LIKE '%.%'
        """).df()

        if raw_trades_df.empty:
            logger.warning("No trades found in database (after filtering out CASH/Forex).")
            return

        logger.info(f"Loaded {len(raw_trades_df)} raw executions.")

        # Run FIFO Engine
        closed_df, open_df = PnLEngine.calculate_fifo_pnl(raw_trades_df)

        if not closed_df.empty:
            print("\n--- PERFORMANCE SUMMARY ---")
            print(f"Total Closed Trades: {len(closed_df)}")
            print(f"Total Realized P&L: ${closed_df['net_pnl'].sum():,.2f}")
            print(f"Total Open Assets:   {len(open_df)}")

            # --- OPEN STOCKS ---
            stocks_open = open_df[~open_df['asset_id'].str.contains(' ')]
            if not stocks_open.empty:
                print("\n--- OPEN STOCK POSITIONS ---")
                print(stocks_open.sort_values('quantity', ascending=False))

            # --- OPEN OPTIONS ---
            # Options asset_id contains spaces: "CCJ 20250919 75.0 P"
            options_open = open_df[open_df['asset_id'].str.contains(' ')]
            if not options_open.empty:
                print("\n--- OPEN OPTION POSITIONS ---")
                print(options_open.sort_values('quantity', ascending=False))
            else:
                print("\n--- NO OPEN OPTION POSITIONS ---")

        else:
            logger.info("No closed trades generated.")

    except Exception as e:
        logger.error(f"Logic Engine failed: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    # Usually fetch_new=True once to get data, then False to iterate on logic
    run_pipeline(fetch_new=False)