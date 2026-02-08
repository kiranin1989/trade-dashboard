import logging
import sys
from config import settings
from core.ibkr_client import IBKRFlexClient
from core.database import DatabaseManager
from core.parser import parse_ibkr_xml

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def run_pipeline():
    """
    Executes the full data ingestion pipeline:
    Fetch -> Parse -> Store
    """
    logger.info("--- Starting IBKR Data Ingestion ---")

    # 1. Initialize Components
    try:
        db = DatabaseManager()
        client = IBKRFlexClient(token=settings.IBKR_TOKEN, query_id=settings.IBKR_QUERY_ID)
    except Exception as e:
        logger.critical(f"Initialization failed: {e}")
        return

    # 2. Fetch Data from IBKR
    logger.info("Requesting report from IBKR...")
    result = client.request_report()

    if not result:
        logger.error("Failed to retrieve report reference. Aborting.")
        return

    ref_code, url = result
    logger.info(f"Report ready. Downloading with Ref: {ref_code}")

    xml_content = client.download_report(ref_code, url)
    if not xml_content:
        logger.error("Download failed or returned empty content.")
        return

    # 3. Parse Data
    logger.info("Parsing XML content...")
    data_map = parse_ibkr_xml(xml_content)

    trades_df = data_map.get('trades')
    cash_df = data_map.get('transactions')

    logger.info(f"Parsed {len(trades_df)} trades/executions and {len(cash_df)} cash transactions.")

    # 4. Save to Database
    if not trades_df.empty:
        logger.info("Saving trades to database...")
        db.save_dataframe('trades', trades_df)

    if not cash_df.empty:
        logger.info("Saving transactions to database...")
        db.save_dataframe('transactions', cash_df)

    # 5. Verification (Read back to confirm)
    verify_ingestion(db)

    # Cleanup
    db.close()
    logger.info("--- Pipeline Completed Successfully ---")


def verify_ingestion(db: DatabaseManager):
    """
    Simple query to show the user what is currently in the DB.
    """
    conn = db.get_connection()
    try:
        # Check Trades
        trade_count = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        logger.info(f"Current Total Trades in DB: {trade_count}")

        # Check Transactions
        trans_count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        logger.info(f"Current Total Transactions in DB: {trans_count}")

        # Show a sample if data exists
        if trade_count > 0:
            logger.info("Sample Trade Data:")
            print(conn.execute("SELECT symbol, trade_date, quantity, price FROM trades LIMIT 3").df())

    except Exception as e:
        logger.error(f"Verification query failed: {e}")


if __name__ == "__main__":
    run_pipeline()