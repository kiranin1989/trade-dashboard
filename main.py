import logging
from config import settings
from core.ibkr_client import IBKRFlexClient

# Setup logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def verify_setup():
    print("--- Environment Verification ---")
    print(f"Database URL: {settings.DATABASE_URL}")
    print(f"Is Offline: {settings.IS_OFFLINE}")

    # Check if tokens are loaded (masking for security)
    token_status = "Loaded" if settings.IBKR_TOKEN else "MISSING"
    query_status = "Loaded" if settings.IBKR_QUERY_ID else "MISSING"

    print(f"IBKR Token: {token_status}")
    print(f"IBKR Query ID: {query_status}")
    print("--------------------------------\n")

    if not settings.IBKR_TOKEN or not settings.IBKR_QUERY_ID:
        logger.error("Stop: missing credentials in .env file.")
        return

    # Initialize the client from the Canvas code
    client = IBKRFlexClient(token=settings.IBKR_TOKEN, query_id=settings.IBKR_QUERY_ID)
    logger.info("IBKRFlexClient initialized successfully.")

    # Uncomment the lines below only if you want to perform a live test:
    # result = client.request_report()
    # if result:
    #     ref_code, url = result
    #     data = client.download_report(ref_code, url)
    #     print("Received Data Sample:", data[:200] if data else "No data received")


if __name__ == "__main__":
    verify_setup()