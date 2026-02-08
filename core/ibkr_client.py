import requests
import time
import logging
import xml.etree.ElementTree as ET
from typing import Optional

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IBKRFlexClient:
    """
    Handles communication with IBKR Flex Web Service.
    Documentation: https://www.interactivebrokers.com/en/software/am/am/reports/flex_queries.htm
    """

    BASE_URL = "https://ndcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest"

    def __init__(self, token: str, query_id: str):
        self.token = token
        self.query_id = query_id

    def request_report(self) -> Optional[str]:
        """Sends the initial request to IBKR to generate the report."""
        params = {
            't': self.token,
            'q': self.query_id,
            'v': '3'  # API Version
        }

        try:
            response = requests.get(self.BASE_URL, params=params)
            response.raise_for_status()

            # IBKR returns XML even for successful requests to provide a Reference Code
            root = ET.fromstring(response.content)

            if root.find('Status').text == 'Success':
                reference_code = root.find('ReferenceCode').text
                url = root.find('Url').text
                logger.info(f"Report request successful. Reference Code: {reference_code}")
                return reference_code, url
            else:
                error_msg = root.find('ErrorMessage').text
                logger.error(f"IBKR Error: {error_msg}")
                return None

        except Exception as e:
            logger.error(f"Failed to request report: {e}")
            return None

    def download_report(self, reference_code: str, download_url: str) -> Optional[str]:
        """
        Downloads the actual report content using the Reference Code.
        IBKR reports can take time to generate; this includes basic retry logic.
        """
        params = {
            't': self.token,
            'q': reference_code,
            'v': '3'
        }

        # Flex Query Web Service often requires a short wait
        # We'll implement a simple retry loop for Milestone 1
        for attempt in range(5):
            try:
                logger.info(f"Attempting to download report (Attempt {attempt + 1})...")
                response = requests.get(download_url, params=params)

                # If the report isn't ready, IBKR returns an XML error 1019
                if b"1019" in response.content and b"Statement generation in progress" in response.content:
                    logger.warning("Statement still generating... waiting 5 seconds.")
                    time.sleep(5)
                    continue

                response.raise_for_status()
                logger.info("Report downloaded successfully.")
                return response.text  # Usually XML or CSV content

            except Exception as e:
                logger.error(f"Download attempt failed: {e}")
                time.sleep(5)

        return None