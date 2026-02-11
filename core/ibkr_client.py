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
    Includes headers to avoid bot detection in Cloud environments.
    """

    BASE_URL = "https://ndcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest"

    def __init__(self, token: str, query_id: str):
        self.token = token
        self.query_id = query_id
        # IBKR sometimes blocks requests without a User-Agent
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def request_report(self) -> Optional[str]:
        """Sends the initial request to IBKR to generate the report."""
        params = {
            't': self.token,
            'q': self.query_id,
            'v': '3'
        }

        try:
            logger.info("Sending Report Request to IBKR...")
            response = requests.get(self.BASE_URL, params=params, headers=self.headers)
            response.raise_for_status()

            # IBKR returns XML
            root = ET.fromstring(response.content)

            if root.find('Status').text == 'Success':
                reference_code = root.find('ReferenceCode').text
                url = root.find('Url').text
                logger.info(f"Report request successful. Reference Code: {reference_code}")
                return reference_code, url
            else:
                error_msg = root.find('ErrorMessage').text
                code = root.find('ErrorCode').text if root.find('ErrorCode') is not None else "Unknown"
                logger.error(f"IBKR Request Error [{code}]: {error_msg}")
                return None

        except Exception as e:
            logger.error(f"Failed to request report: {e}")
            if 'response' in locals() and response.content:
                logger.error(f"Raw Response: {response.text}")
            return None

    def download_report(self, reference_code: str, download_url: str) -> Optional[str]:
        """
        Downloads the actual report.
        Retries up to 10 times to handle 'Statement generation in progress'.
        """
        params = {
            't': self.token,
            'q': reference_code,
            'v': '3'
        }

        # Increased retries for Cloud environments
        max_retries = 10

        for attempt in range(max_retries):
            try:
                logger.info(f"Downloading Report (Attempt {attempt + 1}/{max_retries})...")
                response = requests.get(download_url, params=params, headers=self.headers)

                # Check for "Generation in Progress" (Error 1019)
                if b"1019" in response.content and b"Statement generation in progress" in response.content:
                    logger.warning("Statement generating... waiting 3 seconds.")
                    time.sleep(3)
                    continue

                # Check for other XML Errors
                if b"<ErrorCode>" in response.content and b"<ErrorMessage>" in response.content:
                    root = ET.fromstring(response.content)
                    err = root.find('ErrorMessage').text
                    code = root.find('ErrorCode').text
                    logger.error(f"IBKR Download Error [{code}]: {err}")
                    # If it's a fatal error (not 1019), stop trying
                    if code != "1019":
                        return None

                response.raise_for_status()

                # Basic validation: ensure we got some data
                if len(response.text) < 50:
                    logger.error(f"Downloaded content seems too short/empty: {response.text}")
                    return None

                logger.info("Report downloaded successfully.")
                return response.text

            except Exception as e:
                logger.error(f"Download attempt failed: {e}")
                time.sleep(3)

        logger.error("Max retries exceeded. Report download failed.")
        return None