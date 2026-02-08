import pandas as pd
import xml.etree.ElementTree as ET
import logging

logger = logging.getLogger(__name__)


def parse_ibkr_xml(xml_content: str) -> dict:
    """
    Parses IBKR Flex Query XML content into a dictionary of DataFrames.
    Includes Multiplier for correct Options P&L.
    """
    try:
        root = ET.fromstring(xml_content)
        trades_data = []

        for trade in root.findall(".//Trade"):
            strike = trade.get('strike')
            strike_val = float(strike) if strike else None

            # IBKR Multiplier (Default to 1 if missing/stock)
            mult = float(trade.get('multiplier') or 1)

            trades_data.append({
                'trade_id': trade.get('tradeID'),
                'symbol': trade.get('symbol'),
                'description': trade.get('description'),
                'asset_class': trade.get('assetCategory'),
                'trade_date': trade.get('dateTime'),
                'quantity': float(trade.get('quantity') or 0),
                'price': float(trade.get('tradePrice') or 0),
                'commission': float(trade.get('ibCommission') or 0),
                'realized_pnl': 0.0,
                'currency': 'USD',
                'flex_query_run_id': str(mult),  # Temporary hijacking this col to store multiplier for now
                'buy_sell': trade.get('buySell'),
                'open_close': trade.get('openCloseIndicator'),
                'close_price': float(trade.get('closePrice') or 0),
                'underlying': trade.get('underlyingSymbol'),
                'strike': strike_val,
                'expiry': trade.get('expiry'),
                'put_call': trade.get('putCall'),
            })

        return {
            'trades': pd.DataFrame(trades_data),
            'transactions': pd.DataFrame()  # Simplified for now
        }
    except Exception as e:
        logger.error(f"XML Parsing failed: {e}")
        return {'trades': pd.DataFrame(), 'transactions': pd.DataFrame()}