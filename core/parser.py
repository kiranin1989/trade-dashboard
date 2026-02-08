import pandas as pd
import xml.etree.ElementTree as ET
import logging

logger = logging.getLogger(__name__)


def parse_ibkr_xml(xml_content: str) -> dict:
    """
    Parses IBKR Flex Query XML content into a dictionary of DataFrames.
    Aligned strictly with core/database.py schema order.
    """
    try:
        root = ET.fromstring(xml_content)

        # 1. Parse Trades (Executions)
        trades_data = []

        # IBKR often uses the <Trade> tag for rows in the Executions section
        for trade in root.findall(".//Trade"):
            # Extract Option specific fields safely
            strike = trade.get('strike')
            strike_val = float(strike) if strike else None

            # CRITICAL: This dictionary order MUST match the CREATE TABLE order in database.py
            trades_data.append({
                # 1. trade_id
                'trade_id': trade.get('tradeID'),

                # 2. symbol
                'symbol': trade.get('symbol'),

                # 3. description
                'description': trade.get('description'),

                # 4. asset_class
                'asset_class': trade.get('assetCategory'),

                # 5. trade_date
                'trade_date': trade.get('dateTime'),

                # 6. quantity
                'quantity': float(trade.get('quantity') or 0),

                # 7. price
                'price': float(trade.get('tradePrice') or 0),

                # 8. commission
                'commission': float(trade.get('ibCommission') or 0),

                # 9. realized_pnl (Default to 0 for executions)
                'realized_pnl': 0.0,

                # 10. currency
                'currency': 'USD',

                # 11. flex_query_run_id (Placeholder to match schema count)
                'flex_query_run_id': '',

                # 12. buy_sell
                'buy_sell': trade.get('buySell'),

                # 13. open_close
                'open_close': trade.get('openCloseIndicator'),

                # 14. close_price
                'close_price': float(trade.get('closePrice') or 0),

                # 15. underlying
                'underlying': trade.get('underlyingSymbol'),

                # 16. strike
                'strike': strike_val,

                # 17. expiry
                'expiry': trade.get('expiry'),

                # 18. put_call
                'put_call': trade.get('putCall'),
            })

        # 2. Parse Cash Transactions (Dividends, etc.)
        cash_data = []
        for ct in root.findall(".//CashTransaction"):
            cash_data.append({
                'transaction_id': ct.get('transactionID'),
                'type': ct.get('type'),
                'asset_class': ct.get('assetCategory'),
                'symbol': ct.get('symbol'),
                'amount': float(ct.get('amount') or 0),
                'date': ct.get('dateTime'),
                'description': ct.get('description'),
                'currency': 'USD'
            })

        return {
            'trades': pd.DataFrame(trades_data),
            'transactions': pd.DataFrame(cash_data)
        }
    except Exception as e:
        logger.error(f"XML Parsing failed: {e}")
        return {'trades': pd.DataFrame(), 'transactions': pd.DataFrame()}