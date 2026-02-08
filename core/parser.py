import pandas as pd
import xml.etree.ElementTree as ET
import logging

logger = logging.getLogger(__name__)

def parse_ibkr_xml(xml_content: str) -> dict:
    try:
        root = ET.fromstring(xml_content)
        trades_data = []
        
        for trade in root.findall(".//Trade"):
            strike = trade.get('strike')
            strike_val = float(strike) if strike else None
            
            # Extract Multiplier (Default to 1.0 for Stocks)
            raw_mult = trade.get('multiplier')
            multiplier = float(raw_mult) if raw_mult else 1.0
            
            # ORDER MUST MATCH DATABASE.PY EXACTLY
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
                'flex_query_run_id': '', 
                'buy_sell': trade.get('buySell'),
                'open_close': trade.get('openCloseIndicator'),
                'close_price': float(trade.get('closePrice') or 0),
                'underlying': trade.get('underlyingSymbol'),
                'strike': strike_val,
                'expiry': trade.get('expiry'),
                'put_call': trade.get('putCall'),
                'multiplier': multiplier # <--- Mapped to new column
            })
        
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