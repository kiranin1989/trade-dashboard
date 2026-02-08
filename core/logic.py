import pandas as pd
import logging
from collections import deque

logger = logging.getLogger(__name__)


class PnLEngine:
    @staticmethod
    def _generate_asset_key(row):
        asset_class = str(row.get('asset_class', ''))
        if 'OPT' in asset_class or 'FOP' in asset_class:
            root = row.get('underlying') if row.get('underlying') else row.get('symbol')
            return f"{root} {row.get('expiry')} {row.get('strike')} {row.get('put_call')}"
        return row.get('symbol')

    @staticmethod
    def calculate_fifo_pnl(trades_df: pd.DataFrame):
        if trades_df.empty:
            return pd.DataFrame(), pd.DataFrame()

        trades_df = trades_df.sort_values(by='trade_date')
        closed_trades = []
        portfolio = {}  # Key -> deque of open lots

        for _, row in trades_df.iterrows():
            asset_key = PnLEngine._generate_asset_key(row)
            qty = float(row['quantity'])
            price = float(row['price'])
            comm = float(row['commission'])
            # We stored multiplier in flex_query_run_id in the parser update
            multiplier = float(row['flex_query_run_id']) if row['flex_query_run_id'] else 1.0

            if str(row['buy_sell']).upper() in ['SELL', 'SLD'] and qty > 0:
                qty = -qty

            if asset_key not in portfolio:
                portfolio[asset_key] = deque()
            inventory = portfolio[asset_key]

            # Match FIFO
            if not inventory or (inventory[0]['qty'] > 0 and qty > 0) or (inventory[0]['qty'] < 0 and qty < 0):
                inventory.append(
                    {'qty': qty, 'price': price, 'date': row['trade_date'], 'mult': multiplier, 'comm_total': comm})
            else:
                remaining_qty = qty
                while remaining_qty != 0 and inventory:
                    lot = inventory[0]
                    if abs(remaining_qty) >= abs(lot['qty']):
                        matched_q = lot['qty']
                        inventory.popleft()
                        remaining_qty -= (-matched_q)

                        pnl = (price - lot['price']) * abs(matched_q) * lot['mult'] * (1 if matched_q > 0 else -1)
                        closed_trades.append({
                            'symbol': row['symbol'], 'asset_id': asset_key, 'close_date': row['trade_date'],
                            'quantity': abs(matched_q), 'net_pnl': pnl - abs(comm * (matched_q / qty))
                        })
                    else:
                        matched_q = -remaining_qty
                        lot['qty'] -= matched_q
                        pnl = (price - lot['price']) * abs(matched_q) * lot['mult'] * (1 if matched_q > 0 else -1)
                        closed_trades.append({
                            'symbol': row['symbol'], 'asset_id': asset_key, 'close_date': row['trade_date'],
                            'quantity': abs(matched_q), 'net_pnl': pnl - abs(comm * (matched_q / qty))
                        })
                        remaining_qty = 0
                if remaining_qty != 0:
                    inventory.append(
                        {'qty': remaining_qty, 'price': price, 'date': row['trade_date'], 'mult': multiplier,
                         'comm_total': comm})

        # Calculate Open Positions
        open_pos_list = []
        for asset_id, lots in portfolio.items():
            total_qty = sum(l['qty'] for l in lots)
            if abs(total_qty) > 0.00001:
                avg_price = sum(l['price'] * abs(l['qty']) for l in lots) / abs(total_qty)
                open_pos_list.append({'asset_id': asset_id, 'quantity': total_qty, 'avg_price': avg_price})

        return pd.DataFrame(closed_trades), pd.DataFrame(open_pos_list)