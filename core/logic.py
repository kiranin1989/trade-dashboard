import pandas as pd
import logging
from collections import deque

logger = logging.getLogger(__name__)


class PnLEngine:
    """
    Core Financial Logic.
    Calculates Realized P&L using FIFO (First-In-First-Out) methodology.
    """

    @staticmethod
    def _generate_asset_key(row):
        asset_class = str(row.get('asset_class', ''))
        root = row.get('underlying') if row.get('underlying') else row.get('symbol')
        if 'OPT' in asset_class or 'FOP' in asset_class:
            return f"{root} {row.get('expiry')} {row.get('strike')} {row.get('put_call')}"
        return root

    @staticmethod
    def calculate_fifo_pnl(trades_df: pd.DataFrame, cash_df: pd.DataFrame = None):
        if trades_df.empty:
            return pd.DataFrame(), pd.DataFrame()

        trades_df = trades_df.sort_values(by='trade_date')
        closed_trades = []
        portfolio = {}

        # --- PART 1: PROCESS TRADES (FIFO) ---
        for _, row in trades_df.iterrows():
            asset_key = PnLEngine._generate_asset_key(row)
            root_symbol = row.get('underlying') if row.get('underlying') else row.get('symbol')

            # Capture Option Metadata for Strategy Analysis
            # Use .get() to avoid errors if cols missing in raw data
            meta_asset_class = row.get('asset_class')
            meta_put_call = row.get('put_call')
            meta_strike = row.get('strike')
            meta_expiry = row.get('expiry')

            qty = float(row['quantity'])
            price = float(row['price'])
            comm = float(row['commission'])
            multiplier = float(row['multiplier']) if row.get('multiplier') and pd.notna(row['multiplier']) else 1.0

            # Check for IBKR Code (Assignment/Exercise)
            code = str(row.get('code', ''))

            if str(row['buy_sell']).upper() in ['SELL', 'SLD'] and qty > 0:
                qty = -qty

            current_comm_per_unit = comm / abs(qty) if qty != 0 else 0.0

            if asset_key not in portfolio:
                portfolio[asset_key] = deque()
            inventory = portfolio[asset_key]

            # Match FIFO
            if not inventory or (inventory[0]['qty'] > 0 and qty > 0) or (inventory[0]['qty'] < 0 and qty < 0):
                inventory.append({
                    'qty': qty, 'price': price, 'date': row['trade_date'],
                    'mult': multiplier, 'comm_per_unit': current_comm_per_unit
                })
            else:
                remaining_qty = qty
                while remaining_qty != 0 and inventory:
                    lot = inventory[0]

                    # Close Reason
                    if 'A' in code:
                        close_reason = "Assigned"
                    elif 'Ex' in code:
                        close_reason = "Exercised"
                    elif 'Ep' in code:
                        close_reason = "Expired"
                    elif ('OPT' in str(meta_asset_class)) and price == 0.0:
                        close_reason = "Expired"
                    else:
                        close_reason = "Trade"

                    if abs(remaining_qty) >= abs(lot['qty']):
                        matched_q = lot['qty']
                        inventory.popleft()
                        remaining_qty -= (-matched_q)

                        direction = 1 if matched_q > 0 else -1
                        gross_pnl = (price - lot['price']) * abs(matched_q) * lot['mult'] * direction
                        total_comm = (abs(matched_q) * lot['comm_per_unit']) + (abs(matched_q) * current_comm_per_unit)
                        net_pnl = gross_pnl + total_comm

                        closed_trades.append({
                            'root_symbol': root_symbol,
                            'asset_id': asset_key,
                            'quantity': abs(matched_q),
                            'entry_date': lot['date'],
                            'close_date': row['trade_date'],
                            'commission': total_comm,
                            'net_pnl': net_pnl,
                            'close_reason': close_reason,
                            # NEW METADATA
                            'asset_class': meta_asset_class,
                            'put_call': meta_put_call,
                            'strike': meta_strike,
                            'expiry': meta_expiry
                        })
                    else:
                        matched_q = -remaining_qty
                        lot['qty'] -= matched_q

                        direction = 1 if matched_q > 0 else -1
                        gross_pnl = (price - lot['price']) * abs(matched_q) * lot['mult'] * direction
                        total_comm = (abs(matched_q) * lot['comm_per_unit']) + (abs(matched_q) * current_comm_per_unit)
                        net_pnl = gross_pnl + total_comm

                        closed_trades.append({
                            'root_symbol': root_symbol,
                            'asset_id': asset_key,
                            'quantity': abs(matched_q),
                            'entry_date': lot['date'],
                            'close_date': row['trade_date'],
                            'commission': total_comm,
                            'net_pnl': net_pnl,
                            'close_reason': close_reason,
                            # NEW METADATA
                            'asset_class': meta_asset_class,
                            'put_call': meta_put_call,
                            'strike': meta_strike,
                            'expiry': meta_expiry
                        })
                        remaining_qty = 0
                if remaining_qty != 0:
                    inventory.append({
                        'qty': remaining_qty, 'price': price, 'date': row['trade_date'],
                        'mult': multiplier, 'comm_per_unit': current_comm_per_unit
                    })

        # --- PART 2: PROCESS DIVIDENDS ---
        if cash_df is not None and not cash_df.empty:
            div_types = ['Dividends', 'PaymentInLieuOfDividends', 'WithholdingTax']
            divs = cash_df[cash_df['type'].isin(div_types)]

            for _, row in divs.iterrows():
                closed_trades.append({
                    'root_symbol': row['symbol'],
                    'asset_id': 'DIVIDEND',
                    'quantity': 0,
                    'entry_date': row['date'],
                    'close_date': row['date'],
                    'commission': 0.0,
                    'net_pnl': float(row['amount']),
                    'close_reason': row['type'],
                    'asset_class': 'CASH',  # Placeholder
                    'put_call': None,
                    'strike': None,
                    'expiry': None
                })

        # --- PART 3: CALCULATE OPEN POSITIONS ---
        open_pos_list = []
        for asset_id, lots in portfolio.items():
            total_qty = sum(l['qty'] for l in lots)
            if abs(total_qty) > 0.00001:
                root = asset_id.split(' ')[0]
                avg_price = sum(l['price'] * abs(l['qty']) for l in lots) / abs(total_qty)
                open_pos_list.append({
                    'root_symbol': root,
                    'asset_id': asset_id,
                    'quantity': total_qty,
                    'avg_price': avg_price
                })

        return pd.DataFrame(closed_trades), pd.DataFrame(open_pos_list)