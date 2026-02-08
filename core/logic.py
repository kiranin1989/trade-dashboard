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
        """
        Creates a unique identifier for the asset.
        """
        asset_class = str(row.get('asset_class', ''))
        # Use underlying if available, else symbol
        root = row.get('underlying') if row.get('underlying') else row.get('symbol')

        if 'OPT' in asset_class or 'FOP' in asset_class:
            return f"{root} {row.get('expiry')} {row.get('strike')} {row.get('put_call')}"
        return root

    @staticmethod
    def calculate_fifo_pnl(trades_df: pd.DataFrame):
        """
        Process executions to calculate Realized P&L and Open Positions.
        Relies on 'multiplier' column being present and correct in the input DataFrame.
        """
        if trades_df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # Ensure sorted by date to respect FIFO
        trades_df = trades_df.sort_values(by='trade_date')

        closed_trades = []
        portfolio = {}

        for _, row in trades_df.iterrows():
            asset_key = PnLEngine._generate_asset_key(row)

            # Extract root symbol for UI grouping
            root_symbol = row.get('underlying') if row.get('underlying') else row.get('symbol')

            qty = float(row['quantity'])
            price = float(row['price'])
            comm = float(row['commission'])

            # Strict reliance on Database Column
            # If this is 1.0 for options, the upstream IBKR Flex Query needs to be updated.
            multiplier = float(row['multiplier']) if row.get('multiplier') and pd.notna(row['multiplier']) else 1.0

            # Normalize Sell Quantity
            if str(row['buy_sell']).upper() in ['SELL', 'SLD'] and qty > 0:
                qty = -qty

            # Calculate commission per unit for this specific execution
            current_comm_per_unit = comm / abs(qty) if qty != 0 else 0.0

            if asset_key not in portfolio:
                portfolio[asset_key] = deque()
            inventory = portfolio[asset_key]

            # --- FIFO MATCHING LOGIC ---

            # CASE 1: Open/Add Position (Same Sign)
            if not inventory or (inventory[0]['qty'] > 0 and qty > 0) or (inventory[0]['qty'] < 0 and qty < 0):
                inventory.append({
                    'qty': qty,
                    'price': price,
                    'date': row['trade_date'],
                    'mult': multiplier,
                    'comm_per_unit': current_comm_per_unit
                })

            # CASE 2: Close Position (Opposite Sign)
            else:
                remaining_qty = qty
                while remaining_qty != 0 and inventory:
                    lot = inventory[0]

                    if abs(remaining_qty) >= abs(lot['qty']):
                        # FULL MATCH of the Inventory Lot
                        matched_q = lot['qty']
                        inventory.popleft()
                        remaining_qty -= (-matched_q)

                        # Gross P&L
                        direction = 1 if matched_q > 0 else -1
                        gross_pnl = (price - lot['price']) * abs(matched_q) * lot['mult'] * direction

                        # Net P&L (Gross + Negative Commissions)
                        entry_comm = abs(matched_q) * lot['comm_per_unit']
                        exit_comm = abs(matched_q) * current_comm_per_unit
                        total_comm = entry_comm + exit_comm
                        net_pnl = gross_pnl + total_comm

                        closed_trades.append({
                            'root_symbol': root_symbol,
                            'symbol': row['symbol'],
                            'asset_id': asset_key,
                            'close_date': row['trade_date'],
                            'quantity': abs(matched_q),
                            'net_pnl': net_pnl,
                            'commission': total_comm
                        })
                    else:
                        # PARTIAL MATCH of the Inventory Lot
                        matched_q = -remaining_qty
                        lot['qty'] -= matched_q

                        # Gross P&L
                        direction = 1 if matched_q > 0 else -1
                        gross_pnl = (price - lot['price']) * abs(matched_q) * lot['mult'] * direction

                        # Net P&L
                        entry_comm = abs(matched_q) * lot['comm_per_unit']
                        exit_comm = abs(matched_q) * current_comm_per_unit
                        total_comm = entry_comm + exit_comm
                        net_pnl = gross_pnl + total_comm

                        closed_trades.append({
                            'root_symbol': root_symbol,
                            'symbol': row['symbol'],
                            'asset_id': asset_key,
                            'close_date': row['trade_date'],
                            'quantity': abs(matched_q),
                            'net_pnl': net_pnl,
                            'commission': total_comm
                        })
                        remaining_qty = 0

                if remaining_qty != 0:
                    # Add remainder to inventory (Flip Position)
                    inventory.append({
                        'qty': remaining_qty,
                        'price': price,
                        'date': row['trade_date'],
                        'mult': multiplier,
                        'comm_per_unit': current_comm_per_unit
                    })

        # Calculate Open Positions for UI
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