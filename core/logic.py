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
    def calculate_fifo_pnl(trades_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process a DataFrame of raw executions and return a DataFrame of CLOSED trades with P&L.

        Expected Input Columns:
        - symbol, asset_class, trade_date, quantity, price, commission, buy_sell

        Returns:
        - DataFrame with [symbol, close_date, quantity, entry_price, exit_price, gross_pnl, net_pnl]
        """
        if trades_df.empty:
            return pd.DataFrame()

        # Ensure sorted by date
        trades_df = trades_df.sort_values(by='trade_date')

        closed_trades = []

        # We need to track inventory per symbol
        # Structure: { 'AAPL': deque([ {'qty': 10, 'price': 150, 'date': ...}, ... ]) }
        portfolio = {}

        for index, row in trades_df.iterrows():
            symbol = row['symbol']
            qty = row['quantity']  # Positive for Buy (usually), check buy_sell logic below if needed
            price = row['price']
            date = row['trade_date']
            comm = row['commission']

            # Normalize Quantity based on Buy/Sell if IBKR data is absolute
            # (In your parser, you might want to ensure 'SLD'/'SELL' makes quantity negative if it isn't already)
            # Assuming here: Signed quantity is not guaranteed, so we check Buy/Sell
            if row['buy_sell'] == 'SELL' and qty > 0:
                qty = -qty

            if symbol not in portfolio:
                portfolio[symbol] = deque()

            inventory = portfolio[symbol]

            # CASE 1: Opening or Adding to Position (Same sign)
            # If inventory is empty, or new trade has same sign as inventory (Long+Long or Short+Short)
            if not inventory or (inventory[0]['qty'] > 0 and qty > 0) or (inventory[0]['qty'] < 0 and qty < 0):
                inventory.append({
                    'qty': qty,
                    'price': price,
                    'date': date,
                    'comm_per_share': comm / abs(qty) if qty != 0 else 0
                })

            # CASE 2: Closing Position (Opposite sign)
            else:
                remaining_qty = qty  # E.g., Selling -50

                while remaining_qty != 0 and inventory:
                    match_lot = inventory[0]  # FIFO: Look at oldest lot

                    # Determine how much we can match
                    # If matching -50 against +100, we match 50.
                    # If matching -50 against +20, we match 20 and look for next lot.

                    if abs(remaining_qty) >= abs(match_lot['qty']):
                        # We exhaust this lot completely
                        matched_q = match_lot['qty']
                        entry_price = match_lot['price']
                        entry_date = match_lot['date']

                        # Remove this lot from inventory
                        inventory.popleft()

                        # Adjust remaining to close
                        remaining_qty -= (-matched_q)  # e.g. -50 - (-20) = -30

                        # Record the P&L
                        # P&L = (Exit Price - Entry Price) * Quantity
                        # Note: If Long (+), Exit is Sell (-).
                        # Calculation: (ExitPrice - EntryPrice) * MatchedQty (Positive)
                        # Wait, easier math: (Price_Now - Price_Old) * Direction?

                        # Let's match standard formula: (SellPrice - BuyPrice) * Qty
                        if matched_q > 0:  # We were Long, now Selling
                            pnl = (price - entry_price) * abs(matched_q)
                        else:  # We were Short, now Buying
                            pnl = (entry_price - price) * abs(matched_q)

                        closed_trades.append({
                            'symbol': symbol,
                            'entry_date': entry_date,
                            'close_date': date,
                            'quantity': abs(matched_q),
                            'entry_price': entry_price,
                            'exit_price': price,
                            'gross_pnl': pnl,
                            # Allocate commission proportionally (This trade comm + Prorated Entry comm)
                            'commission': (comm * (abs(matched_q) / abs(qty))) + (
                                        match_lot['comm_per_share'] * abs(matched_q)),
                            'asset_class': row['asset_class']
                        })

                    else:
                        # Partial Close: We only close a portion of the oldest lot
                        # Trade is -10, Lot is +100. We close 10.
                        matched_q = -remaining_qty  # The amount we are closing

                        match_lot['qty'] -= matched_q  # Reduce lot size

                        entry_price = match_lot['price']

                        if matched_q > 0:  # We were Long
                            pnl = (price - entry_price) * abs(matched_q)
                        else:
                            pnl = (entry_price - price) * abs(matched_q)

                        closed_trades.append({
                            'symbol': symbol,
                            'entry_date': match_lot['date'],
                            'close_date': date,
                            'quantity': abs(matched_q),
                            'entry_price': entry_price,
                            'exit_price': price,
                            'gross_pnl': pnl,
                            'commission': (comm * (abs(matched_q) / abs(qty))) + (
                                        match_lot['comm_per_share'] * abs(matched_q)),
                            'asset_class': row['asset_class']
                        })

                        remaining_qty = 0

                # If we exhausted inventory but still have quantity left, it becomes a new open position
                # (e.g. We had +10 Long, we Sold -50. Result: -40 Short position)
                if remaining_qty != 0:
                    inventory.append({
                        'qty': remaining_qty,
                        'price': price,
                        'date': date,
                        'comm_per_share': comm / abs(qty) if qty != 0 else 0
                    })

        # Create DataFrame
        results_df = pd.DataFrame(closed_trades)
        if not results_df.empty:
            results_df['net_pnl'] = results_df['gross_pnl'] - results_df['commission']

        return results_df