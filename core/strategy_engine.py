import pandas as pd
import logging
import uuid

logger = logging.getLogger(__name__)


class StrategyEngine:
    """
    Groups individual trade executions into "Strategies" (e.g., Spreads, Iron Condors).
    """

    @staticmethod
    def group_executions_into_strategies(trades_df: pd.DataFrame, time_threshold_seconds=10) -> pd.DataFrame:
        """
        Groups trades based on ENTRY time to identify intent.
        """
        if trades_df.empty:
            return trades_df

        df = trades_df.copy()

        # 1. Determine Grouping Column (Prioritize Entry Intent)
        # We group by when the trade was OPENED, not closed.
        # This separates spreads entered on different days that expire together.
        group_col = 'entry_date' if 'entry_date' in df.columns else 'trade_date'

        # Ensure datetime
        if not pd.api.types.is_datetime64_any_dtype(df[group_col]):
            df[group_col] = pd.to_datetime(df[group_col])

        # Sort strictly by Symbol then Entry Time
        df = df.sort_values(by=['root_symbol', group_col])

        # Initialize
        df['strategy_id'] = None
        df['strategy_type'] = 'Single'

        # Grouping Logic
        current_strategy_id = str(uuid.uuid4())
        prev_row = None
        strategy_buffer = []

        for index, row in df.iterrows():
            is_new_group = False

            if prev_row is None:
                is_new_group = True
            else:
                if row['root_symbol'] != prev_row['root_symbol']:
                    is_new_group = True
                else:
                    # Time Delta Check on ENTRY time
                    time_diff = (row[group_col] - prev_row[group_col]).total_seconds()
                    if abs(time_diff) > time_threshold_seconds:
                        is_new_group = True

            if is_new_group:
                if strategy_buffer:
                    strat_type = StrategyEngine._classify_strategy(df.loc[strategy_buffer])
                    df.loc[strategy_buffer, 'strategy_type'] = strat_type

                current_strategy_id = str(uuid.uuid4())
                strategy_buffer = [index]
            else:
                strategy_buffer.append(index)

            df.at[index, 'strategy_id'] = current_strategy_id
            prev_row = row

        if strategy_buffer:
            strat_type = StrategyEngine._classify_strategy(df.loc[strategy_buffer])
            df.loc[strategy_buffer, 'strategy_type'] = strat_type

        return df

    @staticmethod
    def _classify_strategy(cluster_df: pd.DataFrame) -> str:
        """
        Determines if a cluster of trades is a Spread, Iron Condor, etc.
        """
        count = len(cluster_df)
        if count == 1:
            return "Single"

        # Safely extract attributes
        try:
            # Handle potential missing columns if running on raw data
            asset_classes = [str(x) for x in
                             cluster_df['asset_class'].unique().tolist()] if 'asset_class' in cluster_df.columns else []

            # Helper to get unique non-null values
            def get_unique(col):
                return cluster_df[col].dropna().unique() if col in cluster_df.columns else []

            rights = get_unique('put_call')
            expiries = get_unique('expiry')
            strikes = get_unique('strike')

        except Exception:
            return "Multi-Leg (Unknown)"

        # 1. Stock Combinations
        is_stock = any('STK' in x for x in asset_classes)
        is_opt = any('OPT' in x or 'FOP' in x for x in asset_classes)

        if is_stock and is_opt:
            return "Covered/Protected Stock"

            # 2. Pure Options Strategies
        if count == 2:
            if len(expiries) == 1:
                # Same Expiry
                if len(rights) == 1:
                    return "Vertical Spread"  # Same expiry, same type (Call/Call), diff strikes
                else:
                    return "Straddle/Strangle"  # Same expiry, diff type (Call/Put)
            else:
                return "Calendar/Diagonal Spread"

        if count == 3:
            return "Butterfly/Ladder"

        if count == 4:
            # Iron Condor usually has 4 legs, same expiry
            if len(expiries) == 1:
                return "Iron Condor"

        return f"Custom {count}-Leg"

    @staticmethod
    def aggregate_strategy_pnl(df_with_strategies: pd.DataFrame) -> pd.DataFrame:
        """
        Collapses the expanded trade list into 1 row per Strategy.
        Uses the LATEST close_date to represent when the strategy finished.
        """
        # We need a date to plot this strategy on the chart.
        # Prefer the Close Date (Realized P&L date).
        date_col = 'close_date' if 'close_date' in df_with_strategies.columns else 'trade_date'

        # Aggregation Dictionary
        agg_rules = {
            date_col: 'max',  # Strategy "ends" when the last leg closes
            'net_pnl': 'sum',
            'commission': 'sum',
            'quantity': 'count',
        }

        # Optional columns if they exist
        if 'close_reason' in df_with_strategies.columns:
            agg_rules['close_reason'] = lambda x: ', '.join(sorted(set([str(i) for i in x if i])))

        metrics = df_with_strategies.groupby(['strategy_id', 'root_symbol', 'strategy_type']).agg(
            agg_rules).reset_index()

        metrics = metrics.rename(columns={date_col: 'date', 'quantity': 'leg_count'})
        metrics = metrics.sort_values(by='date', ascending=False)

        return metrics