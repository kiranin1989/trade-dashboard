import pandas as pd
import duckdb
from core.strategy_engine import StrategyEngine
import os

# Configuration
DB_PATH = "data/trading_data.duckdb"
CSV_PATH = "export.csv"  # If you prefer to test with the CSV you uploaded


def load_data():
    """Loads data either from DB (preferred) or CSV."""
    if os.path.exists(DB_PATH):
        print(f"Loading from Database: {DB_PATH}")
        conn = duckdb.connect(DB_PATH)
        # We need the processed P&L data, but the DB stores raw trades.
        # Ideally, we'd run the Logic Engine first.
        # For this test, let's assume we want to test grouping on RAW executions first
        # (grouping works on raw execution times).
        # However, to see 'net_pnl', we need the output of PnLEngine.

        # Let's import the full pipeline for accurate testing
        from core.data_service import DataService
        ds = DataService()
        closed, _ = ds.get_processed_data()
        return closed

    elif os.path.exists(CSV_PATH):
        print(f"Loading from CSV: {CSV_PATH}")
        df = pd.read_csv(CSV_PATH)
        # Basic cleanup for CSV to match Logic Engine output format
        if 'net_pnl' not in df.columns:
            print("Warning: CSV lacks 'net_pnl'. Grouping will work, but P&L stats won't.")
            df['net_pnl'] = 0.0

            # Ensure root_symbol exists (Logic Engine creates this)
        if 'root_symbol' not in df.columns and 'underlying' in df.columns:
            df['root_symbol'] = df['underlying']

        return df
    else:
        print("No data source found.")
        return pd.DataFrame()


def run_test():
    df = load_data()
    if df.empty:
        return

    print(f"Loaded {len(df)} trade legs.")

    # 1. Run Grouping
    print("Running Strategy Engine...")
    df_grouped = StrategyEngine.group_executions_into_strategies(df)

    # 2. Aggregate
    strategies = StrategyEngine.aggregate_strategy_pnl(df_grouped)

    print(f" identified {len(strategies)} unique strategies/executions.")

    # 3. Analysis: Multi-Leg Trades
    multi_leg = strategies[strategies['strategy_type'] != 'Single']

    print(f"\n--- Multi-Leg Strategies Found: {len(multi_leg)} ---")
    if not multi_leg.empty:
        print(multi_leg[['date', 'root_symbol', 'strategy_type', 'net_pnl', 'leg_count']].head(10))

        # Check Win Rate Impact
        win_count = len(multi_leg[multi_leg['net_pnl'] > 0])
        print(f"\nMulti-Leg Win Rate: {win_count}/{len(multi_leg)} ({win_count / len(multi_leg) * 100:.1f}%)")

    else:
        print("No multi-leg strategies found. Check time thresholds?")


if __name__ == "__main__":
    run_test()