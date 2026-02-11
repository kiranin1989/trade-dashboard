import pandas as pd
from core.campaign_engine import CampaignEngine
from core.data_service import DataService


def run_test():
    print("Loading data for Campaign Test...")
    ds = DataService()
    closed_df, _ = ds.get_processed_data()

    if closed_df.empty:
        print("No closed trades found.")
        return

    print(f"Loaded {len(closed_df)} closed trades.")

    # 1. Run Smart Campaign Engine (No manual tolerance needed)
    print("Running Smart Campaign Engine...")
    df_campaigns = CampaignEngine.identify_campaigns(closed_df)

    # 2. Aggregate Stats
    stats = CampaignEngine.aggregate_campaign_stats(df_campaigns)

    print(f"\n--- Campaign Analysis ---")
    print(f"Total Campaigns Identified: {len(stats)}")

    if not stats.empty:
        # Show Top 5 Profitable Campaigns
        print("\n--- Top 5 Profitable Campaigns ---")
        top_winners = stats.sort_values('total_pnl', ascending=False).head(5)
        print(
            top_winners[['root_symbol', 'start_date', 'duration_days', 'trades_count', 'total_pnl', 'roi_annualized']])

        # Show "Longest" Campaign (True Wheel)
        print("\n--- Longest Duration Campaign ---")
        longest = stats.sort_values('duration_days', ascending=False).iloc[0]
        print(longest[['root_symbol', 'start_date', 'end_date', 'duration_days', 'trades_count']])

        # Check SPOT specifically to verify the fix
        print("\n--- Checking SPOT Campaigns (Should be split) ---")
        spot_camps = stats[stats['root_symbol'] == 'SPOT'].sort_values('start_date', ascending=False).head(5)
        print(spot_camps[['start_date', 'end_date', 'trades_count', 'total_pnl']])


if __name__ == "__main__":
    run_test()