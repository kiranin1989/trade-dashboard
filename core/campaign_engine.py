import pandas as pd
import logging
import uuid

logger = logging.getLogger(__name__)


class CampaignEngine:
    """
    Groups trades into 'Campaigns' based on time continuity.
    Uses Dynamic Tolerance based on close reasons (Trade vs Assignment).
    """

    @staticmethod
    def identify_campaigns(closed_trades_df: pd.DataFrame) -> pd.DataFrame:
        """
        Groups trades into campaigns.

        Dynamic Tolerance Logic:
        - If previous trade ended via 'Trade' (Manual): Short tolerance (2 hours).
        - If previous trade ended via 'Assigned/Expired': Long tolerance (4 days) to bridge weekends.
        """
        if closed_trades_df.empty:
            return pd.DataFrame()

        df = closed_trades_df.copy()

        # Filter SPX (Cash settled, no Wheel)
        df = df[~df['root_symbol'].isin(['SPX', 'SPXW'])]
        if df.empty: return pd.DataFrame()

        # Ensure datetimes
        for col in ['entry_date', 'close_date']:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col])

        # Sort
        df = df.sort_values(by=['root_symbol', 'entry_date'])

        df['campaign_id'] = None

        # Tolerances
        TOLERANCE_STRICT = pd.Timedelta(hours=2)  # For manual closes (Trade)
        TOLERANCE_LOOSE = pd.Timedelta(days=4)  # For passive closes (Assign/Expire)

        for symbol, group in df.groupby('root_symbol'):
            group = group.sort_values('entry_date')

            current_camp_id = str(uuid.uuid4())

            # State Variables for the current campaign window
            camp_end = group.iloc[0]['close_date']
            camp_end_reason = group.iloc[0]['close_reason']

            for idx, row in group.iterrows():
                # Determine allowed gap based on how the LAST trade ended
                if camp_end_reason in ['Assigned', 'Exercised', 'Expired']:
                    tolerance = TOLERANCE_LOOSE
                else:
                    tolerance = TOLERANCE_STRICT

                # Check Overlap
                if row['entry_date'] <= (camp_end + tolerance):
                    # --- LINKED ---
                    df.at[idx, 'campaign_id'] = current_camp_id

                    # Extend window?
                    if row['close_date'] > camp_end:
                        camp_end = row['close_date']
                        camp_end_reason = row['close_reason']
                else:
                    # --- NEW CAMPAIGN ---
                    current_camp_id = str(uuid.uuid4())
                    df.at[idx, 'campaign_id'] = current_camp_id

                    # Reset Window
                    camp_end = row['close_date']
                    camp_end_reason = row['close_reason']

        return df

    @staticmethod
    def aggregate_campaign_stats(df_with_campaigns: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates high-level metrics for each campaign.
        """
        if 'campaign_id' not in df_with_campaigns.columns or df_with_campaigns.empty:
            return pd.DataFrame()

        def estimate_capital(rows):
            """
            Conservative Capital Estimate: Max Notional Exposure.
            """
            max_cap = 0.0
            for _, r in rows.iterrows():
                val = 0.0
                qty = abs(r['quantity'])
                if 'strike' in r and pd.notna(r['strike']) and r['strike'] > 0:
                    val = r['strike'] * 100 * qty
                elif 'STK' in str(r.get('asset_class', '')):
                    price = r.get('close_price', 0)
                    if price == 0 and r.get('net_pnl') != 0: price = 100
                    val = price * qty
                if val > max_cap: max_cap = val
            return max_cap if max_cap > 0 else 1.0

        stats = []
        for camp_id, group in df_with_campaigns.groupby('campaign_id'):
            start_date = group['entry_date'].min()
            end_date = group['close_date'].max()
            duration_days = (end_date - start_date).total_seconds() / (24 * 3600)
            if duration_days < 1: duration_days = 1

            total_pnl = group['net_pnl'].sum()
            capital = estimate_capital(group)

            roi_abs = total_pnl / capital
            roi_annualized = roi_abs * (365 / duration_days)

            stats.append({
                'campaign_id': camp_id,
                'root_symbol': group['root_symbol'].iloc[0],
                'start_date': start_date,
                'end_date': end_date,
                'duration_days': round(duration_days, 1),
                'trades_count': len(group),
                'total_pnl': total_pnl,
                'capital_est': capital,
                'roi_abs': roi_abs * 100,
                'roi_annualized': roi_annualized * 100
            })

        stats_df = pd.DataFrame(stats)
        if not stats_df.empty:
            stats_df = stats_df.sort_values(by='end_date', ascending=False)

        return stats_df