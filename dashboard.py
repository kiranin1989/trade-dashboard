import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from core.data_service import DataService
from datetime import datetime, timedelta, date
from config import settings

# Page Config
st.set_page_config(page_title="IBKR Trading Dashboard", layout="wide")
st.title("📈 Trading Performance Dashboard")

data_service = DataService()

# --- SIDEBAR CONTROLS ---
st.sidebar.header("Controls")

# 0. Sync Status
last_sync = data_service.get_last_sync()
st.sidebar.caption(f"Last Updated: {last_sync}")

# --- DEBUG INFO ---
mode = "CLOUD (MotherDuck)" if settings.MOTHERDUCK_TOKEN else "LOCAL (File)"
st.sidebar.caption(f"Mode: {mode}")
# ------------------

if st.sidebar.button("🔄 Sync with IBKR"):
    with st.spinner("Connecting to IBKR... This may take up to 30s."):
        success, msg = data_service.sync_ibkr_data()
        if success:
            st.sidebar.success(msg)
            st.rerun()  # Refresh the page to show new data
        else:
            st.sidebar.error(msg)

st.sidebar.divider()

# 1. VIEW MODE
view_mode = st.sidebar.radio("View Mode", ["Standard Dashboard", "Strategy Lab"], index=0)
st.sidebar.divider()

# 2. GLOBAL FILTERS
st.sidebar.subheader("Global Filters")
time_period = st.sidebar.selectbox("Time Period", ["YTD", "Last Year", "MTD", "WTD", "Since Inception", "Custom"])

today = datetime.now().date()
start_date = None
end_date = today

if time_period == "YTD":
    start_date = date(today.year, 1, 1)
elif time_period == "Last Year":
    start_date = date(today.year - 1, 1, 1)
    end_date = date(today.year - 1, 12, 31)
elif time_period == "MTD":
    start_date = date(today.year, today.month, 1)
elif time_period == "WTD":
    start_date = today - timedelta(days=today.weekday())
elif time_period == "Since Inception":
    start_date = None

if time_period == "Custom":
    date_range = st.sidebar.date_input("Custom Range", [today - timedelta(days=30), today])
    if len(date_range) == 2:
        start_date, end_date = date_range

# --- LOAD RAW DATA ---
closed_df, open_df = data_service.get_processed_data()

if closed_df.empty:
    st.warning("No trading data found. Click 'Sync with IBKR' in the sidebar.")
    st.stop()

# Helper for Global Filtering
if start_date is None:
    start_date = closed_df['close_date'].min().date()

# Apply Filters to RAW data for Standard View
# For Strategy view, we apply filters AFTER grouping to ensure full campaigns are captured
mask = (closed_df['close_date'].dt.date >= start_date) & (closed_df['close_date'].dt.date <= end_date)
filtered_df = closed_df.loc[mask].copy()

all_roots = sorted(closed_df['root_symbol'].astype(str).unique())
selected_roots = st.sidebar.multiselect("Filter by Ticker", all_roots)

if selected_roots:
    filtered_df = filtered_df[filtered_df['root_symbol'].isin(selected_roots)]

# ==============================================================================
# VIEW 1: STANDARD DASHBOARD
# ==============================================================================
if view_mode == "Standard Dashboard":
    st.sidebar.divider()
    st.sidebar.subheader("Chart Settings")
    chart_resolution = st.sidebar.selectbox("Resolution", ["Daily", "Weekly", "Monthly"], index=0)
    chart_view = st.sidebar.radio("View Type", ["Cumulative P&L", "Period P&L"])

    show_benchmark = st.sidebar.checkbox("Show S&P 500 Benchmark", value=False)
    benchmark_capital = 50000
    if show_benchmark:
        benchmark_capital = st.sidebar.number_input("Benchmark Principal ($)", value=50000, step=1000)

    # --- BENCHMARK DATA ---
    sp500_df = pd.DataFrame()
    if show_benchmark:
        try:
            fetch_start = start_date - timedelta(days=5) if start_date else datetime(2020, 1, 1).date()
            sp500_data = data_service.get_benchmark_data("^GSPC", start_date=fetch_start)
            if not sp500_data.empty:
                sp500_df = sp500_data.rename(columns={'close': 'Close'})
                sp500_df = sp500_df[sp500_df.index.date <= end_date]
        except Exception:
            pass

    # --- METRICS ---
    total_pnl = filtered_df['net_pnl'].sum()
    div_types = ['Dividends', 'PaymentInLieuOfDividends', 'WithholdingTax']
    div_df = filtered_df[filtered_df['close_reason'].isin(div_types)]
    total_divs = div_df['net_pnl'].sum()
    trade_only_df = filtered_df[~filtered_df.index.isin(div_df.index)]
    win_count = len(trade_only_df[trade_only_df['net_pnl'] > 0])
    total_trades = len(trade_only_df)
    win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Net P&L", f"${total_pnl:,.2f}")
    m2.metric("Dividend Income", f"${total_divs:,.2f}")
    m3.metric("Trade Win Rate", f"{win_rate:.1f}%")
    m4.metric("Trades Executed", total_trades)

    # --- TABS ---
    tab_equity, tab_symbols, tab_raw = st.tabs(["Equity Curve", "P&L by Ticker", "Trade Journal"])

    with tab_equity:
        if not filtered_df.empty:
            df_chart = filtered_df.copy()
            df_chart = df_chart.set_index('close_date')
            rule_map = {"Daily": "D", "Weekly": "W-FRI", "Monthly": "MS"}
            rule = rule_map.get(chart_resolution, "D")

            df_resampled = df_chart['net_pnl'].resample(rule).sum().fillna(0)

            # SP500
            sp_resampled_price = pd.Series(dtype=float)
            if not sp500_df.empty:
                sp_resampled_price = sp500_df['Close'].resample(rule).last().dropna()
                common_start = max(df_resampled.index.min(), sp_resampled_price.index.min())
                common_end = min(df_resampled.index.max(), sp_resampled_price.index.max())
                sp_resampled_price = sp_resampled_price.loc[common_start:common_end]

            fig = go.Figure()

            if chart_view == "Cumulative P&L":
                user_series = df_resampled.cumsum()
                fig.add_trace(go.Scatter(x=user_series.index, y=user_series.values, name="My P&L ($)", fill='tozeroy',
                                         line=dict(color='#00CC96')))

                if not sp_resampled_price.empty:
                    start_price = sp_resampled_price.iloc[0]
                    sp_dollar_growth = ((sp_resampled_price / start_price) - 1) * benchmark_capital
                    fig.add_trace(
                        go.Scatter(x=sp_dollar_growth.index, y=sp_dollar_growth.values, name=f"S&P 500 (Simulated)",
                                   line=dict(color='gray', dash='dot')))
            else:
                user_series = df_resampled
                colors = ['green' if v >= 0 else 'red' for v in user_series.values]
                fig.add_trace(go.Bar(x=user_series.index, y=user_series.values, name="Period P&L", marker_color=colors))

                if not sp_resampled_price.empty:
                    sp_dollar_change = sp_resampled_price.pct_change().fillna(0) * benchmark_capital
                    fig.add_trace(go.Scatter(x=sp_dollar_change.index, y=sp_dollar_change.values, name=f"S&P 500 P&L",
                                             line=dict(color='yellow', width=2), mode='lines+markers'))

            fig.update_layout(template="plotly_dark", hovermode="x unified", legend=dict(orientation="h", y=1.1),
                              yaxis_title="Profit / Loss ($)")
            st.plotly_chart(fig, width="stretch")
        else:
            st.info("No trades in this period.")

    with tab_symbols:
        if not filtered_df.empty:
            sym_pnl = filtered_df.groupby('root_symbol')['net_pnl'].sum().sort_values()
            fig_bar = px.bar(sym_pnl, orientation='h', title="Net P&L by Ticker", color=sym_pnl.values,
                             color_continuous_scale=['red', 'green'])
            st.plotly_chart(fig_bar, width="stretch")

    with tab_raw:
        st.subheader("Detailed Trade Log")
        display_cols = ['root_symbol', 'asset_id', 'quantity', 'entry_date', 'close_date', 'commission', 'net_pnl',
                        'close_reason']
        valid_cols = [c for c in display_cols if c in filtered_df.columns]
        st.dataframe(filtered_df[valid_cols].sort_values('close_date', ascending=False), width="stretch")

    st.divider()
    st.subheader("📋 Current Open Positions")
    if not open_df.empty:
        open_view = open_df.copy()
        if selected_roots: open_view = open_view[open_view['root_symbol'].isin(selected_roots)]
        st.dataframe(open_view.sort_values('root_symbol'), width="stretch")
    else:
        st.info("No open positions.")


# ==============================================================================
# VIEW 2: STRATEGY LAB
# ==============================================================================
elif view_mode == "Strategy Lab":
    st.title("🔬 Strategy & Campaign Analytics")

    # 1. Process Data (Using FULL history to group correctly)
    strat_df = data_service.get_strategy_data(closed_df)
    camp_df = data_service.get_campaign_data(closed_df)

    # 2. Apply Filters to the RESULTS
    if not strat_df.empty:
        s_mask = (strat_df['date'].dt.date >= start_date) & (strat_df['date'].dt.date <= end_date)
        if selected_roots:
            s_mask = s_mask & strat_df['root_symbol'].isin(selected_roots)
        strat_view = strat_df.loc[s_mask].copy()
    else:
        strat_view = pd.DataFrame()

    if not camp_df.empty:
        c_mask = (camp_df['end_date'].dt.date >= start_date) & (camp_df['end_date'].dt.date <= end_date)
        if selected_roots:
            c_mask = c_mask & camp_df['root_symbol'].isin(selected_roots)
        camp_view = camp_df.loc[c_mask].copy()
    else:
        camp_view = pd.DataFrame()

    # --- TOP METRICS ---
    c1, c2, c3 = st.columns(3)

    if not strat_view.empty:
        strat_wins = len(strat_view[strat_view['net_pnl'] > 0])
        strat_total = len(strat_view)
        real_win_rate = (strat_wins / strat_total * 100) if strat_total > 0 else 0
        c1.metric("Strategy Win Rate", f"{real_win_rate:.1f}%", help="Based on grouped Spreads/Condors")

    if not camp_view.empty:
        avg_roi = camp_view['roi_annualized'].mean()
        c2.metric("Avg Annualized ROI", f"{avg_roi:.1f}%", help="Average of all closed campaigns")
        c3.metric("Campaigns Completed", len(camp_view))

    # --- TABS ---
    tab_campaigns, tab_spreads = st.tabs(["🎡 Wheel Campaigns", "🦋 Strategy Log"])

    with tab_campaigns:
        st.subheader("The Wheel: Campaign Performance")
        st.markdown("*Campaigns are linked chains of Puts -> Assignments -> Covered Calls.*")

        if not camp_view.empty:
            camp_cols = ['root_symbol', 'start_date', 'end_date', 'duration_days', 'trades_count', 'total_pnl',
                         'roi_annualized']
            camp_display = camp_view[camp_cols].sort_values('end_date', ascending=False)

            # --- FIX: ABSOLUTE SIZE FOR BUBBLES ---
            camp_display['pnl_size'] = camp_display['total_pnl'].abs()

            # --- FIX: COLOR SCALING ---
            # Center the color scale at 0 (White/Yellow) to handle negative ROI correctly
            max_roi = max(abs(camp_display['roi_annualized'].min()), abs(camp_display['roi_annualized'].max()))

            fig_camp = px.scatter(
                camp_display,
                x='duration_days',
                y='total_pnl',  # Y-Axis = Profit ($)
                size='pnl_size',
                color='roi_annualized',  # Color = Efficiency (%)
                color_continuous_scale='RdYlGn',
                range_color=[-max_roi, max_roi],  # Symmetrical scale centers 0
                hover_data=['root_symbol', 'roi_annualized'],
                title="Campaign P&L ($) vs Duration (Color = Annualized ROI)"
            )
            st.plotly_chart(fig_camp, width="stretch")

            st.dataframe(camp_display.drop(columns=['pnl_size']), width="stretch")
        else:
            st.info("No closed campaigns found.")

    with tab_spreads:
        st.subheader("Strategy Execution Log")

        if not strat_view.empty:
            win_rates = strat_view.groupby('strategy_type').apply(
                lambda x: len(x[x['net_pnl'] > 0]) / len(x) * 100).sort_values()

            fig_type = px.bar(win_rates, orientation='h', title="Win Rate by Strategy Type",
                              labels={'value': 'Win Rate (%)', 'strategy_type': 'Type'})
            st.plotly_chart(fig_type, width="stretch")

            st.dataframe(strat_view[['date', 'root_symbol', 'strategy_type', 'net_pnl', 'leg_count', 'close_reason']],
                         width="stretch")
        else:
            st.info("No strategies found.")