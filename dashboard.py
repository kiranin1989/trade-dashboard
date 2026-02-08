import streamlit as st
import pandas as pd
import plotly.express as px
from core.data_service import DataService
from datetime import datetime, timedelta, date

# Page Config
st.set_page_config(page_title="IBKR Trading Dashboard", layout="wide")
st.title("📈 Trading Performance Dashboard")

# Initialize Data Service
data_service = DataService()

# --- SIDEBAR CONTROLS ---
st.sidebar.header("Controls")

# Date Presets
st.sidebar.subheader("Time Period")
time_period = st.sidebar.selectbox(
    "Select Period",
    ["YTD", "Last Year", "MTD", "WTD", "Since Inception", "Custom"]
)

# Date Calculation Logic
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

# --- LOAD DATA ---
closed_df, open_df = data_service.get_processed_data()

if closed_df.empty:
    st.warning("No trading data found. Please run main.py to fetch data.")
else:
    if start_date is None:
        start_date = closed_df['close_date'].min().date()

    mask = (closed_df['close_date'].dt.date >= start_date) & (closed_df['close_date'].dt.date <= end_date)
    filtered_df = closed_df.loc[mask].copy()

    # Symbol Filter
    all_roots = sorted(filtered_df['root_symbol'].astype(str).unique())
    selected_roots = st.sidebar.multiselect("Filter by Ticker", all_roots)

    if selected_roots:
        filtered_df = filtered_df[filtered_df['root_symbol'].isin(selected_roots)]

    # --- TOP METRICS ---
    st.markdown(f"### Performance ({time_period})")
    m1, m2, m3, m4 = st.columns(4)

    total_pnl = filtered_df['net_pnl'].sum()
    win_count = len(filtered_df[filtered_df['net_pnl'] > 0])
    total_trades = len(filtered_df)
    win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0
    total_comm = filtered_df['commission'].sum() if 'commission' in filtered_df.columns else 0

    m1.metric("Net P&L", f"${total_pnl:,.2f}", delta=None)
    m2.metric("Win Rate", f"{win_rate:.1f}%")
    m3.metric("Trades Executed", total_trades)
    m4.metric("Commissions", f"${total_comm:,.2f}")

    # --- VISUALIZATION TABS ---
    tab_equity, tab_symbols, tab_raw = st.tabs(["Equity Curve", "P&L by Ticker", "Trade Journal"])

    with tab_equity:
        if not filtered_df.empty:
            df_sorted = filtered_df.sort_values('close_date')
            df_sorted['cum_pnl'] = df_sorted['net_pnl'].cumsum()

            fig = px.line(df_sorted, x='close_date', y='cum_pnl',
                          title="Cumulative P&L",
                          labels={'cum_pnl': 'Net P&L ($)', 'close_date': 'Date'},
                          template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No trades in this period.")

    with tab_symbols:
        if not filtered_df.empty:
            sym_pnl = filtered_df.groupby('root_symbol')['net_pnl'].sum().sort_values()
            fig_bar = px.bar(sym_pnl, orientation='h',
                             title="Net P&L by Ticker (Stock + Options)",
                             color=sym_pnl.values,
                             color_continuous_scale=['red', 'green'],
                             labels={'value': 'Net P&L ($)', 'root_symbol': 'Ticker'})
            st.plotly_chart(fig_bar, use_container_width=True)

    with tab_raw:
        st.subheader("Detailed Trade Log")
        # --- NEW COLUMN ORDER ---
        display_cols = [
            'root_symbol',
            'asset_id',
            'quantity',
            'entry_date',  # Added
            'close_date',
            'commission',
            'net_pnl',
            'close_reason'  # Added
        ]
        # Filter existing columns to prevent errors if data is empty or missing cols
        valid_cols = [c for c in display_cols if c in filtered_df.columns]

        st.dataframe(
            filtered_df[valid_cols].sort_values('close_date', ascending=False),
            use_container_width=True
        )

    # --- OPEN POSITIONS SECTION ---
    st.divider()
    st.subheader("📋 Current Open Positions")

    if not open_df.empty:
        open_view = open_df.copy()
        if selected_roots:
            open_view = open_view[open_view['root_symbol'].isin(selected_roots)]
        st.dataframe(open_view.sort_values('root_symbol'), use_container_width=True)
    else:
        st.info("No open positions.")