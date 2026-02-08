import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as ui_plot
from core.data_service import DataService
from datetime import datetime, timedelta

# Page Config
st.set_page_config(page_title="IBKR Trading Dashboard", layout="wide")
st.title("📈 Trading Performance Dashboard")

# Initialize Data Service
data_service = DataService()

# 1. Sidebar Controls
st.sidebar.header("Controls")
if st.sidebar.button("🔄 Sync with IBKR"):
    with st.spinner("Fetching latest data..."):
        # We can call the main.py pipeline here in the future
        st.sidebar.success("Sync logic triggered!")

# Load Data
closed_df, open_df = data_service.get_processed_data()

if closed_df.empty:
    st.warning("No trading data found. Please sync with IBKR or check your database.")
else:
    # 2. Sidebar Filters
    st.sidebar.subheader("Filters")

    # Symbol Filter
    all_symbols = sorted(closed_df['symbol'].unique())
    selected_symbols = st.sidebar.multiselect("Filter by Symbol", all_symbols)

    # Asset Class Filter
    asset_types = st.sidebar.multiselect("Asset Type", ["Stocks", "Options"], default=["Stocks", "Options"])

    # Date Filter
    min_date = closed_df['close_date'].min().date()
    max_date = closed_df['close_date'].max().date()
    date_range = st.sidebar.date_input("Date Range", [min_date, max_date])

    # Apply Filters
    df = data_service.apply_filters(closed_df, selected_symbols, date_range, asset_types)

    # 3. Top Level Metrics
    m1, m2, m3, m4 = st.columns(4)
    total_pnl = df['net_pnl'].sum()
    win_rate = (len(df[df['net_pnl'] > 0]) / len(df) * 100) if not df.empty else 0
    total_comm = df['commission'].sum() if 'commission' in df.columns else 0

    m1.metric("Realized P&L", f"${total_pnl:,.2f}", delta=None)
    m2.metric("Win Rate", f"{win_rate:.1f}%")
    m3.metric("Closed Trades", len(df))
    m4.metric("Commissions", f"${total_comm:,.2f}")

    # 4. Main Visuals (Tabs)
    tab_equity, tab_symbols, tab_raw = st.tabs(["Equity Curve", "Symbol Breakdown", "Trade Journal"])

    with tab_equity:
        st.subheader("Cumulative P&L Over Time")
        df_sorted = df.sort_values('close_date')
        df_sorted['cum_pnl'] = df_sorted['net_pnl'].cumsum()

        fig = px.line(df_sorted, x='close_date', y='cum_pnl',
                      labels={'cum_pnl': 'Cumulative P&L ($)', 'close_date': 'Date'},
                      template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)

    with tab_symbols:
        st.subheader("Net P&L by Symbol")
        sym_pnl = df.groupby('symbol')['net_pnl'].sum().sort_values()

        fig_bar = px.bar(sym_pnl, orientation='h',
                         color=sym_pnl > 0,
                         color_discrete_map={True: 'green', False: 'red'},
                         labels={'value': 'Net P&L ($)', 'symbol': 'Ticker'})
        st.plotly_chart(fig_bar, use_container_width=True)

    with tab_raw:
        st.subheader("Trade Execution Log")
        st.dataframe(df.sort_values('close_date', ascending=False), use_container_width=True)

    # 5. Open Positions Summary
    st.divider()
    st.subheader("📋 Current Open Positions")
    if not open_df.empty:
        st.table(open_df)
    else:
        st.info("No open positions detected.")