import altair as alt
import pandas as pd
import requests
import streamlit as st

API_URL = st.sidebar.text_input("API URL", "http://api:8000")
symbol = st.sidebar.text_input("Symbol", "BTCUSDT")
start = st.sidebar.text_input("Start", "2024-01-01 00:00:00")
end = st.sidebar.text_input("End", "2025-12-31 23:59:59")
date = st.sidebar.text_input("Date", "2025-01-01")

st.title("CryptoLake Dashboard")

if st.button("Load OHLCV"):
    resp = requests.get(
        f"{API_URL}/ohlcv/1m", params={"symbol": symbol, "start": start, "end": end}, timeout=10
    )
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    st.subheader("OHLCV 1m")
    st.dataframe(df)
    if not df.empty:
        chart = alt.Chart(df).mark_line().encode(x="window_start:T", y="close:Q")
        st.altair_chart(chart, use_container_width=True)

if st.button("Load Daily Stats"):
    resp = requests.get(
        f"{API_URL}/stats/daily", params={"symbol": symbol, "date": date}, timeout=10
    )
    resp.raise_for_status()
    st.subheader("Daily Stats")
    st.dataframe(pd.DataFrame(resp.json()))
