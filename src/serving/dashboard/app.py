"""Streamlit dashboard for CryptoLake."""

from __future__ import annotations

import os

import pandas as pd
import requests
import streamlit as st

API_BASE_URL = os.getenv("CRYPTO_API_BASE_URL", "http://localhost:8000")
REQUEST_TIMEOUT = 15


def api_get(path: str, params: dict | None = None) -> dict | list:
    """Execute GET request against serving API."""
    response = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


@st.cache_data(ttl=60)
def get_overview() -> dict:
    return api_get("/analytics/market-overview")


@st.cache_data(ttl=60)
def get_coins(limit: int = 100) -> list[dict]:
    return api_get("/analytics/coins", params={"limit": limit})


@st.cache_data(ttl=60)
def get_prices(coin_id: str, days: int) -> list[dict]:
    return api_get(f"/prices/{coin_id}", params={"days": days})


@st.cache_data(ttl=60)
def get_fear_greed(days: int = 30) -> list[dict]:
    return api_get("/analytics/fear-greed", params={"days": days})


def render_header() -> None:
    st.set_page_config(page_title="CryptoLake Dashboard", page_icon="CL", layout="wide")
    st.title("CryptoLake Market Dashboard")
    st.caption("Serving layer on FastAPI + Spark Thrift + Iceberg Gold")


def render_health() -> bool:
    try:
        health = api_get("/health")
    except Exception as exc:
        st.error(f"API unavailable: {exc}")
        return False

    status = health.get("status", "unknown")
    if status == "healthy":
        st.success("API and Spark Thrift are healthy.")
    else:
        st.warning(f"API status: {status}. Details: {health.get('details', 'n/a')}")
    return True


def render_overview_metrics() -> None:
    overview = get_overview()
    col_a, col_b, col_c, col_d = st.columns(4)
    col_a.metric("Tracked Coins", int(overview.get("total_coins", 0)))
    col_b.metric("Fact Rows", int(overview.get("total_fact_rows", 0)))
    col_c.metric("Latest Date", str(overview.get("latest_price_date", "-")))
    avg_fg = overview.get("avg_fear_greed")
    col_d.metric("Avg Fear&Greed", f"{avg_fg:.2f}" if isinstance(avg_fg, (int, float)) else "-")


def render_price_section() -> None:
    coins = get_coins(limit=200)
    coin_ids = [coin["coin_id"] for coin in coins]
    if not coin_ids:
        st.warning("No coins available in dim_coins yet.")
        return

    st.subheader("Price Series")
    selected_coin = st.selectbox("Coin", options=coin_ids, index=0)
    days = st.slider("Days", min_value=7, max_value=180, value=60, step=1)

    rows = get_prices(selected_coin, days=days)
    frame = pd.DataFrame(rows)
    if frame.empty:
        st.warning("No price data available for selected coin.")
        return

    frame["price_date"] = pd.to_datetime(frame["price_date"])
    frame = frame.sort_values("price_date")
    chart_frame = frame.set_index("price_date")[["price_usd", "moving_avg_7d", "moving_avg_30d"]]
    st.line_chart(chart_frame)

    with st.expander("Latest rows", expanded=False):
        st.dataframe(frame.tail(10), use_container_width=True)


def render_sentiment_section() -> None:
    st.subheader("Fear & Greed")
    rows = get_fear_greed(days=60)
    frame = pd.DataFrame(rows)
    if frame.empty:
        st.info("No sentiment data available.")
        return

    frame["index_date"] = pd.to_datetime(frame["index_date"])
    frame = frame.sort_values("index_date")
    st.line_chart(frame.set_index("index_date")[["fear_greed_value"]])
    st.dataframe(frame.tail(10), use_container_width=True)


def main() -> None:
    render_header()
    if not render_health():
        return

    render_overview_metrics()
    st.divider()
    render_price_section()
    st.divider()
    render_sentiment_section()


if __name__ == "__main__":
    main()
