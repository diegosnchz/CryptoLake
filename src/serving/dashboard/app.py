"""Interactive Streamlit dashboard for CryptoLake."""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

API_URL = os.getenv("API_URL") or os.getenv("CRYPTO_API_BASE_URL", "http://localhost:8000")
REQUEST_TIMEOUT = 10

st.set_page_config(
    page_title="CryptoLake Dashboard",
    page_icon="CL",
    layout="wide",
)


def api_get(endpoint: str):
    """Call the serving API and return parsed JSON."""
    try:
        response = requests.get(f"{API_URL}{endpoint}", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        st.error(f"Error calling API: {exc}")
        return None


def render_kpi_card(title: str, value: str, subtitle: str = "") -> str:
    """Return an HTML card used for overview KPIs."""
    subtitle_html = f'<div class="kpi-subtitle">{subtitle}</div>' if subtitle else ""
    return (
        '<div class="kpi-card">'
        f'<div class="kpi-title">{title}</div>'
        f'<div class="kpi-value">{value}</div>'
        f"{subtitle_html}"
        "</div>"
    )


st.markdown(
    """
    <style>
      .kpi-card {
        background: rgba(33, 40, 53, 0.6);
        border: 1px solid rgba(120, 129, 149, 0.35);
        border-radius: 10px;
        padding: 14px 16px;
        min-height: 86px;
      }
      .kpi-title {
        color: #aeb6c2;
        font-size: 0.88rem;
        margin-bottom: 6px;
      }
      .kpi-value {
        color: #f6f8fb;
        font-size: 1.25rem;
        font-weight: 700;
        line-height: 1.2;
      }
      .kpi-subtitle {
        color: #9ca6b5;
        font-size: 0.85rem;
        margin-top: 6px;
      }
    </style>
    """,
    unsafe_allow_html=True,
)


st.title("CryptoLake - Crypto Analytics Dashboard")
st.caption("Powered by Apache Iceberg + Spark + dbt + FastAPI")

health = api_get("/api/v1/health")
if not health or health.get("status") != "healthy":
    st.warning("API not available. Make sure the pipeline has run.")
    st.stop()

st.header("Market Overview")
overview = api_get("/api/v1/analytics/market-overview")
if overview:
    col1, col2, col3, col4 = st.columns(4)
    col1.markdown(
        render_kpi_card("Coins Tracked", str(overview.get("total_coins", 0))),
        unsafe_allow_html=True,
    )
    col2.markdown(
        render_kpi_card("Fact Rows", f"{overview.get('total_fact_rows', 0):,}"),
        unsafe_allow_html=True,
    )
    col3.markdown(
        render_kpi_card(
            "Fear & Greed",
            str(overview.get("latest_fear_greed", "-")),
            str(overview.get("latest_sentiment", "")),
        ),
        unsafe_allow_html=True,
    )
    col4.markdown(
        render_kpi_card(
            "Date Range",
            f"{overview.get('date_range_start', '?')} -> {overview.get('date_range_end', '?')}",
        ),
        unsafe_allow_html=True,
    )

st.header("Price Analysis")
coins = api_get("/api/v1/analytics/coins")
if coins:
    coin_ids = [coin["coin_id"] for coin in coins]
    selected_coin = st.selectbox("Select cryptocurrency:", coin_ids)
    prices = api_get(f"/api/v1/prices/{selected_coin}?limit=365")

    if prices:
        frame = pd.DataFrame(prices)
        frame["price_date"] = pd.to_datetime(frame["price_date"])
        frame = frame.sort_values("price_date")

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=frame["price_date"],
                y=frame["price_usd"],
                name="Price",
                line={"color": "#4A90D9", "width": 2},
            )
        )
        if "moving_avg_7d" in frame.columns:
            fig.add_trace(
                go.Scatter(
                    x=frame["price_date"],
                    y=frame["moving_avg_7d"],
                    name="MA 7d",
                    line={"color": "#F5A623", "dash": "dash"},
                )
            )
        if "moving_avg_30d" in frame.columns:
            fig.add_trace(
                go.Scatter(
                    x=frame["price_date"],
                    y=frame["moving_avg_30d"],
                    name="MA 30d",
                    line={"color": "#1E8449", "dash": "dot"},
                )
            )
        fig.update_layout(
            title=f"{selected_coin.title()} - Price & Moving Averages",
            xaxis_title="Date",
            yaxis_title="Price (USD)",
            template="plotly_dark",
            height=450,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Coin Stats")
        coins_df = pd.DataFrame(coins).sort_values("avg_price", ascending=False).head(20)
        coins_df = coins_df.fillna("")
        table = go.Figure(
            data=[
                go.Table(
                    header={
                        "values": [col.replace("_", " ").title() for col in coins_df.columns],
                        "fill_color": "#1f2a3a",
                        "font": {"color": "white", "size": 12},
                        "align": "left",
                    },
                    cells={
                        "values": [coins_df[col] for col in coins_df.columns],
                        "fill_color": "#0f1724",
                        "font": {"color": "#d8dee9", "size": 11},
                        "align": "left",
                    },
                )
            ]
        )
        table.update_layout(
            margin={"l": 0, "r": 0, "t": 8, "b": 0},
            height=430,
            template="plotly_dark",
        )
        st.plotly_chart(table, use_container_width=True)

st.header("Fear & Greed Index")
fear_greed = api_get("/api/v1/analytics/fear-greed?limit=60")
if fear_greed:
    sentiment_frame = pd.DataFrame(fear_greed)
    sentiment_frame["index_date"] = pd.to_datetime(sentiment_frame["index_date"])
    sentiment_frame = sentiment_frame.sort_values("index_date")

    color_map = {
        "Extreme Fear": "#DC3545",
        "Fear": "#FD7E14",
        "Neutral": "#FFC107",
        "Greed": "#28A745",
        "Extreme Greed": "#20C997",
    }

    sentiment_fig = px.bar(
        sentiment_frame,
        x="index_date",
        y="fear_greed_value",
        color="classification",
        color_discrete_map=color_map,
        title="Fear & Greed Index (last 60 days)",
        template="plotly_dark",
        height=350,
    )
    sentiment_fig.add_hline(y=50, line_dash="dash", line_color="gray")
    st.plotly_chart(sentiment_fig, use_container_width=True)

st.divider()
st.caption("CryptoLake - Apache Iceberg, Spark, dbt, Airflow, FastAPI, Streamlit")
