import altair as alt
import pandas as pd
import streamlit as st
from pyiceberg.catalog import load_catalog

from src.config.settings import settings

st.set_page_config(page_title="CryptoLake Silver Inspector", layout="wide")
st.title("CryptoLake Silver 1m Inspector")


@st.cache_resource(show_spinner=False)
def get_catalog():
    return load_catalog(
        settings.iceberg_catalog_name,
        **{
            "type": "rest",
            "uri": settings.iceberg_rest_uri,
            "s3.endpoint": settings.minio_endpoint,
            "s3.access-key-id": settings.minio_access_key,
            "s3.secret-access-key": settings.minio_secret_key,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


@st.cache_data(ttl=20, show_spinner=False)
def load_latest_rows(limit: int, symbol: str) -> pd.DataFrame:
    catalog = get_catalog()
    table = catalog.load_table("silver.ohlcv_1m")
    frame = table.scan().to_arrow().to_pandas()
    if frame.empty:
        return frame

    selected = frame[
        ["symbol", "window_start", "window_end", "open", "high", "low", "close", "volume", "trades"]
    ].copy()
    # Keep chart inputs stable even if Arrow/Pandas infer object/decimal dtypes.
    selected["window_start"] = pd.to_datetime(selected["window_start"], errors="coerce", utc=True)
    selected["window_end"] = pd.to_datetime(selected["window_end"], errors="coerce", utc=True)
    for numeric_col in ("open", "high", "low", "close", "volume", "trades"):
        selected[numeric_col] = pd.to_numeric(selected[numeric_col], errors="coerce")
    selected = selected.dropna(subset=["window_start", "close"])

    if symbol:
        selected = selected[selected["symbol"] == symbol.upper()]
    selected = selected.sort_values("window_start", ascending=False).head(int(limit))
    return selected


with st.sidebar:
    st.header("Filters")
    symbol = st.text_input("Symbol (optional)", "")
    limit = st.slider("Rows", min_value=10, max_value=500, value=100, step=10)
    refresh = st.button("Refresh")

if refresh:
    load_latest_rows.clear()

df = load_latest_rows(limit=limit, symbol=symbol)

c1, c2, c3 = st.columns(3)
c1.metric("Rows loaded", f"{len(df)}")
c2.metric("Selected symbol", symbol.upper() if symbol else "ALL")
c3.metric("Most recent candle", str(df["window_start"].max()) if not df.empty else "n/a")

st.subheader("Latest silver rows")
st.dataframe(df, use_container_width=True, hide_index=True)

if not df.empty:
    chart_df = df.sort_values("window_start")
    line = (
        alt.Chart(chart_df)
        .mark_line()
        .encode(x="window_start:T", y="close:Q", color="symbol:N")
        .properties(height=320)
    )
    st.subheader("Close price trend")
    st.altair_chart(line, use_container_width=True)
