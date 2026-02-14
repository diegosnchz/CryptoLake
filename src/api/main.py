from fastapi import FastAPI, Query

from src.config.settings import settings
from src.processing.spark_session import build_spark_session

app = FastAPI(title="CryptoLake API", version="0.1.0")
spark = build_spark_session("CryptoLake-API")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> dict:
    bronze = spark.sql(
        f"SELECT count(*) AS c FROM {settings.iceberg_catalog_name}.bronze.futures_trades"
    ).collect()[0]["c"]
    silver = spark.sql(
        f"SELECT count(*) AS c FROM {settings.iceberg_catalog_name}.silver.futures_ohlcv_1m"
    ).collect()[0]["c"]
    gold = spark.sql(
        f"SELECT count(*) AS c FROM {settings.iceberg_catalog_name}.gold.futures_daily_stats"
    ).collect()[0]["c"]
    return {"bronze_rows": bronze, "silver_rows": silver, "gold_rows": gold}


@app.get("/ohlcv/1m")
def ohlcv_1m(
    symbol: str = Query("BTCUSDT"), start: str = Query(...), end: str = Query(...)
) -> list[dict]:
    query = f"""
    SELECT symbol, window_start, window_end, open, high, low, close, volume, trades
    FROM {settings.iceberg_catalog_name}.silver.futures_ohlcv_1m
    WHERE symbol = '{symbol.upper()}'
      AND window_start >= TIMESTAMP('{start}')
      AND window_end <= TIMESTAMP('{end}')
    ORDER BY window_start
    """
    return [r.asDict() for r in spark.sql(query).collect()]


@app.get("/stats/daily")
def stats_daily(symbol: str = Query("BTCUSDT"), date: str = Query(...)) -> list[dict]:
    query = f"""
    SELECT symbol, trade_date, total_volume, day_high, day_low, price_range, trades
    FROM {settings.iceberg_catalog_name}.gold.futures_daily_stats
    WHERE symbol = '{symbol.upper()}'
      AND trade_date = DATE('{date}')
    """
    return [r.asDict() for r in spark.sql(query).collect()]
