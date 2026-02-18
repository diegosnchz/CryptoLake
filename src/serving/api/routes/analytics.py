"""Analytics routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from src.serving.api.database import SparkThriftClient, get_thrift_client
from src.serving.api.models.schemas import CoinResponse, FearGreedResponse, MarketOverview

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/market-overview", response_model=MarketOverview)
def market_overview(client: SparkThriftClient = Depends(get_thrift_client)) -> MarketOverview:
    """Return top-level overview metrics."""
    stats_query = """
        SELECT
            (SELECT COUNT(*) FROM cryptolake.gold.dim_coins) AS total_coins,
            (SELECT COUNT(*) FROM cryptolake.gold.fact_market_daily) AS total_fact_rows,
            (SELECT MIN(price_date) FROM cryptolake.gold.fact_market_daily) AS date_range_start,
            (SELECT MAX(price_date) FROM cryptolake.gold.fact_market_daily) AS date_range_end
    """
    sentiment_query = """
        SELECT
            fear_greed_value AS latest_fear_greed,
            classification AS latest_sentiment
        FROM cryptolake.silver.fear_greed
        ORDER BY index_date DESC
        LIMIT 1
    """
    try:
        stats_row = client.fetch_one(query=stats_query) or {
            "total_coins": 0,
            "total_fact_rows": 0,
            "date_range_start": None,
            "date_range_end": None,
        }
        sentiment_row = client.fetch_one(query=sentiment_query) or {
            "latest_fear_greed": None,
            "latest_sentiment": None,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Spark Thrift query failed: {exc}",
        ) from exc
    payload = {**stats_row, **sentiment_row}
    return MarketOverview.model_validate(payload)


@router.get("/coins", response_model=list[CoinResponse])
def list_coins(
    limit: int = Query(default=200, ge=1, le=1000),
    client: SparkThriftClient = Depends(get_thrift_client),
) -> list[CoinResponse]:
    """Return list of coins from dim_coins."""
    try:
        rows = client.fetch_all(
            query="""
                SELECT
                    coin_id,
                    first_tracked_date,
                    last_tracked_date,
                    total_days_tracked,
                    all_time_low,
                    all_time_high,
                    avg_price,
                    avg_daily_volume,
                    price_range_pct
                FROM cryptolake.gold.dim_coins
                ORDER BY avg_price DESC
                LIMIT %s
            """,
            params=(limit,),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Spark Thrift query failed: {exc}",
        ) from exc
    return [CoinResponse.model_validate(row) for row in rows]


@router.get("/fear-greed", response_model=list[FearGreedResponse])
def fear_greed_history(
    limit: int = Query(default=30, ge=1, le=365),
    client: SparkThriftClient = Depends(get_thrift_client),
) -> list[FearGreedResponse]:
    """Return recent Fear and Greed values."""
    try:
        rows = client.fetch_all(
            query="""
                SELECT
                    index_date,
                    CAST(fear_greed_value AS INT) AS fear_greed_value,
                    classification
                FROM cryptolake.silver.fear_greed
                ORDER BY index_date DESC
                LIMIT %s
            """,
            params=(limit,),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Spark Thrift query failed: {exc}",
        ) from exc
    ordered_rows = list(reversed(rows))
    return [FearGreedResponse.model_validate(row) for row in ordered_rows]
