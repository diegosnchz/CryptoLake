"""Analytics routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from src.serving.api.database import SparkThriftClient, get_thrift_client
from src.serving.api.models.schemas import CoinResponse, FearGreedResponse, MarketOverview

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/market-overview", response_model=MarketOverview)
def market_overview(client: SparkThriftClient = Depends(get_thrift_client)) -> MarketOverview:
    """Return top-level overview metrics."""
    query = """
        SELECT
            (SELECT COUNT(*) FROM gold.dim_coins) AS total_coins,
            (SELECT COUNT(*) FROM gold.fact_market_daily) AS total_fact_rows,
            (SELECT MAX(price_date) FROM gold.fact_market_daily) AS latest_price_date,
            (
                SELECT ROUND(AVG(CAST(fear_greed_value AS DOUBLE)), 2)
                FROM (
                    SELECT
                        price_date,
                        MAX(fear_greed_value) AS fear_greed_value
                    FROM gold.fact_market_daily
                    WHERE fear_greed_value IS NOT NULL
                    GROUP BY price_date
                ) x
            ) AS avg_fear_greed
    """
    try:
        row = client.fetch_one(query=query) or {
            "total_coins": 0,
            "total_fact_rows": 0,
            "latest_price_date": None,
            "avg_fear_greed": None,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Spark Thrift query failed: {exc}",
        ) from exc
    return MarketOverview.model_validate(row)


@router.get("/coins", response_model=list[CoinResponse])
def list_coins(
    limit: int = Query(default=20, ge=1, le=200),
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
                FROM gold.dim_coins
                ORDER BY coin_id
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
    days: int = Query(default=30, ge=1, le=365),
    client: SparkThriftClient = Depends(get_thrift_client),
) -> list[FearGreedResponse]:
    """Return recent Fear and Greed values."""
    try:
        rows = client.fetch_all(
            query="""
                SELECT
                    price_date AS index_date,
                    CAST(MAX(fear_greed_value) AS INT) AS fear_greed_value,
                    MAX(market_sentiment) AS classification
                FROM gold.fact_market_daily
                WHERE fear_greed_value IS NOT NULL
                GROUP BY price_date
                ORDER BY index_date DESC
                LIMIT %s
            """,
            params=(days,),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Spark Thrift query failed: {exc}",
        ) from exc
    ordered_rows = list(reversed(rows))
    return [FearGreedResponse.model_validate(row) for row in ordered_rows]
