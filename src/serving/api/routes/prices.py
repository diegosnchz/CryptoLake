"""Price routes."""

from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query

from src.serving.api.database import SparkThriftClient, get_thrift_client
from src.serving.api.models.schemas import PriceResponse

router = APIRouter(prefix="/prices", tags=["prices"])


@router.get("/{coin_id}", response_model=list[PriceResponse])
def get_prices(
    coin_id: str,
    start_date: date | None = Query(
        default=None,
        description="Start date (default: 30 days ago).",
    ),
    end_date: date | None = Query(
        default=None,
        description="End date (default: today).",
    ),
    limit: int = Query(default=100, ge=1, le=1000),
    client: SparkThriftClient = Depends(get_thrift_client),
) -> list[PriceResponse]:
    """Return most recent price rows for one coin."""
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=30)
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date.")

    query = """
        SELECT
            coin_id,
            price_date,
            price_usd,
            market_cap_usd,
            volume_24h_usd,
            price_change_pct_1d,
            moving_avg_7d,
            moving_avg_30d,
            volatility_7d,
            fear_greed_value,
            market_sentiment,
            ma30_signal
        FROM cryptolake.gold.fact_market_daily
        WHERE coin_id = %s
          AND price_date BETWEEN %s AND %s
        ORDER BY price_date DESC
        LIMIT %s
    """
    try:
        rows = client.fetch_all(
            query=query,
            params=(coin_id, start_date.isoformat(), end_date.isoformat(), limit),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Spark Thrift query failed: {exc}",
        ) from exc
    if not rows:
        raise HTTPException(status_code=404, detail=f"No price data found for coin '{coin_id}'.")

    ordered_rows = list(reversed(rows))
    return [PriceResponse.model_validate(row) for row in ordered_rows]
