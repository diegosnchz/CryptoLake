"""Price routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from src.serving.api.database import SparkThriftClient, get_thrift_client
from src.serving.api.models.schemas import PriceResponse

router = APIRouter(prefix="/prices", tags=["prices"])


@router.get("/{coin_id}", response_model=list[PriceResponse])
def get_prices(
    coin_id: str,
    days: int = Query(default=30, ge=1, le=365),
    client: SparkThriftClient = Depends(get_thrift_client),
) -> list[PriceResponse]:
    """Return most recent price rows for one coin."""
    query = """
        SELECT
            coin_id,
            price_date,
            price_usd,
            price_change_pct_1d,
            moving_avg_7d,
            moving_avg_30d,
            volatility_7d,
            fear_greed_value,
            market_sentiment,
            ma30_signal
        FROM gold.fact_market_daily
        WHERE coin_id = %s
        ORDER BY price_date DESC
        LIMIT %s
    """
    try:
        rows = client.fetch_all(query=query, params=(coin_id, days))
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Spark Thrift query failed: {exc}",
        ) from exc
    if not rows:
        raise HTTPException(status_code=404, detail=f"No price data found for coin '{coin_id}'.")

    ordered_rows = list(reversed(rows))
    return [PriceResponse.model_validate(row) for row in ordered_rows]
