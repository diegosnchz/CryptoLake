"""Schema definitions for API responses."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict


class PriceResponse(BaseModel):
    """One market record for one coin and one day."""

    model_config = ConfigDict(from_attributes=True)

    coin_id: str
    price_date: date
    price_usd: float
    price_change_pct_1d: float | None = None
    moving_avg_7d: float | None = None
    moving_avg_30d: float | None = None
    volatility_7d: float | None = None
    fear_greed_value: int | None = None
    market_sentiment: str | None = None
    ma30_signal: str | None = None


class CoinResponse(BaseModel):
    """Aggregated coin information from dim_coins."""

    model_config = ConfigDict(from_attributes=True)

    coin_id: str
    first_tracked_date: date | None = None
    last_tracked_date: date | None = None
    total_days_tracked: int | None = None
    all_time_low: float | None = None
    all_time_high: float | None = None
    avg_price: float | None = None
    avg_daily_volume: float | None = None
    price_range_pct: float | None = None


class FearGreedResponse(BaseModel):
    """Fear and Greed values by date."""

    model_config = ConfigDict(from_attributes=True)

    index_date: date
    fear_greed_value: int
    classification: str


class MarketOverview(BaseModel):
    """High-level snapshot for dashboard top metrics."""

    model_config = ConfigDict(from_attributes=True)

    total_coins: int
    total_fact_rows: int
    latest_price_date: date | None = None
    avg_fear_greed: float | None = None


class HealthResponse(BaseModel):
    """Health endpoint response."""

    model_config = ConfigDict(from_attributes=True)

    status: str
    thrift_connected: bool
    tables_available: int
    details: str | None = None
