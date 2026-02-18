"""Unit tests for serving API response schemas."""

from __future__ import annotations

from datetime import date

from src.serving.api.models.schemas import (
    CoinResponse,
    FearGreedResponse,
    HealthResponse,
    MarketOverview,
    PriceResponse,
)


def test_price_response() -> None:
    response = PriceResponse(
        coin_id="bitcoin",
        price_date=date(2025, 1, 15),
        price_usd=95000.0,
    )
    assert response.coin_id == "bitcoin"
    assert response.price_usd == 95000.0
    assert response.moving_avg_7d is None


def test_coin_response() -> None:
    response = CoinResponse(
        coin_id="ethereum",
        all_time_high=4000.0,
        avg_price=3200.0,
    )
    assert response.coin_id == "ethereum"
    assert response.first_tracked_date is None


def test_market_overview() -> None:
    overview = MarketOverview(total_coins=8, total_fact_rows=5000)
    assert overview.total_coins == 8


def test_fear_greed_response() -> None:
    response = FearGreedResponse(
        index_date=date(2025, 2, 1),
        fear_greed_value=25,
        classification="Extreme Fear",
    )
    assert response.fear_greed_value == 25
    assert response.classification == "Extreme Fear"


def test_health_response() -> None:
    health = HealthResponse(
        status="healthy",
        thrift_connected=True,
        tables_available=3,
    )
    assert health.status == "healthy"
