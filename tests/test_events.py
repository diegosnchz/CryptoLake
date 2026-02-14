import math

import pytest

from src.contracts.events import TradeEventV1, normalize_binance_payload_to_trade_event_v1


def test_normalizer_accepts_direct_payload() -> None:
    payload = {
        "s": "btcusdt",
        "E": 1700000000100,
        "T": 1700000000000,
        "p": "42000.5",
        "q": "0.123",
        "m": False,
        "a": 12345,
    }

    event = normalize_binance_payload_to_trade_event_v1(payload)

    assert event.event_version == "v1"
    assert event.source == "binance"
    assert event.symbol == "BTCUSDT"
    assert event.event_time_ms == 1700000000100
    assert event.trade_time_ms == 1700000000000
    assert event.price == 42000.5
    assert event.quantity == 0.123
    assert event.trade_id == 12345
    assert event.is_buyer_maker is False


def test_normalizer_accepts_stream_wrapped_payload() -> None:
    payload = {
        "stream": "btcusdt@aggTrade",
        "data": {
            "s": "BTCUSDT",
            "E": 1700000000100,
            "T": 1700000000000,
            "p": "42000.5",
            "q": "0.123",
            "m": True,
            "a": 12345,
        },
    }

    event = normalize_binance_payload_to_trade_event_v1(payload)

    assert event.symbol == "BTCUSDT"
    assert event.is_buyer_maker is True


@pytest.mark.parametrize(
    "payload",
    [
        {"E": 1700000000100, "p": "42000.5", "q": "0.123"},
        {"s": "BTCUSDT", "p": "42000.5", "q": "0.123"},
        {"s": "BTCUSDT", "E": 1700000000100, "q": "0.123"},
        {"s": "BTCUSDT", "E": 1700000000100, "p": "42000.5"},
    ],
)
def test_normalizer_rejects_missing_required_fields(payload: dict) -> None:
    with pytest.raises(ValueError, match="Missing required trade event fields"):
        normalize_binance_payload_to_trade_event_v1(payload)


def test_trade_event_v1_serialization_stays_on_v1_and_disallows_nan() -> None:
    event = TradeEventV1(
        symbol="BTCUSDT",
        event_time_ms=1700000000100,
        price=42000.5,
        quantity=0.123,
        trade_id=1,
    )
    encoded = event.model_dump_json(exclude_none=True)

    assert '"event_version":"v1"' in encoded
    assert "NaN" not in encoded

    with pytest.raises(ValueError):
        TradeEventV1(
            symbol="BTCUSDT",
            event_time_ms=1700000000100,
            price=math.nan,
            quantity=0.123,
        )
