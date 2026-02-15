from src.processing.batch.silver_quality import is_valid_trade_record


def test_is_valid_trade_record_accepts_valid_payload() -> None:
    assert is_valid_trade_record(
        {
            "symbol": "BTCUSDT",
            "event_time_ms": 1700000000000,
            "price": 42000.5,
            "quantity": 0.123,
        }
    )


def test_is_valid_trade_record_rejects_invalid_values() -> None:
    assert not is_valid_trade_record(
        {"symbol": "BTCUSDT", "event_time_ms": 1, "price": 0, "quantity": 1}
    )
    assert not is_valid_trade_record(
        {"symbol": "BTCUSDT", "event_time_ms": 1, "price": 1, "quantity": -1}
    )
    assert not is_valid_trade_record(
        {"symbol": "", "event_time_ms": 1, "price": 1, "quantity": 0}
    )
    assert not is_valid_trade_record(
        {"symbol": "BTCUSDT", "price": 1, "quantity": 0}
    )
