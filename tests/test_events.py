from src.contracts.events import parse_binance_aggtrade


def test_parse_binance_aggtrade_maps_fields() -> None:
    payload = {
        "s": "btcusdt",
        "T": 1700000000000,
        "p": "42000.5",
        "q": "0.123",
        "m": False,
        "a": 12345,
    }
    event = parse_binance_aggtrade(payload)

    assert event.symbol == "BTCUSDT"
    assert float(event.price) == 42000.5
    assert float(event.qty) == 0.123
    assert event.side == "buy"
    assert event.trade_id == 12345
    assert event.exchange == "binance_futures"


def test_parse_binance_aggtrade_raises_on_invalid_payload() -> None:
    payload = {"s": "btcusdt", "p": "42000.5"}

    try:
        parse_binance_aggtrade(payload)
        raise AssertionError("Expected ValueError for malformed payload")
    except ValueError as exc:
        assert "Invalid Binance aggTrade payload" in str(exc)
