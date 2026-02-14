import json

from src.ingestion.streaming.binance_producer import (
    BinanceFuturesKafkaProducer,
    extract_trade_payload,
)


class DummyProducer:
    def __init__(self, fail_times: int = 0) -> None:
        self.sent: list[tuple[str, str, str]] = []
        self.fail_times = fail_times
        self.poll_calls: list[int] = []

    def produce(self, topic, key, value, callback):  # noqa: ANN001
        if self.fail_times > 0:
            self.fail_times -= 1
            raise BufferError("queue is full")
        self.sent.append((topic, key, value))

    def poll(self, timeout):  # noqa: ANN001
        self.poll_calls.append(timeout)
        return None

    def flush(self, _=0):
        return None


def test_extract_trade_payload_from_combined_stream() -> None:
    message = {
        "stream": "btcusdt@aggTrade",
        "data": {"s": "BTCUSDT", "T": 1, "p": "1", "q": "2", "m": False, "a": 3},
    }

    payload = extract_trade_payload(message)
    assert payload["s"] == "BTCUSDT"


def test_extract_trade_payload_from_direct_payload() -> None:
    message = {"s": "BTCUSDT", "T": 1, "p": "1", "q": "2", "m": False, "a": 3}

    payload = extract_trade_payload(message)
    assert payload["a"] == 3


def test_extract_trade_payload_raises_for_missing_fields() -> None:
    message = {"data": {"s": "BTCUSDT"}}

    try:
        extract_trade_payload(message)
        raise AssertionError("Expected ValueError for missing fields")
    except ValueError as exc:
        assert "Missing Binance aggTrade fields" in str(exc)


def test_publish_retries_on_kafka_buffer_error() -> None:
    dummy = DummyProducer(fail_times=2)
    producer = BinanceFuturesKafkaProducer(kafka_producer=dummy)
    message = json.dumps(
        {"s": "BTCUSDT", "T": 1700000000000, "p": "42000.5", "q": "0.123", "m": False, "a": 1}
    )

    producer._publish(message)

    assert len(dummy.sent) == 1
    assert dummy.poll_calls[:2] == [1, 1]
    assert dummy.poll_calls[-1] == 0


def test_publish_serializes_numeric_fields() -> None:
    dummy = DummyProducer()
    producer = BinanceFuturesKafkaProducer(kafka_producer=dummy)
    message = json.dumps(
        {"s": "BTCUSDT", "T": 1700000000000, "p": "42000.5", "q": "0.123", "m": False, "a": 1}
    )

    producer._publish(message)
    _, key, raw = dummy.sent[0]
    payload = json.loads(raw)

    assert key == "BTCUSDT"
    assert isinstance(payload["price"], float)
    assert isinstance(payload["qty"], float)
