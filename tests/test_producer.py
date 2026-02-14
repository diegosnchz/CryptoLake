import json

from src.contracts.events import FuturesTradeEvent
from src.ingestion.streaming.binance_producer import BinanceFuturesKafkaProducer


class DummyProducer:
    def __init__(self) -> None:
        self.sent = []

    def produce(self, topic, key, value, callback):  # noqa: ANN001
        self.sent.append((topic, key, value))

    def poll(self, _):
        return None

    def flush(self, _=0):
        return None


def test_producer_serialization_topic(monkeypatch) -> None:
    producer = BinanceFuturesKafkaProducer()
    dummy = DummyProducer()
    producer.producer = dummy

    event = FuturesTradeEvent(
        symbol="BTCUSDT",
        event_time="2024-01-01T00:00:00Z",
        price="1",
        qty="2",
        side="buy",
        trade_id=1,
        exchange="binance_futures",
        ingest_ts="2024-01-01T00:00:01Z",
    )
    producer.producer.produce(
        "binance_futures_realtime", event.symbol, event.model_dump_json(), producer._delivery_report
    )
    assert len(dummy.sent) == 1
    topic, key, raw = dummy.sent[0]
    assert topic == "binance_futures_realtime"
    assert key == "BTCUSDT"
    assert json.loads(raw)["symbol"] == "BTCUSDT"
