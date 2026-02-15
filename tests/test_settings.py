from src.config.settings import Settings


def test_settings_prefers_canonical_topic(monkeypatch) -> None:
    monkeypatch.setenv("KAFKA_TOPIC_PRICES_REALTIME", "prices.realtime")
    monkeypatch.setenv("KAFKA_TOPIC_FUTURES", "legacy.topic")
    parsed = Settings()

    assert parsed.kafka_topic_prices_realtime == "prices.realtime"
    assert parsed.kafka_topic_futures == "prices.realtime"


def test_settings_falls_back_to_legacy_topic(monkeypatch) -> None:
    monkeypatch.delenv("KAFKA_TOPIC_PRICES_REALTIME", raising=False)
    monkeypatch.setenv("KAFKA_TOPIC_FUTURES", "legacy.topic")
    parsed = Settings()

    assert parsed.kafka_topic_prices_realtime == "legacy.topic"
    assert parsed.kafka_topic_futures == "legacy.topic"
