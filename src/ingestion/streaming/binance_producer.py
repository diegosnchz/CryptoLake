import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import structlog
import websockets
from confluent_kafka import Producer

from src.config.logging import configure_logging
from src.config.settings import settings
from src.contracts.events import parse_binance_aggtrade

configure_logging(settings.log_level)
logger = structlog.get_logger(__name__)


def extract_trade_payload(message: dict[str, Any]) -> dict[str, Any]:
    payload = message.get("data", message)
    if not isinstance(payload, dict):
        raise ValueError("Websocket payload must be an object")

    required = {"s", "T", "p", "q", "m", "a"}
    missing = required - set(payload.keys())
    if missing:
        raise ValueError(f"Missing Binance aggTrade fields: {sorted(missing)}")

    return payload


class BinanceFuturesKafkaProducer:
    def __init__(self, kafka_producer: Producer | None = None) -> None:
        self.topic = settings.kafka_topic_prices_realtime
        self.max_backoff_seconds = max(
            1,
            getattr(settings, "producer_reconnect_max_backoff_seconds", 60),
        )
        self.producer = kafka_producer or Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": "binance-futures-producer",
                "acks": "all",
                "enable.idempotence": True,
            }
        )

    async def run_forever(self) -> None:
        streams = "/".join([f"{symbol}@aggTrade" for symbol in settings.symbols])
        ws_url = f"{settings.binance_ws_base_url}?streams={streams}"
        reconnect_delay = 1

        while True:
            try:
                await self._consume(ws_url)
                reconnect_delay = 1
            except asyncio.CancelledError:
                logger.info("producer_cancelled")
                raise
            except Exception as exc:  # noqa: BLE001
                delay = min(reconnect_delay, self.max_backoff_seconds)
                logger.warning(
                    "producer_retrying_after_error",
                    error=str(exc),
                    retry_in_seconds=delay,
                )
                await asyncio.sleep(delay)
                reconnect_delay = min(reconnect_delay * 2, self.max_backoff_seconds)

    async def _consume(self, ws_url: str) -> None:
        logger.info("connecting_websocket", ws_url=ws_url, topic=self.topic)
        async with websockets.connect(
            ws_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            max_queue=1000,
        ) as websocket:
            logger.info("websocket_connected", ws_url=ws_url)
            async for raw_message in websocket:
                try:
                    self._publish(raw_message)
                except ValueError as exc:
                    logger.warning("invalid_ws_message_skipped", error=str(exc))

    def _publish(self, raw_message: str) -> None:
        payload = extract_trade_payload(json.loads(raw_message))
        event = parse_binance_aggtrade(payload, ingest_ts=datetime.now(timezone.utc))
        encoded = json.dumps(
            {
                "symbol": event.symbol,
                "event_time": event.event_time.isoformat(),
                "price": float(event.price),
                "qty": float(event.qty),
                "side": event.side,
                "trade_id": event.trade_id,
                "exchange": event.exchange,
                "ingest_ts": event.ingest_ts.isoformat(),
            }
        )

        retries = 0
        while True:
            try:
                self.producer.produce(
                    self.topic,
                    key=event.symbol,
                    value=encoded,
                    callback=self._delivery_report,
                )
                self.producer.poll(0)
                return
            except BufferError:
                retries += 1
                if retries > 3:
                    logger.error(
                        "kafka_buffer_exhausted",
                        symbol=event.symbol,
                        trade_id=event.trade_id,
                    )
                    return
                logger.warning("kafka_buffer_full_retry", retries=retries)
                self.producer.poll(1)

    @staticmethod
    def _delivery_report(err, msg) -> None:  # noqa: ANN001
        if err is not None:
            logger.error("kafka_delivery_failed", error=str(err))
            return
        logger.debug(
            "kafka_delivery_ok",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )

    def flush(self) -> None:
        self.producer.flush(10)


async def _main() -> None:
    producer = BinanceFuturesKafkaProducer()
    try:
        await producer.run_forever()
    finally:
        producer.flush()


if __name__ == "__main__":
    asyncio.run(_main())
