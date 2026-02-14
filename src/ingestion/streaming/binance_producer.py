import asyncio
import json
from datetime import datetime, timezone

import structlog
import websockets
from confluent_kafka import Producer

from src.config.logging import configure_logging
from src.config.settings import settings
from src.contracts.events import parse_binance_aggtrade

configure_logging(settings.log_level)
logger = structlog.get_logger(__name__)


class BinanceFuturesKafkaProducer:
    def __init__(self) -> None:
        self.producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": "binance-futures-producer",
                "acks": "all",
            }
        )

    async def run(self) -> None:
        streams = "/".join([f"{symbol}@aggTrade" for symbol in settings.symbols])
        ws_url = f"{settings.binance_ws_base_url}?streams={streams}"
        logger.info(
            "connecting_websocket",
            ws_url=ws_url,
            topic=settings.kafka_topic_prices_realtime,
        )

        async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as websocket:
            while True:
                msg = await websocket.recv()
                data = json.loads(msg)
                if "data" not in data:
                    continue

                event = parse_binance_aggtrade(data["data"], ingest_ts=datetime.now(timezone.utc))
                encoded = event.model_dump_json()
                self.producer.produce(
                    settings.kafka_topic_prices_realtime,
                    key=event.symbol,
                    value=encoded,
                    callback=self._delivery_report,
                )
                self.producer.poll(0)

    @staticmethod
    def _delivery_report(err, msg) -> None:  # noqa: ANN001
        if err is not None:
            logger.error("kafka_delivery_failed", error=str(err))

    def flush(self) -> None:
        self.producer.flush(10)


async def _main() -> None:
    producer = BinanceFuturesKafkaProducer()
    try:
        await producer.run()
    finally:
        producer.flush()


if __name__ == "__main__":
    asyncio.run(_main())
