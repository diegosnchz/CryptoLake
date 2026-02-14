import argparse

import structlog
from pyspark.sql.functions import col, from_json, to_date

from src.config.logging import configure_logging
from src.config.settings import settings
from src.processing.schemas.bronze import futures_trade_schema
from src.processing.spark_session import build_spark_session

configure_logging(settings.log_level)
logger = structlog.get_logger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stream Kafka futures trades into Iceberg bronze table",
    )
    parser.add_argument(
        "--mode",
        choices=("continuous", "available-now"),
        default="continuous",
        help="continuous keeps the stream running; available-now drains current offsets and exits",
    )
    parser.add_argument(
        "--trigger-seconds",
        type=int,
        default=30,
        help="processing trigger for continuous mode",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    spark = build_spark_session("CryptoLake-StreamToBronze")
    catalog = settings.iceberg_catalog_name
    table = f"{catalog}.{settings.iceberg_namespace_bronze}.futures_trades"

    starting_offsets = "earliest" if args.mode == "available-now" else "latest"

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic_prices_realtime)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), futures_trade_schema).alias("event")
    ).select("event.*").where(col("event_time").isNotNull())
    bronze_df = parsed.withColumn("event_date", to_date(col("event_time")))

    checkpoint = f"s3a://{settings.minio_bucket_bronze}/checkpoints/bronze/futures_trades"
    writer = bronze_df.writeStream.format("iceberg").outputMode("append").option(
        "checkpointLocation", checkpoint
    )
    if args.mode == "available-now":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{args.trigger_seconds} seconds")

    query = writer.toTable(table)
    logger.info(
        "bronze_stream_started",
        table=table,
        mode=args.mode,
        topic=settings.kafka_topic_prices_realtime,
        checkpoint=checkpoint,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
