import argparse

import structlog
from pyspark.sql.functions import col, current_timestamp, from_json, lit, to_date, when

from src.config.logging import configure_logging
from src.config.settings import settings
from src.contracts.spark_schema import TRADE_EVENT_V1_SPARK_SCHEMA
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
        from_json(col("value").cast("string"), TRADE_EVENT_V1_SPARK_SCHEMA).alias("event")
    ).select("event.*")

    bronze_df = (
        parsed.where(
            col("symbol").isNotNull()
            & col("event_time_ms").isNotNull()
            & col("price").isNotNull()
            & col("quantity").isNotNull()
        )
        .withColumn("event_time", (col("event_time_ms") / 1000).cast("timestamp"))
        .withColumn(
            "side",
            when(col("is_buyer_maker").isNull(), lit(None).cast("string"))
            .when(col("is_buyer_maker"), lit("sell"))
            .otherwise(lit("buy")),
        )
        .withColumn("qty", col("quantity"))
        .withColumn("exchange", lit("binance_futures"))
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("event_date", to_date(col("event_time")))
        .select(
            "symbol",
            "event_time",
            "price",
            "qty",
            "side",
            "trade_id",
            "exchange",
            "ingest_ts",
            "event_date",
        )
    )

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
