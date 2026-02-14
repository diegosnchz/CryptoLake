import structlog
from pyspark.sql.functions import col, from_json, to_date

from src.config.logging import configure_logging
from src.config.settings import settings
from src.processing.schemas.bronze import futures_trade_schema
from src.processing.spark_session import build_spark_session

configure_logging(settings.log_level)
logger = structlog.get_logger(__name__)


def main() -> None:
    spark = build_spark_session("CryptoLake-StreamToBronze")
    catalog = settings.iceberg_catalog_name
    table = f"{catalog}.bronze.futures_trades"

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic_prices_realtime)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), futures_trade_schema).alias("event")
    ).select("event.*")
    bronze_df = parsed.withColumn("event_date", to_date(col("event_time")))

    checkpoint = f"s3a://{settings.minio_bucket_bronze}/checkpoints/bronze/futures_trades"
    query = (
        bronze_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="30 seconds")
        .toTable(table)
    )
    logger.info("bronze_stream_started", table=table)
    query.awaitTermination()


if __name__ == "__main__":
    main()
