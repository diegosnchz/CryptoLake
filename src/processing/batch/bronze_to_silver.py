import structlog
from pyspark.sql import functions as F

from src.config.logging import configure_logging
from src.config.settings import settings
from src.processing.spark_session import build_spark_session

configure_logging(settings.log_level)
logger = structlog.get_logger(__name__)


def upsert_microbatch(batch_df, batch_id: int) -> None:  # noqa: ANN001
    batch_df.createOrReplaceTempView("silver_updates")
    spark = batch_df.sparkSession
    target = f"{settings.iceberg_catalog_name}.silver.futures_ohlcv_1m"
    spark.sql(
        f"""
        MERGE INTO {target} t
        USING silver_updates s
        ON t.symbol = s.symbol AND t.window_start = s.window_start
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    logger.info("silver_upsert_batch", batch_id=batch_id)


def main() -> None:
    spark = build_spark_session("CryptoLake-BronzeToSilver")
    source = f"{settings.iceberg_catalog_name}.bronze.futures_trades"

    bronze_stream = spark.readStream.table(source).withWatermark("event_time", "10 minutes")

    agg = (
        bronze_stream.groupBy(F.window("event_time", "1 minute"), F.col("symbol"))
        .agg(
            F.min_by("price", "event_time").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "event_time").alias("close"),
            F.sum("qty").alias("volume"),
            F.countDistinct("trade_id").alias("trades"),
        )
        .select(
            "symbol",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trades",
            F.current_timestamp().alias("updated_at"),
        )
    )

    checkpoint = f"s3a://{settings.minio_bucket_silver}/checkpoints/silver/futures_ohlcv_1m"
    query = (
        agg.writeStream.outputMode("update")
        .option("checkpointLocation", checkpoint)
        .foreachBatch(upsert_microbatch)
        .trigger(processingTime="1 minute")
        .start()
    )
    logger.info("silver_stream_started")
    query.awaitTermination()


if __name__ == "__main__":
    main()
