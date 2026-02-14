import structlog
from pyspark.sql import functions as F

from src.config.logging import configure_logging
from src.config.settings import settings
from src.processing.spark_session import build_spark_session

configure_logging(settings.log_level)
logger = structlog.get_logger(__name__)


def main() -> None:
    spark = build_spark_session("CryptoLake-BronzeToSilver1m")
    catalog = settings.iceberg_catalog_name
    source = f"{catalog}.bronze.futures_trades"
    target = f"{catalog}.silver.ohlcv_1m"

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target} (
            symbol STRING,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            trades BIGINT,
            trade_date DATE,
            updated_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (trade_date, symbol)
        """
    )

    bronze_df = spark.table(source)
    silver_df = (
        bronze_df.groupBy(F.window("event_time", "1 minute"), F.col("symbol"))
        .agg(
            F.min_by("price", "event_time").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "event_time").alias("close"),
            F.sum("qty").alias("volume"),
            F.count("trade_id").alias("trades"),
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
            F.to_date(F.col("window.start")).alias("trade_date"),
            F.current_timestamp().alias("updated_at"),
        )
    )

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    silver_df.writeTo(target).overwritePartitions()
    logger.info("silver_1m_overwrite_done", table=target)


if __name__ == "__main__":
    main()
