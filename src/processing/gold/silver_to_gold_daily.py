import structlog
from pyspark.sql import functions as F

from src.config.logging import configure_logging
from src.config.settings import settings
from src.processing.spark_session import build_spark_session

configure_logging(settings.log_level)
logger = structlog.get_logger(__name__)


def main() -> None:
    spark = build_spark_session("CryptoLake-SilverToGoldDaily")

    source = f"{settings.iceberg_catalog_name}.silver.futures_ohlcv_1m"
    target = f"{settings.iceberg_catalog_name}.gold.futures_daily_stats"

    daily = (
        spark.table(source)
        .withColumn("trade_date", F.to_date("window_start"))
        .groupBy("symbol", "trade_date")
        .agg(
            F.sum("volume").alias("total_volume"),
            F.max("high").alias("day_high"),
            F.min("low").alias("day_low"),
            F.sum("trades").alias("trades"),
        )
        .withColumn("price_range", F.col("day_high") - F.col("day_low"))
        .withColumn("updated_at", F.current_timestamp())
    )

    daily.createOrReplaceTempView("gold_updates")
    spark.sql(
        f"""
        MERGE INTO {target} t
        USING gold_updates s
        ON t.symbol = s.symbol AND t.trade_date = s.trade_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    logger.info("gold_upsert_done")


if __name__ == "__main__":
    main()
