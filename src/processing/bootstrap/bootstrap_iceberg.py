import structlog

from src.config.logging import configure_logging
from src.config.settings import settings
from src.processing.spark_session import build_spark_session

configure_logging(settings.log_level)
logger = structlog.get_logger(__name__)


def bootstrap() -> None:
    spark = build_spark_session("CryptoLake-IcebergBootstrap")
    catalog = settings.iceberg_catalog_name
    ns_bronze = settings.iceberg_namespace_bronze
    ns_silver = settings.iceberg_namespace_silver
    ns_gold = settings.iceberg_namespace_gold

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns_bronze}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns_silver}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns_gold}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{ns_bronze}.futures_trades (
            symbol STRING,
            event_time TIMESTAMP,
            price DOUBLE,
            qty DOUBLE,
            side STRING,
            trade_id BIGINT,
            exchange STRING,
            ingest_ts TIMESTAMP,
            event_date DATE
        ) USING iceberg
        PARTITIONED BY (days(event_time), symbol)
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{ns_silver}.futures_ohlcv_1m (
            symbol STRING,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            trades BIGINT,
            updated_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(window_start), symbol)
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{ns_gold}.futures_daily_stats (
            symbol STRING,
            trade_date DATE,
            total_volume DOUBLE,
            day_high DOUBLE,
            day_low DOUBLE,
            price_range DOUBLE,
            trades BIGINT,
            updated_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (trade_date, symbol)
        """
    )
    logger.info("bootstrap_done")


if __name__ == "__main__":
    bootstrap()
