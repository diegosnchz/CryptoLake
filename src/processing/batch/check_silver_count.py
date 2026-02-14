import structlog

from src.config.logging import configure_logging
from src.config.settings import settings
from src.processing.spark_session import build_spark_session

configure_logging(settings.log_level)
logger = structlog.get_logger(__name__)


def main() -> None:
    spark = build_spark_session("CryptoLake-CheckSilverCount")
    table = f"{settings.iceberg_catalog_name}.silver.ohlcv_1m"
    count_value = spark.table(table).count()
    logger.info("silver_count_checked", table=table, count=count_value)
    if count_value <= 0:
        raise RuntimeError(f"Silver table {table} is empty")


if __name__ == "__main__":
    main()
