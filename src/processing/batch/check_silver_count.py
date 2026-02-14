import time
from uuid import uuid4

import structlog

from src.config.logging import configure_logging
from src.config.settings import settings
from src.processing.spark_session import build_spark_session

configure_logging(settings.log_level)
logger = structlog.get_logger(__name__)


def main() -> None:
    run_id = str(uuid4())
    started_at = time.perf_counter()
    logger.info("job_started", job="check_silver_count", run_id=run_id)

    spark = build_spark_session("CryptoLake-CheckSilverCount")
    table = f"{settings.iceberg_catalog_name}.silver.ohlcv_1m"
    count_value = spark.table(table).count()
    duration_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info("silver_count_checked", run_id=run_id, table=table, count=count_value)
    logger.info("job_finished", job="check_silver_count", run_id=run_id, duration_ms=duration_ms)
    if count_value <= 0:
        raise RuntimeError(f"Silver table {table} is empty")


if __name__ == "__main__":
    main()
