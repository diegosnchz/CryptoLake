"""Entry point for quality checks executed with spark-submit."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.quality.validators import (
    BronzeValidator,
    CheckResult,
    CheckStatus,
    GoldValidator,
    SilverValidator,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

RESULTS_TABLE = "cryptolake.quality.check_results"
RESULTS_SCHEMA = StructType(
    [
        StructField("check_name", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("metric_value", DoubleType(), True),
        StructField("threshold", DoubleType(), True),
        StructField("message", StringType(), True),
        StructField("checked_at", StringType(), False),
        StructField("run_id", StringType(), False),
    ]
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Run CryptoLake data quality checks.")
    parser.add_argument(
        "--layer",
        action="append",
        choices=["bronze", "silver", "gold", "all"],
        help="Layer to validate. Repeat --layer to run multiple layers.",
    )
    parser.add_argument("--output", default="", help="Optional JSON report output path.")
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Return non-zero exit code when warnings are found.",
    )
    return parser.parse_args()


def resolve_layers(raw_layers: Sequence[str] | None) -> list[str]:
    """Resolve CLI layers into concrete validator order."""
    if not raw_layers:
        return ["bronze", "silver", "gold"]
    if "all" in raw_layers:
        return ["bronze", "silver", "gold"]

    ordered: list[str] = []
    for layer in raw_layers:
        if layer not in ordered:
            ordered.append(layer)
    return ordered


def run_checks(spark: SparkSession, layers: list[str]) -> list[CheckResult]:
    """Run validators for selected layers."""
    validators = {
        "bronze": BronzeValidator(spark),
        "silver": SilverValidator(spark),
        "gold": GoldValidator(spark),
    }

    results: list[CheckResult] = []
    for layer in layers:
        results.extend(validators[layer].check_all())
    return results


def summarize(results: list[CheckResult]) -> dict[str, int]:
    """Build status summary counters."""
    counters = {
        CheckStatus.PASSED.value: 0,
        CheckStatus.FAILED.value: 0,
        CheckStatus.WARNING.value: 0,
        CheckStatus.ERROR.value: 0,
    }
    for result in results:
        counters[result.status.value] += 1
    return counters


def print_report(results: list[CheckResult], counters: dict[str, int]) -> None:
    """Print human-readable report to stdout."""
    print("=" * 96)
    print("CryptoLake Data Quality Report")
    print("=" * 96)
    for result in results:
        print(
            f"[{result.status.value.upper():7}] "
            f"{result.layer:<6} {result.check_name:<26} {result.table_name:<36} {result.message}"
        )
    total = len(results)
    print("-" * 96)
    print(
        f"Total={total} "
        f"Passed={counters['passed']} "
        f"Failed={counters['failed']} "
        f"Warnings={counters['warning']} "
        f"Errors={counters['error']}"
    )
    print("=" * 96)


def save_output(path: str, results: list[CheckResult], counters: dict[str, int]) -> None:
    """Save JSON report to path."""
    payload = {"summary": counters, "results": [result.to_dict() for result in results]}
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("JSON report written to %s", output_path)


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """Check whether table exists in catalog."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False


def persist_results(spark: SparkSession, results: list[CheckResult], run_id: str) -> None:
    """Persist check results in Iceberg table."""
    rows = [dict(**result.to_dict(), run_id=run_id) for result in results]
    if not rows:
        logger.info("No quality rows to persist.")
        return

    spark.sql("CREATE NAMESPACE IF NOT EXISTS cryptolake.quality")
    dataframe = spark.createDataFrame(rows, schema=RESULTS_SCHEMA)

    if table_exists(spark, RESULTS_TABLE):
        dataframe.writeTo(RESULTS_TABLE).append()
    else:
        dataframe.writeTo(RESULTS_TABLE).using("iceberg").create()
    logger.info("Persisted %s quality rows into %s", len(rows), RESULTS_TABLE)


def main() -> int:
    """Program entry point."""
    args = parse_args()
    layers = resolve_layers(args.layer)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    spark = SparkSession.builder.appName("CryptoLake-Quality").getOrCreate()
    try:
        logger.info("Running quality checks (run_id=%s, layers=%s)", run_id, layers)
        results = run_checks(spark, layers)
        counters = summarize(results)
        print_report(results, counters)
        persist_results(spark, results, run_id)

        if args.output:
            save_output(args.output, results, counters)

        should_fail = counters["failed"] > 0 or counters["error"] > 0
        if args.fail_on_warning:
            should_fail = should_fail or counters["warning"] > 0
        return 1 if should_fail else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
