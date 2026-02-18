"""Entry point for quality checks executed with spark-submit."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from src.quality.validators import (
    BronzeValidator,
    CheckStatus,
    GoldValidator,
    SilverValidator,
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Run CryptoLake data quality checks")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Layer to validate.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional path for JSON report.",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Return non-zero exit code if warnings are found.",
    )
    return parser.parse_args()


def run_checks(spark: SparkSession, layer: str) -> list[dict]:
    """Run validators for selected layer and return serialized results."""
    validators = []
    if layer in ("bronze", "all"):
        validators.append(BronzeValidator(spark))
    if layer in ("silver", "all"):
        validators.append(SilverValidator(spark))
    if layer in ("gold", "all"):
        validators.append(GoldValidator(spark))

    all_results = []
    for validator in validators:
        validator.check_all()
        all_results.extend(validator.results)
    return [result.to_dict() for result in all_results]


def print_report(results: list[dict]) -> dict[str, int]:
    """Print human-readable report and return status counters."""
    counters = {
        CheckStatus.PASSED.value: 0,
        CheckStatus.FAILED.value: 0,
        CheckStatus.WARNING.value: 0,
        CheckStatus.ERROR.value: 0,
    }

    print("=" * 88)
    print("CryptoLake Data Quality Report")
    print("=" * 88)
    for result in results:
        status = str(result["status"])
        counters[status] += 1
        print(
            f"[{status.upper():7}] "
            f"{result['layer']:<6} {result['check_name']:<24} "
            f"{result['table_name']:<36} {result['message']}"
        )

    total = len(results)
    print("-" * 88)
    print(
        f"Total={total} "
        f"Passed={counters['passed']} "
        f"Failed={counters['failed']} "
        f"Warnings={counters['warning']} "
        f"Errors={counters['error']}"
    )
    print("=" * 88)
    return counters


def save_output(path: str, results: list[dict], counters: dict[str, int]) -> None:
    """Save JSON report to path."""
    payload = {"summary": counters, "results": results}
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"JSON report written to: {file_path}")


def main() -> int:
    """Program entry point."""
    args = parse_args()
    spark = SparkSession.builder.appName(f"CryptoLake-Quality-{args.layer}").getOrCreate()
    try:
        results = run_checks(spark, args.layer)
        counters = print_report(results)
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
