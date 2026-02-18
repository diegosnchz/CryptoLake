"""Data quality validators for Bronze, Silver and Gold layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

try:
    from pyspark.sql import SparkSession
except ModuleNotFoundError:  # pragma: no cover - unit tests can run without pyspark
    SparkSession = Any  # type: ignore[assignment]


class CheckStatus(str, Enum):
    """Status for one quality check."""

    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class CheckResult:
    """Result for one data quality check."""

    check_name: str
    layer: str
    table_name: str
    status: CheckStatus
    metric_value: Optional[float] = None
    threshold: Optional[float] = None
    message: str = ""
    checked_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, str | float | None]:
        """Serialize result to plain dictionary."""
        return {
            "check_name": self.check_name,
            "layer": self.layer,
            "table_name": self.table_name,
            "status": self.status.value,
            "metric_value": self.metric_value,
            "threshold": self.threshold,
            "message": self.message,
            "checked_at": self.checked_at,
        }


class BaseValidator:
    """Common utilities used by all validators."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.results: list[CheckResult] = []

    def _add(self, result: CheckResult) -> None:
        self.results.append(result)

    def _exists(self, table: str) -> bool:
        try:
            self.spark.sql(f"DESCRIBE TABLE {table}")
            return True
        except Exception:
            return False

    def _count(self, table: str) -> int:
        row = self.spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").first()
        return int(row.cnt) if row and row.cnt is not None else 0

    def _nulls(self, table: str, column: str) -> int:
        row = self.spark.sql(f"SELECT COUNT(*) AS cnt FROM {table} WHERE {column} IS NULL").first()
        return int(row.cnt) if row and row.cnt is not None else 0

    def _columns(self, table: str) -> set[str]:
        return set(self.spark.table(table).columns)

    def get_summary(self) -> dict[str, float]:
        """Return summary counters and pass rate."""
        total = len(self.results)
        passed = sum(1 for item in self.results if item.status == CheckStatus.PASSED)
        failed = sum(1 for item in self.results if item.status == CheckStatus.FAILED)
        warnings = sum(1 for item in self.results if item.status == CheckStatus.WARNING)
        errors = sum(1 for item in self.results if item.status == CheckStatus.ERROR)
        pass_rate = round((passed / total) * 100.0, 1) if total else 0.0
        return {
            "total": float(total),
            "passed": float(passed),
            "failed": float(failed),
            "warnings": float(warnings),
            "errors": float(errors),
            "pass_rate": pass_rate,
        }


class BronzeValidator(BaseValidator):
    """Validate Bronze tables."""

    TABLES = {
        "cryptolake.bronze.historical_prices": {
            "min_rows": 100,
            "required_columns": [
                "coin_id",
                "timestamp_ms",
                "price_usd",
                "market_cap_usd",
                "volume_24h_usd",
                "_ingested_at",
                "_source",
                "_loaded_at",
            ],
            "freshness_col": "_loaded_at",
        },
        "cryptolake.bronze.fear_greed": {
            "min_rows": 30,
            "required_columns": [
                "value",
                "classification",
                "timestamp",
                "_ingested_at",
                "_source",
                "_loaded_at",
            ],
            "freshness_col": "_loaded_at",
        },
    }

    def check_all(self) -> list[CheckResult]:
        for table_name, config in self.TABLES.items():
            self._check_exists(table_name)
            if not self._exists(table_name):
                continue
            self._check_min_rows(table_name, int(config["min_rows"]))
            self._check_schema(table_name, list(config["required_columns"]))
            self._check_freshness(table_name, str(config["freshness_col"]))
        return self.results

    def _check_exists(self, table_name: str) -> None:
        exists = self._exists(table_name)
        self._add(
            CheckResult(
                check_name="table_exists",
                layer="bronze",
                table_name=table_name,
                status=CheckStatus.PASSED if exists else CheckStatus.FAILED,
                message="Table exists" if exists else "Table not found",
            )
        )

    def _check_min_rows(self, table_name: str, min_rows: int) -> None:
        count = self._count(table_name)
        self._add(
            CheckResult(
                check_name="min_row_count",
                layer="bronze",
                table_name=table_name,
                status=CheckStatus.PASSED if count >= min_rows else CheckStatus.FAILED,
                metric_value=float(count),
                threshold=float(min_rows),
                message=f"Rows={count}, expected>={min_rows}",
            )
        )

    def _check_schema(self, table_name: str, required_columns: list[str]) -> None:
        columns = self._columns(table_name)
        missing = sorted(set(required_columns) - columns)
        self._add(
            CheckResult(
                check_name="schema_check",
                layer="bronze",
                table_name=table_name,
                status=CheckStatus.PASSED if not missing else CheckStatus.FAILED,
                message="All expected columns exist" if not missing else f"Missing: {missing}",
            )
        )

    def _check_freshness(self, table_name: str, timestamp_col: str) -> None:
        try:
            row = self.spark.sql(
                f"""
                SELECT
                    CAST(
                        (unix_timestamp(current_timestamp()) - unix_timestamp(MAX({timestamp_col})))
                        / 3600.0
                    AS DOUBLE) AS hours_ago
                FROM {table_name}
                """
            ).first()
            hours_ago = row.hours_ago if row else None
            if hours_ago is None:
                self._add(
                    CheckResult(
                        check_name="data_freshness",
                        layer="bronze",
                        table_name=table_name,
                        status=CheckStatus.WARNING,
                        message="Could not calculate freshness",
                    )
                )
                return

            threshold = 48.0
            if hours_ago <= threshold:
                status = CheckStatus.PASSED
            elif hours_ago <= 72:
                status = CheckStatus.WARNING
            else:
                status = CheckStatus.FAILED

            self._add(
                CheckResult(
                    check_name="data_freshness",
                    layer="bronze",
                    table_name=table_name,
                    status=status,
                    metric_value=float(hours_ago),
                    threshold=threshold,
                    message=f"Latest row age={hours_ago:.2f}h",
                )
            )
        except Exception as exc:
            self._add(
                CheckResult(
                    check_name="data_freshness",
                    layer="bronze",
                    table_name=table_name,
                    status=CheckStatus.ERROR,
                    message=f"Error while validating freshness: {exc}",
                )
            )


class SilverValidator(BaseValidator):
    """Validate Silver tables."""

    def check_all(self) -> list[CheckResult]:
        self._check_daily_prices()
        self._check_fear_greed()
        return self.results

    def _check_daily_prices(self) -> None:
        table_name = "cryptolake.silver.daily_prices"
        if not self._exists(table_name):
            self._add(
                CheckResult(
                    check_name="table_exists",
                    layer="silver",
                    table_name=table_name,
                    status=CheckStatus.FAILED,
                    message="Table not found",
                )
            )
            return

        dups = self.spark.sql(
            f"""
            SELECT COUNT(*) AS cnt
            FROM (
                SELECT coin_id, price_date, COUNT(*) AS records
                FROM {table_name}
                GROUP BY coin_id, price_date
                HAVING records > 1
            )
            """
        ).first()
        duplicates = int(dups.cnt) if dups else 0
        self._add(
            CheckResult(
                check_name="no_duplicates",
                layer="silver",
                table_name=table_name,
                status=CheckStatus.PASSED if duplicates == 0 else CheckStatus.FAILED,
                metric_value=float(duplicates),
                threshold=0.0,
                message=f"Duplicate keys={duplicates}",
            )
        )

        negative_prices = self.spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_name} WHERE price_usd <= 0"
        ).first()
        invalid_price_rows = int(negative_prices.cnt) if negative_prices else 0
        self._add(
            CheckResult(
                check_name="positive_prices",
                layer="silver",
                table_name=table_name,
                status=CheckStatus.PASSED if invalid_price_rows == 0 else CheckStatus.FAILED,
                metric_value=float(invalid_price_rows),
                threshold=0.0,
                message=f"Rows with price_usd<=0: {invalid_price_rows}",
            )
        )

        for key_col in ("coin_id", "price_date", "price_usd"):
            nulls = self._nulls(table_name, key_col)
            self._add(
                CheckResult(
                    check_name=f"not_null_{key_col}",
                    layer="silver",
                    table_name=table_name,
                    status=CheckStatus.PASSED if nulls == 0 else CheckStatus.FAILED,
                    metric_value=float(nulls),
                    threshold=0.0,
                    message=f"Nulls in {key_col}: {nulls}",
                )
            )

        future_dates = self.spark.sql(
            f"SELECT COUNT(*) AS cnt FROM {table_name} WHERE price_date > CURRENT_DATE()"
        ).first()
        future_rows = int(future_dates.cnt) if future_dates else 0
        self._add(
            CheckResult(
                check_name="no_future_dates",
                layer="silver",
                table_name=table_name,
                status=CheckStatus.PASSED if future_rows == 0 else CheckStatus.FAILED,
                metric_value=float(future_rows),
                threshold=0.0,
                message=f"Rows with future dates: {future_rows}",
            )
        )

    def _check_fear_greed(self) -> None:
        table_name = "cryptolake.silver.fear_greed"
        if not self._exists(table_name):
            self._add(
                CheckResult(
                    check_name="table_exists",
                    layer="silver",
                    table_name=table_name,
                    status=CheckStatus.FAILED,
                    message="Table not found",
                )
            )
            return

        out_of_range = self.spark.sql(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {table_name}
            WHERE fear_greed_value < 0 OR fear_greed_value > 100
            """
        ).first()
        bad_range = int(out_of_range.cnt) if out_of_range else 0
        self._add(
            CheckResult(
                check_name="value_range_0_100",
                layer="silver",
                table_name=table_name,
                status=CheckStatus.PASSED if bad_range == 0 else CheckStatus.FAILED,
                metric_value=float(bad_range),
                threshold=0.0,
                message=f"Values out of range: {bad_range}",
            )
        )

        valid_labels = [
            "Extreme Fear",
            "Fear",
            "Neutral",
            "Greed",
            "Extreme Greed",
        ]
        in_clause = ", ".join(f"'{label}'" for label in valid_labels)
        invalid_label_rows = self.spark.sql(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {table_name}
            WHERE classification NOT IN ({in_clause})
            """
        ).first()
        invalid = int(invalid_label_rows.cnt) if invalid_label_rows else 0
        self._add(
            CheckResult(
                check_name="valid_classifications",
                layer="silver",
                table_name=table_name,
                status=CheckStatus.PASSED if invalid == 0 else CheckStatus.FAILED,
                metric_value=float(invalid),
                threshold=0.0,
                message=f"Rows with invalid labels: {invalid}",
            )
        )

        index_nulls = self._nulls(table_name, "index_date")
        self._add(
            CheckResult(
                check_name="not_null_index_date",
                layer="silver",
                table_name=table_name,
                status=CheckStatus.PASSED if index_nulls == 0 else CheckStatus.FAILED,
                metric_value=float(index_nulls),
                threshold=0.0,
                message=f"Null index_date rows: {index_nulls}",
            )
        )


class GoldValidator(BaseValidator):
    """Validate Gold star schema tables."""

    def check_all(self) -> list[CheckResult]:
        self._check_dim_coins()
        self._check_dim_dates()
        self._check_fact_market_daily()
        self._check_referential_integrity()
        return self.results

    def _check_dim_coins(self) -> None:
        table_name = "cryptolake.gold.dim_coins"
        if not self._exists(table_name):
            self._add(
                CheckResult(
                    check_name="table_exists",
                    layer="gold",
                    table_name=table_name,
                    status=CheckStatus.FAILED,
                    message="Table not found",
                )
            )
            return

        total = self._count(table_name)
        distinct = self.spark.sql(
            f"SELECT COUNT(DISTINCT coin_id) AS cnt FROM {table_name}"
        ).first()
        distinct_count = int(distinct.cnt) if distinct else 0
        duplicated = total - distinct_count
        self._add(
            CheckResult(
                check_name="coin_id_unique",
                layer="gold",
                table_name=table_name,
                status=CheckStatus.PASSED if duplicated == 0 else CheckStatus.FAILED,
                metric_value=float(duplicated),
                threshold=0.0,
                message=f"Duplicated coin_id rows: {duplicated}",
            )
        )

    def _check_dim_dates(self) -> None:
        table_name = "cryptolake.gold.dim_dates"
        if not self._exists(table_name):
            self._add(
                CheckResult(
                    check_name="table_exists",
                    layer="gold",
                    table_name=table_name,
                    status=CheckStatus.FAILED,
                    message="Table not found",
                )
            )
            return

        row = self.spark.sql(
            f"""
            SELECT
                COUNT(DISTINCT date_day) AS actual_days,
                DATEDIFF(MAX(date_day), MIN(date_day)) + 1 AS expected_days
            FROM {table_name}
            """
        ).first()
        actual_days = int(row.actual_days) if row else 0
        expected_days = int(row.expected_days) if row and row.expected_days else 0
        gaps = max(expected_days - actual_days, 0)
        self._add(
            CheckResult(
                check_name="no_date_gaps",
                layer="gold",
                table_name=table_name,
                status=CheckStatus.PASSED if gaps == 0 else CheckStatus.FAILED,
                metric_value=float(gaps),
                threshold=0.0,
                message=f"Missing calendar dates: {gaps}",
            )
        )

    def _check_fact_market_daily(self) -> None:
        table_name = "cryptolake.gold.fact_market_daily"
        if not self._exists(table_name):
            self._add(
                CheckResult(
                    check_name="table_exists",
                    layer="gold",
                    table_name=table_name,
                    status=CheckStatus.FAILED,
                    message="Table not found",
                )
            )
            return

        count = self._count(table_name)
        self._add(
            CheckResult(
                check_name="fact_not_empty",
                layer="gold",
                table_name=table_name,
                status=CheckStatus.PASSED if count > 0 else CheckStatus.FAILED,
                metric_value=float(count),
                threshold=1.0,
                message=f"Fact rows: {count}",
            )
        )

    def _check_referential_integrity(self) -> None:
        fact_table = "cryptolake.gold.fact_market_daily"
        coin_table = "cryptolake.gold.dim_coins"
        date_table = "cryptolake.gold.dim_dates"

        if not (self._exists(fact_table) and self._exists(coin_table) and self._exists(date_table)):
            self._add(
                CheckResult(
                    check_name="referential_integrity_ready",
                    layer="gold",
                    table_name=fact_table,
                    status=CheckStatus.WARNING,
                    message="Skipped FK checks because one or more tables are missing",
                )
            )
            return

        missing_coin_fk = self.spark.sql(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {fact_table} f
            LEFT JOIN {coin_table} c
                ON f.coin_id = c.coin_id
            WHERE c.coin_id IS NULL
            """
        ).first()
        missing_coin_rows = int(missing_coin_fk.cnt) if missing_coin_fk else 0
        self._add(
            CheckResult(
                check_name="fk_coin_id_exists",
                layer="gold",
                table_name=fact_table,
                status=CheckStatus.PASSED if missing_coin_rows == 0 else CheckStatus.FAILED,
                metric_value=float(missing_coin_rows),
                threshold=0.0,
                message=f"Fact rows with unknown coin_id: {missing_coin_rows}",
            )
        )

        missing_date_fk = self.spark.sql(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {fact_table} f
            LEFT JOIN {date_table} d
                ON f.price_date = d.date_day
            WHERE d.date_day IS NULL
            """
        ).first()
        missing_date_rows = int(missing_date_fk.cnt) if missing_date_fk else 0
        self._add(
            CheckResult(
                check_name="fk_price_date_exists",
                layer="gold",
                table_name=fact_table,
                status=CheckStatus.PASSED if missing_date_rows == 0 else CheckStatus.FAILED,
                metric_value=float(missing_date_rows),
                threshold=0.0,
                message=f"Fact rows with unknown date: {missing_date_rows}",
            )
        )
