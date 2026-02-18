"""Database access layer for Spark Thrift Server."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from typing import Any, Iterator, Sequence

from pyhive import hive

from src.config.settings import settings


@dataclass(frozen=True)
class ThriftConfig:
    """Connection configuration for Spark Thrift."""

    host: str = settings.thrift_host
    port: int = settings.thrift_port
    username: str = settings.thrift_username
    auth: str = settings.thrift_auth
    database: str = settings.thrift_database


class SparkThriftClient:
    """Simple query client for Spark Thrift."""

    def __init__(self, config: ThriftConfig):
        self.config = config

    @contextmanager
    def connect(self) -> Iterator[hive.Connection]:
        """Open a connection and close it automatically."""
        connection = hive.Connection(
            host=self.config.host,
            port=self.config.port,
            username=self.config.username,
            auth=self.config.auth,
            database=self.config.database,
        )
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return value

    def fetch_all(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Execute query and return rows as dictionaries."""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, tuple(params or ()))
            columns = [column[0] for column in cursor.description or []]
            rows = cursor.fetchall()

        result: list[dict[str, Any]] = []
        for row in rows:
            result.append(
                {col: self._normalize_value(value) for col, value in zip(columns, row, strict=True)}
            )
        return result

    def fetch_one(self, query: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        """Execute query and return one row or None."""
        rows = self.fetch_all(query=query, params=params)
        return rows[0] if rows else None

    def ping(self) -> bool:
        """Return True when thrift connection is working."""
        try:
            row = self.fetch_one("SELECT 1 AS ok")
            return bool(row and row.get("ok") == 1)
        except Exception:
            return False

    def count_existing_gold_tables(self) -> int:
        """Return number of expected gold tables present in schema."""
        expected_tables = {"dim_coins", "dim_dates", "fact_market_daily"}
        rows = self.fetch_all("SHOW TABLES IN gold")
        actual_table_names = {str(row.get("tableName", "")).strip() for row in rows}
        return len(expected_tables.intersection(actual_table_names))


@lru_cache(maxsize=1)
def get_thrift_client() -> SparkThriftClient:
    """Provide singleton SparkThriftClient."""
    return SparkThriftClient(config=ThriftConfig())
