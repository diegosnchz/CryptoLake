"""Health check routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from src.serving.api.database import SparkThriftClient, get_thrift_client
from src.serving.api.models.schemas import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health(client: SparkThriftClient = Depends(get_thrift_client)) -> HealthResponse:
    """Return health status for serving API and Spark Thrift connectivity."""
    connected = client.ping()
    tables_available = client.count_existing_gold_tables() if connected else 0

    if connected and tables_available >= 3:
        return HealthResponse(
            status="healthy",
            thrift_connected=True,
            tables_available=tables_available,
            details="Serving API connected to Spark Thrift and Gold tables are available.",
        )

    return HealthResponse(
        status="degraded",
        thrift_connected=connected,
        tables_available=tables_available,
        details="Thrift connectivity or gold tables are incomplete.",
    )
