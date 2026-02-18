"""Health check routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from src.serving.api.database import SparkThriftClient, get_thrift_client
from src.serving.api.models.schemas import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health(client: SparkThriftClient = Depends(get_thrift_client)) -> HealthResponse:
    """Return health status for serving API and Spark Thrift connectivity."""
    try:
        tables = client.fetch_all("SHOW TABLES IN cryptolake.gold")
        return HealthResponse(
            status="healthy",
            thrift_connected=True,
            tables_available=len(tables),
        )
    except Exception:
        return HealthResponse(
            status="degraded",
            thrift_connected=False,
            tables_available=0,
        )
