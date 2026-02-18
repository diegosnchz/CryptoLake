"""FastAPI application for CryptoLake serving layer."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.serving.api.routes.analytics import router as analytics_router
from src.serving.api.routes.health import router as health_router
from src.serving.api.routes.prices import router as prices_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run startup and shutdown hooks."""
    print("CryptoLake API starting...")
    yield
    print("CryptoLake API shutting down...")


app = FastAPI(
    title="CryptoLake API",
    description=(
        "Real-time crypto analytics powered by a Lakehouse architecture. "
        "Queries Apache Iceberg tables via Spark Thrift Server."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(prices_router, prefix="/api/v1")
app.include_router(analytics_router, prefix="/api/v1")
app.include_router(health_router, prefix="/api/v1")


@app.get("/", tags=["meta"])
def root() -> dict[str, str]:
    """Quick metadata endpoint."""
    return {
        "project": "CryptoLake",
        "docs": "/docs",
        "health": "/api/v1/health",
    }
