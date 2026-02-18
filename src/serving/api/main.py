"""FastAPI application for CryptoLake serving layer."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.serving.api.routes.analytics import router as analytics_router
from src.serving.api.routes.health import router as health_router
from src.serving.api.routes.prices import router as prices_router

app = FastAPI(
    title="CryptoLake Serving API",
    description="REST API over Gold tables exposed by Spark Thrift Server.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(prices_router)
app.include_router(analytics_router)


@app.get("/", tags=["meta"])
def root() -> dict[str, str]:
    """Quick metadata endpoint."""
    return {
        "service": "cryptolake-serving-api",
        "docs": "/docs",
        "status_endpoint": "/health",
    }
