"""FastAPI routers for serving layer."""

from .analytics import router as analytics_router
from .health import router as health_router
from .prices import router as prices_router

__all__ = ["health_router", "prices_router", "analytics_router"]
