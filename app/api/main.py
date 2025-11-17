"""
FastAPI Application - REST API for Stock Picker Backtesting

This module provides a RESTful API for:
- Running backtests
- Managing strategies
- Querying results
- Retrieving trade history and equity curves

Run with:
    uvicorn app.api.main:app --reload
    # or
    uvicorn app.api.main:app --host 0.0.0.0 --port 8000
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from app.api.dependencies import get_engine
from app.constant.version import VERSION


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI

    Handles startup and shutdown events.
    """
    # Startup
    logger.info("Starting Stock Picker API...")
    logger.info(f"Version: {VERSION}")

    # Initialize database engine
    engine = get_engine()
    logger.info(f"Database connected: {engine.url}")

    yield

    # Shutdown
    logger.info("Shutting down Stock Picker API...")
    engine.dispose()


# Create FastAPI application
app = FastAPI(
    title="Stock Picker Backtesting API",
    description="REST API for backtesting trading strategies on Chinese stock markets",
    version=VERSION,
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Health check endpoint
@app.get("/api/health", tags=["Health"])
async def health_check():
    """
    Health check endpoint

    Returns API status and version information.
    """
    from app.api.models import HealthResponse

    engine = get_engine()

    return HealthResponse(
        status="healthy",
        version=VERSION,
        database=str(engine.url).split("@")[1] if "@" in str(engine.url) else "connected"
    )


# Root endpoint
@app.get("/", tags=["Root"])
async def root():
    """
    API root endpoint

    Returns welcome message and API information.
    """
    return {
        "message": "Stock Picker Backtesting API",
        "version": VERSION,
        "docs": "/api/docs",
        "health": "/api/health"
    }


# Import and include routers
from app.api.routes import backtest, strategies

app.include_router(backtest.router, prefix="/api/v1", tags=["Backtests"])
app.include_router(strategies.router, prefix="/api/v1", tags=["Strategies"])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
