"""
Backtest API - Strategy backtesting (Simplified for Phase 3)

This is a simplified version focusing on basic API structure.
Full implementation will be completed in later phases.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger
from pydantic import BaseModel


class HealthResponse(BaseModel):
    """Health check response"""

    status: str
    version: str
    database: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle handler for FastAPI app"""
    logger.info("Starting Backtest API...")
    yield
    logger.info("Shutting down Backtest API...")


# FastAPI app
app = FastAPI(
    title="Backtest API",
    description="Strategy backtesting and performance analysis service",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


@app.get("/", response_model=HealthResponse)
async def root():
    """Health check endpoint"""
    return HealthResponse(status="healthy", version="1.0.0", database="postgresql")


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint"""
    return HealthResponse(status="healthy", version="1.0.0", database="postgresql")


if __name__ == "__main__":
    import uvicorn

    # Bind to 0.0.0.0 for Docker container accessibility
    uvicorn.run(app, host="0.0.0.0", port=8001)  # nosec B104
