"""
Calculation API - Technical indicators, risk analytics, and portfolio optimization

This service provides REST endpoints for:
- Technical indicators (MA, RSI, MACD, Bollinger Bands, etc.)
- Risk analytics (VaR, Sharpe, Sortino, Monte Carlo)
- Portfolio optimization (mean-variance, efficient frontier)
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from pydantic import BaseModel

from app.api import indicators, optimization, risk


class HealthResponse(BaseModel):
    """Health check response"""

    status: str
    version: str
    service: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events"""
    logger.info("Starting Calculation API service...")
    # Startup: Initialize Redis connection, load models, etc.
    yield
    # Shutdown: Clean up resources
    logger.info("Shutting down Calculation API service...")


app = FastAPI(
    title="Calculation API",
    description="Technical indicators, risk analytics, and portfolio optimization service",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(indicators.router, prefix="/api/v1/indicators", tags=["indicators"])
app.include_router(risk.router, prefix="/api/v1/risk", tags=["risk"])
app.include_router(optimization.router, prefix="/api/v1/optimization", tags=["optimization"])


@app.get("/", response_model=HealthResponse)
async def root():
    """Root endpoint - health check"""
    return HealthResponse(status="healthy", version="1.0.0", service="calculation-api")


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint"""
    return HealthResponse(status="healthy", version="1.0.0", service="calculation-api")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8005, log_level="info")
