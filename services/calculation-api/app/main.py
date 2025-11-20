"""
Calculation API - Technical indicators, risk analytics, and portfolio optimization

This service provides REST endpoints for:
- Technical indicators (MA, RSI, MACD, Bollinger Bands, etc.)
- Risk analytics (VaR, Sharpe, Sortino, Monte Carlo)
- Portfolio optimization (mean-variance, efficient frontier)
"""

import os
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

# CORS middleware - Security: Whitelist specific origins only
# Get allowed origins from environment variable (comma-separated)
allowed_origins_str = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000")
allowed_origins = [origin.strip() for origin in allowed_origins_str.split(",")]

logger.info(f"CORS allowed origins: {allowed_origins}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,  # Whitelist only trusted origins
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],  # Explicit methods only
    allow_headers=["Content-Type", "Authorization"],  # Explicit headers only
    max_age=600,  # Cache preflight requests for 10 minutes
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

    # Bind to 0.0.0.0 for Docker container accessibility
    uvicorn.run(app, host="0.0.0.0", port=8005, log_level="info")  # nosec B104
