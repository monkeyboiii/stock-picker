"""
Backtest API - Strategy backtesting and performance analysis

FastAPI service for:
- Running strategy backtests
- Storing and retrieving backtest results
- Performance analytics (P&L, Sharpe ratio, etc.)
- Trade log management
"""

from contextlib import asynccontextmanager
from datetime import date
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.constant.schedule import previous_trade_day
from app.db.engine import engine_from_env
from app.db.models import FeedDaily


# Pydantic schemas
class BacktestRequest(BaseModel):
    """Request to run a backtest"""

    strategy_name: str = Field(..., description="Name of the strategy")
    start_date: date = Field(..., description="Backtest start date")
    end_date: date = Field(..., description="Backtest end date")
    initial_capital: float = Field(
        100000.0, description="Initial capital in CNY", ge=0
    )
    filter_id: str = Field("T8", description="Filter ID to use (T2, T3, T6, T7, T8)")


class BacktestResponse(BaseModel):
    """Response from backtest execution"""

    success: bool
    strategy_name: str
    start_date: date
    end_date: date
    total_trades: int
    total_return: float
    message: str


class PerformanceMetrics(BaseModel):
    """Performance metrics for a backtest"""

    total_return: float = Field(..., description="Total return (%)")
    sharpe_ratio: Optional[float] = Field(None, description="Sharpe ratio")
    max_drawdown: Optional[float] = Field(None, description="Maximum drawdown (%)")
    win_rate: Optional[float] = Field(None, description="Win rate (%)")
    total_trades: int = Field(..., description="Total number of trades")
    profitable_trades: int = Field(..., description="Number of profitable trades")


class HealthResponse(BaseModel):
    """Health check response"""

    status: str
    version: str
    database: str


# Database engine
engine = engine_from_env()


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
    return HealthResponse(
        status="healthy", version="1.0.0", database="postgresql"
    )


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint with database connectivity test"""
    try:
        with Session(engine) as session:
            # Test database connectivity
            session.execute("SELECT 1")
        return HealthResponse(
            status="healthy", version="1.0.0", database="connected"
        )
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "version": "1.0.0",
                "database": "disconnected",
                "error": str(e),
            },
        )


@app.post("/backtest", response_model=BacktestResponse)
async def run_backtest(request: BacktestRequest):
    """
    Run a backtest for a specific strategy

    - **strategy_name**: Name of the strategy
    - **start_date**: Backtest start date
    - **end_date**: Backtest end date
    - **initial_capital**: Initial capital in CNY (default: 100,000)
    - **filter_id**: Filter ID to use (T2, T3, T6, T7, T8)

    This is a simplified backtest implementation. The full backtesting
    framework will be implemented in later phases.
    """
    try:
        logger.info(
            f"Running backtest for {request.strategy_name} from {request.start_date} to {request.end_date}"
        )

        # TODO: Implement full backtest logic
        # For now, just count the number of stocks in feed_daily
        with Session(engine) as session:
            trades_count = (
                session.query(FeedDaily)
                .filter(
                    FeedDaily.trade_day >= request.start_date,
                    FeedDaily.trade_day <= request.end_date,
                    FeedDaily.filter_id == request.filter_id,
                )
                .count()
            )

        # Placeholder return calculation
        total_return = 0.0  # TODO: Calculate actual return

        return BacktestResponse(
            success=True,
            strategy_name=request.strategy_name,
            start_date=request.start_date,
            end_date=request.end_date,
            total_trades=trades_count,
            total_return=total_return,
            message=f"Backtest completed for {request.strategy_name}. Found {trades_count} potential trades.",
        )
    except Exception as e:
        logger.error(f"Backtest failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/backtest/{strategy_name}/performance", response_model=PerformanceMetrics)
async def get_performance(
    strategy_name: str,
    start_date: date,
    end_date: date,
    filter_id: str = "T8",
):
    """
    Get performance metrics for a strategy

    - **strategy_name**: Name of the strategy
    - **start_date**: Analysis start date
    - **end_date**: Analysis end date
    - **filter_id**: Filter ID used

    This is a simplified implementation. Full analytics will be
    implemented in later phases.
    """
    try:
        # TODO: Implement full performance analytics
        # For now, return placeholder metrics
        with Session(engine) as session:
            total_trades = (
                session.query(FeedDaily)
                .filter(
                    FeedDaily.trade_day >= start_date,
                    FeedDaily.trade_day <= end_date,
                    FeedDaily.filter_id == filter_id,
                )
                .count()
            )

        return PerformanceMetrics(
            total_return=0.0,  # TODO: Calculate actual metrics
            sharpe_ratio=None,
            max_drawdown=None,
            win_rate=None,
            total_trades=total_trades,
            profitable_trades=0,
        )
    except Exception as e:
        logger.error(f"Failed to get performance metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/feed/{trade_date}")
async def get_feed_for_date(trade_date: date, filter_id: str = "T8"):
    """
    Get filtered stock feed for a specific date (used in backtesting)

    - **trade_date**: Trading date
    - **filter_id**: Filter ID (T2, T3, T6, T7, T8)
    """
    try:
        with Session(engine) as session:
            feed = (
                session.query(FeedDaily)
                .filter(
                    FeedDaily.trade_day == trade_date, FeedDaily.filter_id == filter_id
                )
                .all()
            )

        return {
            "trade_date": trade_date,
            "filter_id": filter_id,
            "count": len(feed),
            "stocks": [item.to_dict() for item in feed],
        }
    except Exception as e:
        logger.error(f"Failed to get feed for {trade_date}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    # Bind to 0.0.0.0 for Docker container accessibility
    uvicorn.run(app, host="0.0.0.0", port=8001)  # nosec B104
