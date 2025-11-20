"""
Trading API - Stock data ingestion, filtering, and analysis

FastAPI service for:
- Stock data ingestion from AKShare
- Technical filtering (tail scraper)
- Stock collection management
- Display formatting (Google Sheets, TDX)
"""

from contextlib import asynccontextmanager
from datetime import date
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.constant.schedule import previous_trade_day
from app.db.engine import engine_from_env
from app.db.models import FeedDaily, Stock, StockDaily


# Pydantic schemas
class IngestRequest(BaseModel):
    """Request to ingest stock data for a specific date"""

    trade_date: Optional[date] = Field(
        None, description="Trading date (defaults to previous trade day)"
    )
    force: bool = Field(False, description="Force re-ingestion even if data exists")


class IngestResponse(BaseModel):
    """Response from data ingestion"""

    success: bool
    trade_date: date
    stocks_updated: int
    message: str


class FilterRequest(BaseModel):
    """Request to filter stocks using tail scraper"""

    trade_date: Optional[date] = Field(
        None, description="Trading date (defaults to previous trade day)"
    )
    filter_id: str = Field("T8", description="Filter ID (T2, T3, T6, T7, T8)")


class FilterResponse(BaseModel):
    """Response from stock filtering"""

    success: bool
    trade_date: date
    filter_id: str
    stocks_found: int
    message: str


class UpdateMetricsRequest(BaseModel):
    """Request to update derived metrics (ma250)"""

    trade_date: Optional[date] = Field(
        None, description="Trading date (defaults to previous trade day)"
    )


class UpdateMetricsResponse(BaseModel):
    """Response from metrics update"""

    success: bool
    trade_date: date
    metrics_updated: int
    message: str


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
    logger.info("Starting Trading API...")
    yield
    logger.info("Shutting down Trading API...")


# FastAPI app
app = FastAPI(
    title="Trading API",
    description="Stock data ingestion, filtering, and analysis service",
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


@app.post("/ingest", response_model=IngestResponse)
async def ingest_stock_data(request: IngestRequest):
    """
    Ingest stock data from AKShare for a specific trade date

    - **trade_date**: Trading date (defaults to previous trade day)
    - **force**: Force re-ingestion even if data exists
    """
    try:
        trade_date = request.trade_date or previous_trade_day()
        logger.info(f"[STUB] Ingesting stock data for {trade_date}...")

        # TODO: Implement actual ingestion logic
        # from app.db.ingest import refresh_stock_daily
        # refresh_stock_daily(engine, today=trade_date)

        return IngestResponse(
            success=True,
            trade_date=trade_date,
            stocks_updated=0,
            message=f"[STUB] Ingestion endpoint ready - implementation pending",
        )
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/update-metrics", response_model=UpdateMetricsResponse)
async def update_metrics(request: UpdateMetricsRequest):
    """
    Update derived metrics (ma250) for all stocks

    - **trade_date**: Trading date (defaults to previous trade day)
    """
    try:
        trade_date = request.trade_date or previous_trade_day()
        logger.info(f"[STUB] Updating metrics for {trade_date}...")

        # TODO: Implement actual metrics calculation
        # from app.utils.update import calculate_ma250
        # calculate_ma250(engine, trade_day=trade_date)

        return UpdateMetricsResponse(
            success=True,
            trade_date=trade_date,
            metrics_updated=0,
            message=f"[STUB] Metrics endpoint ready - implementation pending",
        )
    except Exception as e:
        logger.error(f"Metrics update failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/filter", response_model=FilterResponse)
async def filter_stocks(request: FilterRequest):
    """
    Filter stocks using tail scraper criteria

    - **trade_date**: Trading date (defaults to previous trade day)
    - **filter_id**: Filter ID (T2, T3, T6, T7, T8)
    """
    try:
        trade_date = request.trade_date or previous_trade_day()
        logger.info(f"[STUB] Filtering stocks for {trade_date} with filter {request.filter_id}...")

        # TODO: Implement actual filtering logic
        # from app.filter.tail_scraper import filter_desired
        # filter_desired(engine, trade_day=trade_date)

        return FilterResponse(
            success=True,
            trade_date=trade_date,
            filter_id=request.filter_id,
            stocks_found=0,
            message=f"[STUB] Filter endpoint ready - implementation pending",
        )
    except Exception as e:
        logger.error(f"Filtering failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stocks")
async def list_stocks(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of stocks to return"),
    offset: int = Query(0, ge=0, description="Number of stocks to skip"),
):
    """
    List all stocks in the database

    - **limit**: Maximum number of stocks to return (1-1000)
    - **offset**: Number of stocks to skip
    """
    try:
        with Session(engine) as session:
            stocks = session.query(Stock).offset(offset).limit(limit).all()
            total = session.query(Stock).count()

        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "stocks": [stock.to_dict() for stock in stocks],
        }
    except Exception as e:
        logger.error(f"Failed to list stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stocks/{code}")
async def get_stock(code: str):
    """
    Get detailed information for a specific stock

    - **code**: Stock code (e.g., "600000")
    """
    try:
        with Session(engine) as session:
            stock = session.query(Stock).filter(Stock.code == code).first()
            if not stock:
                raise HTTPException(status_code=404, detail=f"Stock {code} not found")

        return stock.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get stock {code}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stocks/{code}/daily")
async def get_stock_daily(
    code: str,
    start_date: Optional[date] = Query(None, description="Start date"),
    end_date: Optional[date] = Query(None, description="End date"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records"),
):
    """
    Get daily price data for a specific stock

    - **code**: Stock code (e.g., "600000")
    - **start_date**: Start date filter
    - **end_date**: End date filter
    - **limit**: Maximum number of records to return
    """
    try:
        with Session(engine) as session:
            # Check if stock exists
            stock = session.query(Stock).filter(Stock.code == code).first()
            if not stock:
                raise HTTPException(status_code=404, detail=f"Stock {code} not found")

            # Build query
            query = session.query(StockDaily).filter(StockDaily.code == code)

            if start_date:
                query = query.filter(StockDaily.trade_day >= start_date)
            if end_date:
                query = query.filter(StockDaily.trade_day <= end_date)

            # Order by date descending and limit
            daily_data = query.order_by(StockDaily.trade_day.desc()).limit(limit).all()

        return {
            "code": code,
            "name": stock.name,
            "count": len(daily_data),
            "data": [data.to_dict() for data in daily_data],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get daily data for {code}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/feed")
async def get_feed(
    trade_date: Optional[date] = Query(None, description="Trading date"),
    filter_id: str = Query("T8", description="Filter ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records"),
):
    """
    Get filtered stock feed for a specific date

    - **trade_date**: Trading date (defaults to previous trade day)
    - **filter_id**: Filter ID (T2, T3, T6, T7, T8)
    - **limit**: Maximum number of records to return
    """
    try:
        trade_date = trade_date or previous_trade_day()

        with Session(engine) as session:
            feed = (
                session.query(FeedDaily)
                .filter(
                    FeedDaily.trade_day == trade_date, FeedDaily.filter_id == filter_id
                )
                .limit(limit)
                .all()
            )

        return {
            "trade_date": trade_date,
            "filter_id": filter_id,
            "count": len(feed),
            "stocks": [item.to_dict() for item in feed],
        }
    except Exception as e:
        logger.error(f"Failed to get feed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    # Bind to 0.0.0.0 for Docker container accessibility
    uvicorn.run(app, host="0.0.0.0", port=8000)  # nosec B104
