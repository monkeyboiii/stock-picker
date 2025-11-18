"""
API Models - Pydantic schemas for API requests and responses

These models define the structure of data exchanged through the REST API.
They are separate from database models to allow for flexible API versioning.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

from app.constant.trading import (
    DEFAULT_COMMISSION_RATE,
    DEFAULT_INITIAL_CAPITAL,
    DEFAULT_MAX_POSITIONS,
    DEFAULT_PAGE_SIZE,
    DEFAULT_SLIPPAGE_RATE,
    MAX_BACKTEST_YEARS,
    MAX_COMMISSION_RATE,
    MAX_INITIAL_CAPITAL,
    MAX_MAX_POSITIONS,
    MAX_PAGE_SIZE,
    MAX_SLIPPAGE_RATE,
    MIN_PAGE_SIZE,
)


# Request models

class BacktestRunRequest(BaseModel):
    """Request model for creating a new backtest run with comprehensive validation"""

    strategy_id: Optional[UUID] = Field(None, description="UUID of existing strategy")
    strategy_file_content: Optional[str] = Field(None, description="YAML/JSON strategy content")

    start_date: date = Field(..., description="Backtest start date")
    end_date: date = Field(..., description="Backtest end date")

    initial_capital: Decimal = Field(
        DEFAULT_INITIAL_CAPITAL,
        gt=0,
        le=MAX_INITIAL_CAPITAL,
        description="Initial capital in CNY (must be positive)"
    )
    commission_rate: Decimal = Field(
        DEFAULT_COMMISSION_RATE,
        ge=0,
        le=MAX_COMMISSION_RATE,
        description=f"Commission rate (default {float(DEFAULT_COMMISSION_RATE) * 100}%)"
    )
    slippage_rate: Decimal = Field(
        DEFAULT_SLIPPAGE_RATE,
        ge=0,
        le=MAX_SLIPPAGE_RATE,
        description=f"Slippage rate (default {float(DEFAULT_SLIPPAGE_RATE) * 100}%)"
    )
    max_positions: int = Field(
        DEFAULT_MAX_POSITIONS,
        ge=1,
        le=MAX_MAX_POSITIONS,
        description="Maximum concurrent positions"
    )

    @field_validator('start_date')
    @classmethod
    def validate_start_date(cls, v: date) -> date:
        """Validate start_date is not in the future"""
        if v > date.today():
            raise ValueError("start_date cannot be in the future")
        if v.year < 2000:
            raise ValueError("start_date must be after year 2000")
        return v

    @model_validator(mode='after')
    def validate_date_range(self):
        """Validate end_date is after start_date"""
        if self.end_date <= self.start_date:
            raise ValueError(
                f"end_date ({self.end_date}) must be after start_date ({self.start_date})"
            )

        # Check date range is not too long (max years from constants)
        days_diff = (self.end_date - self.start_date).days
        max_days = MAX_BACKTEST_YEARS * 365
        if days_diff > max_days:
            raise ValueError(
                f"Date range too long ({days_diff} days). Maximum is {MAX_BACKTEST_YEARS} years ({max_days} days)"
            )

        return self

    @model_validator(mode='after')
    def validate_strategy_source(self):
        """Validate either strategy_id or strategy_file_content is provided"""
        if not self.strategy_id and not self.strategy_file_content:
            raise ValueError(
                "Either strategy_id or strategy_file_content must be provided"
            )
        if self.strategy_id and self.strategy_file_content:
            raise ValueError(
                "Provide either strategy_id or strategy_file_content, not both"
            )
        return self

    class Config:
        json_schema_extra = {
            "example": {
                "strategy_id": "123e4567-e89b-12d3-a456-426614174000",
                "start_date": "2025-01-01",
                "end_date": "2025-03-31",
                "initial_capital": 1000000.0,
                "max_positions": 20
            }
        }


class StrategyCreateRequest(BaseModel):
    """Request model for creating a new strategy"""

    name: str = Field(..., min_length=1, max_length=255)
    version: str = Field(..., pattern=r"^\d+\.\d+\.\d+$")
    author: Optional[str] = None
    description: Optional[str] = None
    definition: dict = Field(..., description="Strategy definition (YAML/JSON as dict)")

    class Config:
        json_schema_extra = {
            "example": {
                "name": "My Strategy",
                "version": "1.0.0",
                "author": "trader@example.com",
                "description": "Custom momentum strategy",
                "definition": {
                    "parameters": {"take_profit": 10.0},
                    "entry_conditions": {},
                    "exit_conditions": {}
                }
            }
        }


class PaginationParams(BaseModel):
    """Pagination parameters"""

    page: int = Field(1, ge=1, description="Page number (1-indexed)")
    page_size: int = Field(DEFAULT_PAGE_SIZE, ge=MIN_PAGE_SIZE, le=MAX_PAGE_SIZE, description="Items per page")

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size

    @property
    def limit(self) -> int:
        return self.page_size


# Response models

class BacktestRunSummary(BaseModel):
    """Summary of a backtest run (for list views)"""

    id: str
    strategy_id: str
    start_date: date
    end_date: date
    initial_capital: float

    # Results
    total_return: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    win_rate: Optional[float] = None
    total_trades: Optional[int] = None

    # Status
    status: str
    created_at: datetime
    completed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class BacktestRunDetail(BacktestRunSummary):
    """Detailed backtest run information"""

    commission_rate: float
    slippage_rate: float
    profit_factor: Optional[float] = None
    error_message: Optional[str] = None

    class Config:
        from_attributes = True


class TradeResponse(BaseModel):
    """Trade information"""

    id: int
    backtest_run_id: str
    stock_code: str
    stock_name: Optional[str]

    entry_date: date
    entry_price: float
    exit_date: Optional[date]
    exit_price: Optional[float]
    exit_reason: Optional[str]

    shares: int
    position_value: Optional[float]
    gross_pnl: Optional[float]
    commission: Optional[float]
    slippage: Optional[float]
    net_pnl: Optional[float]
    return_pct: Optional[float]

    holding_days: Optional[int]
    collection_name: Optional[str]

    class Config:
        from_attributes = True


class PortfolioSnapshotResponse(BaseModel):
    """Portfolio snapshot information"""

    id: int
    backtest_run_id: str
    snapshot_date: date

    cash: float
    holdings_value: float
    total_value: float

    daily_return: Optional[float]
    cumulative_return: Optional[float]
    drawdown: Optional[float]

    open_positions: Optional[int]
    total_positions_closed: Optional[int]

    class Config:
        from_attributes = True


class StrategyResponse(BaseModel):
    """Strategy information"""

    id: str
    name: str
    version: str
    author: Optional[str]
    description: Optional[str]

    created_at: datetime
    updated_at: datetime
    is_active: bool

    # Don't include full definition in list views
    definition: Optional[dict] = None

    class Config:
        from_attributes = True


class StrategySummary(BaseModel):
    """Strategy summary (for list views)"""

    id: str
    name: str
    version: str
    author: Optional[str]
    description: Optional[str]
    created_at: datetime
    is_active: bool

    class Config:
        from_attributes = True


class PaginatedResponse(BaseModel):
    """Generic paginated response wrapper"""

    items: List
    total: int
    page: int
    page_size: int
    total_pages: int


class BacktestRunsResponse(BaseModel):
    """Paginated list of backtest runs"""

    items: List[BacktestRunSummary]
    total: int
    page: int
    page_size: int
    total_pages: int


class StrategiesResponse(BaseModel):
    """Paginated list of strategies"""

    items: List[StrategySummary]
    total: int
    page: int
    page_size: int
    total_pages: int


class TradesResponse(BaseModel):
    """Paginated list of trades"""

    items: List[TradeResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class ErrorResponse(BaseModel):
    """Error response"""

    error: str
    detail: Optional[str] = None
    code: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response"""

    status: str
    version: str
    database: str
