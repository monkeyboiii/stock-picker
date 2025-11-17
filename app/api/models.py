"""
API Models - Pydantic schemas for API requests and responses

These models define the structure of data exchanged through the REST API.
They are separate from database models to allow for flexible API versioning.
"""

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


# Request models

class BacktestRunRequest(BaseModel):
    """Request model for creating a new backtest run"""

    strategy_id: Optional[str] = Field(None, description="UUID of existing strategy")
    strategy_file_content: Optional[str] = Field(None, description="YAML/JSON strategy content")

    start_date: date = Field(..., description="Backtest start date")
    end_date: date = Field(..., description="Backtest end date")

    initial_capital: float = Field(1000000.0, ge=0, description="Initial capital in CNY")
    commission_rate: float = Field(0.0003, ge=0, le=1, description="Commission rate (default 0.03%)")
    slippage_rate: float = Field(0.001, ge=0, le=1, description="Slippage rate (default 0.1%)")
    max_positions: int = Field(20, ge=1, le=100, description="Maximum concurrent positions")

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
    page_size: int = Field(20, ge=1, le=100, description="Items per page")

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
