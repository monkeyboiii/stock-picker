"""
Strategy Schema - Pydantic models for strategy validation

This module defines the structure and validation rules for trading strategies.
Strategies can be defined in JSON or YAML format and are validated against these schemas.
"""

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator


class ConditionType(str, Enum):
    """Available condition types for strategy definition"""

    PRICE_CHANGE = "price_change"
    INDICATOR = "indicator"
    TECHNICAL = "technical"
    MARKET_CAP = "market_cap"
    PRICE_DIRECTION = "price_direction"
    RISK_FILTER = "risk_filter"
    VOLUME = "volume"
    TIME_BASED = "time_based"
    TAKE_PROFIT = "take_profit"
    STOP_LOSS = "stop_loss"
    RSI = "rsi"
    MACD = "macd"
    BOLLINGER = "bollinger"


class ComparisonOperator(str, Enum):
    """Available comparison operators"""

    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    EQ = "=="
    NEQ = "!="
    BETWEEN = "between"


class PriceChangeCondition(BaseModel):
    """Price change condition (e.g., gain between 3-5%)"""

    type: Literal[ConditionType.PRICE_CHANGE] = ConditionType.PRICE_CHANGE
    field: str = "close"
    comparison: ComparisonOperator
    value: Union[float, List[float]]
    unit: Literal["percent", "absolute"] = "percent"
    lookback: int = 1


class IndicatorCondition(BaseModel):
    """Generic indicator condition (e.g., quantity_relative_ratio >= 1.0)"""

    type: Literal[ConditionType.INDICATOR] = ConditionType.INDICATOR
    name: str
    comparison: ComparisonOperator
    value: float


class TechnicalCondition(BaseModel):
    """Technical indicator condition (e.g., low > ma(250))"""

    type: Literal[ConditionType.TECHNICAL] = ConditionType.TECHNICAL
    indicator: str  # e.g., "ma", "rsi", "macd"
    period: Optional[int] = None
    comparison: ComparisonOperator
    target: str  # Field to compare against (e.g., "low", "close")


class MarketCapCondition(BaseModel):
    """Market capitalization condition"""

    type: Literal[ConditionType.MARKET_CAP] = ConditionType.MARKET_CAP
    field: str = "circulation_capital"
    comparison: ComparisonOperator
    value: Union[float, List[float]]


class PriceDirectionCondition(BaseModel):
    """Price direction condition (e.g., close > open)"""

    type: Literal[ConditionType.PRICE_DIRECTION] = ConditionType.PRICE_DIRECTION
    comparison: str  # e.g., "close > open"


class RiskFilterCondition(BaseModel):
    """Risk filter condition (e.g., exclude ST stocks)"""

    type: Literal[ConditionType.RISK_FILTER] = ConditionType.RISK_FILTER
    exclude: List[str]  # Patterns to exclude


class VolumeCondition(BaseModel):
    """Volume condition"""

    type: Literal[ConditionType.VOLUME] = ConditionType.VOLUME
    comparison: ComparisonOperator
    value: Union[float, str]  # Can be absolute value or expression like "ma(5, volume)"


class TakeProfitCondition(BaseModel):
    """Take profit exit condition"""

    type: Literal[ConditionType.TAKE_PROFIT] = ConditionType.TAKE_PROFIT
    method: Literal["percentage", "absolute"] = "percentage"
    value: float


class StopLossCondition(BaseModel):
    """Stop loss exit condition"""

    type: Literal[ConditionType.STOP_LOSS] = ConditionType.STOP_LOSS
    method: Literal["percentage", "absolute"] = "percentage"
    value: float


class TimeBasedCondition(BaseModel):
    """Time-based exit condition"""

    type: Literal[ConditionType.TIME_BASED] = ConditionType.TIME_BASED
    max_holding_days: int


class RSICondition(BaseModel):
    """RSI indicator condition"""

    type: Literal[ConditionType.RSI] = ConditionType.RSI
    period: int = 14
    comparison: ComparisonOperator
    value: float


class MACDCondition(BaseModel):
    """MACD indicator condition"""

    type: Literal[ConditionType.MACD] = ConditionType.MACD
    fast: int = 12
    slow: int = 26
    signal: int = 9
    comparison: str  # e.g., "macd_line > signal_line"


class BollingerCondition(BaseModel):
    """Bollinger Bands condition"""

    type: Literal[ConditionType.BOLLINGER] = ConditionType.BOLLINGER
    period: int = 20
    std_dev: float = 2.0
    comparison: str  # e.g., "close < bb_lower"


# Union type for all condition types
StrategyCondition = Union[
    PriceChangeCondition,
    IndicatorCondition,
    TechnicalCondition,
    MarketCapCondition,
    PriceDirectionCondition,
    RiskFilterCondition,
    VolumeCondition,
    TakeProfitCondition,
    StopLossCondition,
    TimeBasedCondition,
    RSICondition,
    MACDCondition,
    BollingerCondition,
]


class ConditionGroup(BaseModel):
    """Group of conditions with logical operator"""

    operator: Literal["AND", "OR", "NOT"] = "AND"
    conditions: List[Union[StrategyCondition, "ConditionGroup"]]

    @field_validator("conditions")
    @classmethod
    def validate_conditions(cls, v):
        if not v:
            raise ValueError("Condition group must have at least one condition")
        return v


class PositionSizing(BaseModel):
    """Position sizing configuration"""

    method: Literal["equal_weight", "risk_parity", "kelly", "fixed"] = "equal_weight"
    max_positions: int = Field(default=20, ge=1, le=100)
    capital_per_position: Optional[str] = "1/max_positions"
    risk_per_trade: Optional[float] = Field(default=None, ge=0.0, le=100.0)


class RiskManagement(BaseModel):
    """Risk management configuration"""

    max_portfolio_drawdown: float = Field(default=20.0, ge=0.0, le=100.0)
    max_position_size: float = Field(default=10.0, ge=0.0, le=100.0)
    stop_trading_on_drawdown: bool = True


class StrategyDefinition(BaseModel):
    """Complete strategy definition"""

    name: str = Field(..., min_length=1, max_length=255)
    version: str = Field(default="1.0.0", pattern=r"^\d+\.\d+\.\d+$")
    author: Optional[str] = None
    description: Optional[str] = None

    parameters: Dict[str, Union[float, int, str]] = Field(default_factory=dict)

    entry_conditions: ConditionGroup
    exit_conditions: ConditionGroup

    position_sizing: PositionSizing = Field(default_factory=PositionSizing)
    risk_management: Optional[RiskManagement] = None

    @field_validator("parameters")
    @classmethod
    def validate_parameters(cls, v):
        """Ensure parameters have valid names (used for substitution)"""
        for key in v.keys():
            if not key.replace("_", "").isalnum():
                raise ValueError(f"Invalid parameter name: {key}")
        return v

    @model_validator(mode="after")
    def validate_strategy(self):
        """Cross-field validation"""
        # Ensure referenced parameters exist
        # This is a placeholder - full implementation in validator.py
        return self


class StrategyWrapper(BaseModel):
    """Wrapper for strategy definition (top-level structure)"""

    strategy: StrategyDefinition


# Allow forward references
ConditionGroup.model_rebuild()
