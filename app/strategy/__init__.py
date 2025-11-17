"""
Strategy Package - Strategy DSL, parsing, validation, and SQL generation

This package provides a comprehensive strategy definition language for backtesting.

Main components:
- schema: Pydantic models for strategy structure
- parser: YAML/JSON parsing and parameter substitution
- validator: Deep validation of strategy definitions
- query_builder: SQL query generation from strategy conditions

Usage:
    >>> from app.strategy import parse_strategy, validate_strategy, build_query_from_strategy
    >>> strategy = parse_strategy("strategies/my_strategy.yaml")
    >>> validate_strategy(strategy)
    >>> query = build_query_from_strategy(strategy, date(2025, 3, 10))
"""

from app.strategy.parser import StrategyParser, StrategyParseError, parse_strategy
from app.strategy.query_builder import QueryBuilder, build_query_from_strategy
from app.strategy.schema import (
    ConditionGroup,
    ConditionType,
    PositionSizing,
    RiskManagement,
    StrategyDefinition,
    StrategyWrapper,
)
from app.strategy.validator import (
    StrategyValidationError,
    StrategyValidator,
    validate_strategy,
)

__all__ = [
    # Parser
    "StrategyParser",
    "StrategyParseError",
    "parse_strategy",
    # Validator
    "StrategyValidator",
    "StrategyValidationError",
    "validate_strategy",
    # Query Builder
    "QueryBuilder",
    "build_query_from_strategy",
    # Schema
    "StrategyDefinition",
    "StrategyWrapper",
    "ConditionGroup",
    "ConditionType",
    "PositionSizing",
    "RiskManagement",
]
