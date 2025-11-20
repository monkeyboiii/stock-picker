"""
Query Builder - Convert strategy conditions to SQL queries

This module generates PostgreSQL queries from strategy definitions,
allowing dynamic filtering based on user-defined conditions.
"""

from datetime import date
from typing import Any, List

from loguru import logger
from sqlalchemy import Double, and_, func, or_, select, true
from sqlalchemy.sql import Select

from app.db.models import Collection, CollectionDaily, RelationCollectionStock, Stock, StockDaily
from app.strategy.schema import (
    ConditionGroup,
    ConditionType,
    MarketCapCondition,
    PriceChangeCondition,
    PriceDirectionCondition,
    RiskFilterCondition,
    StrategyCondition,
    StrategyDefinition,
    TechnicalCondition,
)


class QueryBuilder:
    """Builds SQL queries from strategy conditions"""

    def __init__(self, trade_day: date):
        """
        Initialize query builder

        Args:
            trade_day: Trade date to query for
        """
        self.trade_day = trade_day

        # Table aliases
        self.sd = StockDaily.__table__.alias("sd")
        self.s = Stock.__table__.alias("s")
        self.rcs = RelationCollectionStock.__table__.alias("rcs")
        self.c = Collection.__table__.alias("c")
        self.cd = CollectionDaily.__table__.alias("cd")

    def build_entry_query(self, strategy: StrategyDefinition) -> Select:
        """
        Build SQL query for entry conditions

        Args:
            strategy: Strategy definition

        Returns:
            SQLAlchemy Select statement
        """
        logger.debug(f"Building entry query for strategy: {strategy.name}")

        # Start with base query
        base_query = (
            select(
                self.sd.c.trade_day,
                self.s.c.code,
                self.s.c.name,
                self.sd.c.close,
                self.sd.c.volume,
                self.sd.c.ma_250,
            )
            .select_from(self.s)
            .join(self.sd, self.s.c.code == self.sd.c.code)
            .where(self.sd.c.trade_day == self.trade_day)
        )

        # Build WHERE clause from entry conditions
        where_clause = self._build_condition_group(strategy.entry_conditions)

        # Apply WHERE clause
        if where_clause is not None:
            base_query = base_query.where(where_clause)

        logger.debug("Entry query built successfully")
        return base_query

    def _build_condition_group(self, group: ConditionGroup) -> Any:
        """
        Build SQL expression from condition group

        Args:
            group: Condition group

        Returns:
            SQLAlchemy expression
        """
        if not group.conditions:
            return true()

        # Build expressions for each condition
        expressions = []
        for condition in group.conditions:
            if isinstance(condition, ConditionGroup):
                # Recursive handling of nested groups
                expr = self._build_condition_group(condition)
            else:
                # Build expression for individual condition
                expr = self._build_condition(condition)

            if expr is not None:
                expressions.append(expr)

        if not expressions:
            return true()

        # Combine with operator
        if group.operator == "AND":
            return and_(*expressions)
        elif group.operator == "OR":
            return or_(*expressions)
        elif group.operator == "NOT":
            # NOT applies to all conditions
            return ~and_(*expressions)
        else:
            raise ValueError(f"Unknown operator: {group.operator}")

    def _build_condition(self, condition: StrategyCondition) -> Any:
        """
        Build SQL expression from individual condition

        Args:
            condition: Strategy condition

        Returns:
            SQLAlchemy expression or None if not applicable
        """
        cond_type = condition.type

        if cond_type == ConditionType.INDICATOR:
            # Simple indicator comparison (e.g., quantity_relative_ratio >= 1.0)
            field = getattr(self.sd.c, condition.name, None)
            if field is None:
                logger.warning(f"Field '{condition.name}' not found in stock_daily")
                return None

            return self._apply_comparison(field, condition.comparison, condition.value)

        elif cond_type == ConditionType.TECHNICAL:
            # Technical indicator (e.g., low > ma_250)
            target_field = getattr(self.sd.c, condition.target, None)
            if target_field is None:
                logger.warning(f"Field '{condition.target}' not found")
                return None

            # For now, only support ma_250 (can be extended)
            if condition.indicator == "ma" and condition.period == 250:
                indicator_field = self.sd.c.ma_250
                return self._apply_comparison(target_field, condition.comparison, indicator_field)
            else:
                logger.warning(
                    f"Indicator '{condition.indicator}' not yet implemented"
                )
                return None

        elif cond_type == ConditionType.MARKET_CAP:
            # Market cap condition
            field = getattr(self.sd.c, condition.field, None)
            if field is None:
                logger.warning(f"Field '{condition.field}' not found")
                return None

            return self._apply_comparison(field, condition.comparison, condition.value)

        elif cond_type == ConditionType.PRICE_DIRECTION:
            # Price direction (e.g., "close > open")
            if "close > open" in condition.comparison or "close>open" in condition.comparison:
                return self.sd.c.close > self.sd.c.open
            elif "close < open" in condition.comparison or "close<open" in condition.comparison:
                return self.sd.c.close < self.sd.c.open
            else:
                logger.warning(f"Unknown price direction: {condition.comparison}")
                return None

        elif cond_type == ConditionType.RISK_FILTER:
            # Risk filters (exclude ST, *, etc.)
            expressions = []
            for pattern in condition.exclude:
                if pattern == "ST":
                    expressions.append(~self.s.c.name.ilike("ST%"))
                elif pattern == "*":
                    expressions.append(~self.s.c.name.like("%*%"))
                else:
                    expressions.append(~self.s.c.name.like(f"%{pattern}%"))

            return and_(*expressions) if expressions else None

        elif cond_type == ConditionType.PRICE_CHANGE:
            # Price change condition (not used for entry, but included for completeness)
            logger.debug("Price change conditions require historical data lookup")
            return None

        else:
            logger.warning(f"Condition type '{cond_type}' not implemented in query builder")
            return None

    def _apply_comparison(self, field: Any, operator: str, value: Any) -> Any:
        """
        Apply comparison operator to field and value

        Args:
            field: SQLAlchemy column
            operator: Comparison operator (>, >=, <, <=, ==, !=, between)
            value: Value to compare against (can be scalar or list for 'between')

        Returns:
            SQLAlchemy expression
        """
        # Handle SQLAlchemy columns being passed as value (for field-to-field comparison)
        if hasattr(value, "key"):  # It's a SQLAlchemy column
            if operator in [">", "gt"]:
                return field > value
            elif operator in [">=", "gte"]:
                return field >= value
            elif operator in ["<", "lt"]:
                return field < value
            elif operator in ["<=", "lte"]:
                return field <= value
            elif operator in ["==", "eq"]:
                return field == value
            elif operator in ["!=", "neq"]:
                return field != value
            else:
                raise ValueError(f"Unknown operator: {operator}")

        # Handle scalar/list values
        if operator in [">", "gt"]:
            return field > value
        elif operator in [">=", "gte"]:
            return field >= value
        elif operator in ["<", "lt"]:
            return field < value
        elif operator in ["<=", "lte"]:
            return field <= value
        elif operator in ["==", "eq"]:
            return field == value
        elif operator in ["!=", "neq"]:
            return field != value
        elif operator == "between":
            if not isinstance(value, (list, tuple)) or len(value) != 2:
                raise ValueError("'between' operator requires a list of 2 values")
            return field.between(value[0], value[1])
        else:
            raise ValueError(f"Unknown operator: {operator}")


def build_query_from_strategy(
    strategy: StrategyDefinition,
    trade_day: date,
) -> Select:
    """
    Build SQL query from strategy definition

    Args:
        strategy: Strategy definition
        trade_day: Trade date to query

    Returns:
        SQLAlchemy Select statement

    Example:
        >>> strategy = parse_strategy("my_strategy.yaml")
        >>> query = build_query_from_strategy(strategy, date(2025, 3, 10))
        >>> with Session(engine) as session:
        ...     results = session.execute(query).fetchall()
    """
    builder = QueryBuilder(trade_day)
    return builder.build_entry_query(strategy)
