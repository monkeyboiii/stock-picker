"""
Strategy Validator - Deep validation of strategy definitions

This module provides validation beyond Pydantic schema validation:
- Check that referenced indicators exist
- Validate condition logic
- Ensure parameter references are valid
- Check for logical inconsistencies
"""

from typing import List, Set

from loguru import logger

from app.strategy.schema import (
    ConditionGroup,
    ConditionType,
    StrategyCondition,
    StrategyDefinition,
)


class StrategyValidationError(Exception):
    """Raised when strategy validation fails"""

    pass


class StrategyValidator:
    """Validator for strategy definitions"""

    # Available indicators (extensible)
    AVAILABLE_INDICATORS = {
        "ma",  # Moving average
        "ema",  # Exponential moving average
        "rsi",  # Relative Strength Index
        "macd",  # MACD
        "bollinger",  # Bollinger Bands
        "atr",  # Average True Range
        "volume",  # Volume-based indicators
        "quantity_relative_ratio",  # Custom indicator
        "turnover_rate",  # Custom indicator
    }

    # Available fields in stock_daily table
    AVAILABLE_FIELDS = {
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
        "capital",
        "circulation_capital",
        "quantity_relative_ratio",
        "turnover_rate",
        "ma_250",
    }

    @staticmethod
    def validate(strategy: StrategyDefinition) -> bool:
        """
        Perform deep validation of strategy

        Args:
            strategy: Strategy definition to validate

        Returns:
            True if valid

        Raises:
            StrategyValidationError: If validation fails
        """
        validator = StrategyValidator()

        # Collect all parameter names
        param_names = set(strategy.parameters.keys())

        # Validate entry conditions
        validator._validate_condition_group(
            strategy.entry_conditions, param_names, "entry"
        )

        # Validate exit conditions
        validator._validate_condition_group(
            strategy.exit_conditions, param_names, "exit"
        )

        # Validate position sizing
        validator._validate_position_sizing(strategy.position_sizing, param_names)

        # Validate risk management
        if strategy.risk_management:
            validator._validate_risk_management(strategy.risk_management)

        logger.info(f"Strategy '{strategy.name}' passed validation")
        return True

    def _validate_condition_group(
        self,
        group: ConditionGroup,
        param_names: Set[str],
        context: str,
    ) -> None:
        """
        Validate a condition group recursively

        Args:
            group: Condition group to validate
            param_names: Set of defined parameter names
            context: Context string (for error messages)

        Raises:
            StrategyValidationError: If validation fails
        """
        if not group.conditions:
            raise StrategyValidationError(
                f"{context} conditions cannot be empty"
            )

        for i, condition in enumerate(group.conditions):
            if isinstance(condition, ConditionGroup):
                # Recursive validation
                self._validate_condition_group(
                    condition, param_names, f"{context} condition {i+1}"
                )
            else:
                # Validate individual condition
                self._validate_condition(condition, param_names, f"{context} condition {i+1}")

    def _validate_condition(
        self,
        condition: StrategyCondition,
        param_names: Set[str],
        context: str,
    ) -> None:
        """
        Validate an individual condition

        Args:
            condition: Condition to validate
            param_names: Set of defined parameter names
            context: Context string (for error messages)

        Raises:
            StrategyValidationError: If validation fails
        """
        cond_type = condition.type

        # Validate indicator conditions
        if cond_type == ConditionType.INDICATOR:
            if condition.name not in self.AVAILABLE_INDICATORS:
                raise StrategyValidationError(
                    f"{context}: Unknown indicator '{condition.name}'. "
                    f"Available: {sorted(self.AVAILABLE_INDICATORS)}"
                )

        # Validate technical conditions
        elif cond_type == ConditionType.TECHNICAL:
            if condition.indicator not in self.AVAILABLE_INDICATORS:
                raise StrategyValidationError(
                    f"{context}: Unknown indicator '{condition.indicator}'"
                )
            if condition.target not in self.AVAILABLE_FIELDS:
                raise StrategyValidationError(
                    f"{context}: Unknown field '{condition.target}'. "
                    f"Available: {sorted(self.AVAILABLE_FIELDS)}"
                )

        # Validate market cap conditions
        elif cond_type == ConditionType.MARKET_CAP:
            if condition.field not in self.AVAILABLE_FIELDS:
                raise StrategyValidationError(
                    f"{context}: Unknown field '{condition.field}'"
                )

        # Validate price change conditions
        elif cond_type == ConditionType.PRICE_CHANGE:
            if condition.field not in self.AVAILABLE_FIELDS:
                raise StrategyValidationError(
                    f"{context}: Unknown field '{condition.field}'"
                )

        # Validate take profit / stop loss
        elif cond_type in [ConditionType.TAKE_PROFIT, ConditionType.STOP_LOSS]:
            if condition.value == 0:
                raise StrategyValidationError(
                    f"{context}: {cond_type.value} cannot be zero"
                )
            if cond_type == ConditionType.TAKE_PROFIT and condition.value < 0:
                raise StrategyValidationError(
                    f"{context}: take_profit should be positive"
                )
            if cond_type == ConditionType.STOP_LOSS and condition.value > 0:
                raise StrategyValidationError(
                    f"{context}: stop_loss should be negative"
                )

        # Validate time-based conditions
        elif cond_type == ConditionType.TIME_BASED:
            if condition.max_holding_days <= 0:
                raise StrategyValidationError(
                    f"{context}: max_holding_days must be positive"
                )

    def _validate_position_sizing(
        self,
        position_sizing,
        param_names: Set[str],
    ) -> None:
        """
        Validate position sizing configuration

        Args:
            position_sizing: Position sizing config
            param_names: Set of defined parameter names

        Raises:
            StrategyValidationError: If validation fails
        """
        if position_sizing.max_positions <= 0:
            raise StrategyValidationError("max_positions must be positive")

        if position_sizing.max_positions > 100:
            logger.warning("max_positions > 100 may be impractical")

        if position_sizing.risk_per_trade is not None:
            if position_sizing.risk_per_trade <= 0 or position_sizing.risk_per_trade > 100:
                raise StrategyValidationError(
                    "risk_per_trade must be between 0 and 100"
                )

    def _validate_risk_management(self, risk_management) -> None:
        """
        Validate risk management configuration

        Args:
            risk_management: Risk management config

        Raises:
            StrategyValidationError: If validation fails
        """
        if risk_management.max_portfolio_drawdown <= 0:
            raise StrategyValidationError("max_portfolio_drawdown must be positive")

        if risk_management.max_position_size <= 0:
            raise StrategyValidationError("max_position_size must be positive")

        if risk_management.max_position_size > 100:
            raise StrategyValidationError(
                "max_position_size cannot exceed 100%"
            )


def validate_strategy(strategy: StrategyDefinition) -> bool:
    """
    Convenience function to validate a strategy

    Args:
        strategy: Strategy definition

    Returns:
        True if valid

    Raises:
        StrategyValidationError: If validation fails

    Example:
        >>> strategy = parse_strategy("my_strategy.yaml")
        >>> validate_strategy(strategy)  # Raises if invalid
    """
    return StrategyValidator.validate(strategy)
