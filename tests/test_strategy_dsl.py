"""
Tests for Phase 2: Strategy DSL components
"""

import pytest

from app.strategy import (
    ConditionType,
    StrategyDefinition,
    StrategyParseError,
    StrategyValidationError,
    parse_strategy,
    validate_strategy,
)


class TestStrategyParser:
    """Tests for strategy parser"""

    def test_parse_yaml_basic(self):
        """Test parsing basic YAML strategy"""
        yaml_str = """
strategy:
  name: Test Strategy
  version: 1.0.0

  parameters:
    test_param: 10.0

  entry_conditions:
    operator: AND
    conditions:
      - type: indicator
        name: quantity_relative_ratio
        comparison: ">="
        value: 1.0

  exit_conditions:
    operator: OR
    conditions:
      - type: take_profit
        method: percentage
        value: 10.0
"""
        strategy = parse_strategy(yaml_str)

        assert strategy.name == "Test Strategy"
        assert strategy.version == "1.0.0"
        assert "test_param" in strategy.parameters
        assert strategy.parameters["test_param"] == 10.0

    def test_parse_json_basic(self):
        """Test parsing basic JSON strategy"""
        json_str = """
{
  "strategy": {
    "name": "Test JSON Strategy",
    "version": "1.0.0",
    "entry_conditions": {
      "operator": "AND",
      "conditions": [
        {
          "type": "indicator",
          "name": "turnover_rate",
          "comparison": ">",
          "value": 5.0
        }
      ]
    },
    "exit_conditions": {
      "operator": "OR",
      "conditions": [
        {
          "type": "stop_loss",
          "method": "percentage",
          "value": -5.0
        }
      ]
    }
  }
}
"""
        strategy = parse_strategy(json_str)

        assert strategy.name == "Test JSON Strategy"
        assert strategy.version == "1.0.0"

    def test_parameter_substitution(self):
        """Test parameter substitution"""
        yaml_str = """
strategy:
  name: Param Test
  version: 1.0.0

  parameters:
    volume_min: 1.5
    take_profit: 12.0

  entry_conditions:
    operator: AND
    conditions:
      - type: indicator
        name: quantity_relative_ratio
        comparison: ">="
        value: "{{volume_min}}"

  exit_conditions:
    operator: OR
    conditions:
      - type: take_profit
        method: percentage
        value: "{{take_profit}}"
"""
        strategy = parse_strategy(yaml_str)

        # Parameters should be substituted
        entry_cond = strategy.entry_conditions.conditions[0]
        assert entry_cond.value == 1.5

        exit_cond = strategy.exit_conditions.conditions[0]
        assert exit_cond.value == 12.0

    def test_parse_invalid_yaml(self):
        """Test parsing invalid YAML"""
        invalid_yaml = """
strategy:
  name: Invalid
  version: 1.0.0
  entry_conditions:
    - this is invalid
"""
        with pytest.raises(StrategyParseError):
            parse_strategy(invalid_yaml)

    def test_parse_missing_required_field(self):
        """Test parsing with missing required field"""
        yaml_str = """
strategy:
  version: 1.0.0
  entry_conditions:
    operator: AND
    conditions: []
  exit_conditions:
    operator: OR
    conditions: []
"""
        # Missing 'name' field
        with pytest.raises(StrategyParseError):
            parse_strategy(yaml_str)


class TestStrategyValidator:
    """Tests for strategy validator"""

    def test_validate_valid_strategy(self):
        """Test validating a valid strategy"""
        yaml_str = """
strategy:
  name: Valid Strategy
  version: 1.0.0

  entry_conditions:
    operator: AND
    conditions:
      - type: indicator
        name: quantity_relative_ratio
        comparison: ">="
        value: 1.0
      - type: risk_filter
        exclude: ["ST"]

  exit_conditions:
    operator: OR
    conditions:
      - type: take_profit
        method: percentage
        value: 10.0
      - type: stop_loss
        method: percentage
        value: -5.0
"""
        strategy = parse_strategy(yaml_str)
        result = validate_strategy(strategy)
        assert result is True

    def test_validate_unknown_indicator(self):
        """Test validation fails for unknown indicator"""
        yaml_str = """
strategy:
  name: Invalid Indicator
  version: 1.0.0

  entry_conditions:
    operator: AND
    conditions:
      - type: indicator
        name: unknown_indicator_xyz
        comparison: ">="
        value: 1.0

  exit_conditions:
    operator: OR
    conditions:
      - type: take_profit
        method: percentage
        value: 10.0
"""
        strategy = parse_strategy(yaml_str)

        with pytest.raises(StrategyValidationError, match="Unknown indicator"):
            validate_strategy(strategy)

    def test_validate_invalid_take_profit(self):
        """Test validation fails for negative take profit"""
        yaml_str = """
strategy:
  name: Invalid Take Profit
  version: 1.0.0

  entry_conditions:
    operator: AND
    conditions:
      - type: indicator
        name: quantity_relative_ratio
        comparison: ">="
        value: 1.0

  exit_conditions:
    operator: OR
    conditions:
      - type: take_profit
        method: percentage
        value: -10.0
"""
        strategy = parse_strategy(yaml_str)

        with pytest.raises(StrategyValidationError, match="take_profit should be positive"):
            validate_strategy(strategy)

    def test_validate_invalid_stop_loss(self):
        """Test validation fails for positive stop loss"""
        yaml_str = """
strategy:
  name: Invalid Stop Loss
  version: 1.0.0

  entry_conditions:
    operator: AND
    conditions:
      - type: indicator
        name: turnover_rate
        comparison: ">"
        value: 5.0

  exit_conditions:
    operator: OR
    conditions:
      - type: stop_loss
        method: percentage
        value: 5.0
"""
        strategy = parse_strategy(yaml_str)

        with pytest.raises(StrategyValidationError, match="stop_loss should be negative"):
            validate_strategy(strategy)


class TestConditionTypes:
    """Tests for condition types"""

    def test_indicator_condition(self):
        """Test indicator condition"""
        yaml_str = """
strategy:
  name: Indicator Test
  version: 1.0.0

  entry_conditions:
    operator: AND
    conditions:
      - type: indicator
        name: quantity_relative_ratio
        comparison: ">="
        value: 1.0

  exit_conditions:
    operator: OR
    conditions:
      - type: take_profit
        method: percentage
        value: 10.0
"""
        strategy = parse_strategy(yaml_str)
        condition = strategy.entry_conditions.conditions[0]

        assert condition.type == ConditionType.INDICATOR
        assert condition.name == "quantity_relative_ratio"
        assert condition.comparison == ">="
        assert condition.value == 1.0

    def test_risk_filter_condition(self):
        """Test risk filter condition"""
        yaml_str = """
strategy:
  name: Risk Filter Test
  version: 1.0.0

  entry_conditions:
    operator: AND
    conditions:
      - type: risk_filter
        exclude: ["ST", "*", "退"]

  exit_conditions:
    operator: OR
    conditions:
      - type: take_profit
        method: percentage
        value: 10.0
"""
        strategy = parse_strategy(yaml_str)
        condition = strategy.entry_conditions.conditions[0]

        assert condition.type == ConditionType.RISK_FILTER
        assert "ST" in condition.exclude
        assert "*" in condition.exclude
        assert "退" in condition.exclude

    def test_technical_condition(self):
        """Test technical condition"""
        yaml_str = """
strategy:
  name: Technical Test
  version: 1.0.0

  entry_conditions:
    operator: AND
    conditions:
      - type: technical
        indicator: ma
        period: 250
        comparison: ">"
        target: low

  exit_conditions:
    operator: OR
    conditions:
      - type: time_based
        max_holding_days: 30
"""
        strategy = parse_strategy(yaml_str)
        condition = strategy.entry_conditions.conditions[0]

        assert condition.type == ConditionType.TECHNICAL
        assert condition.indicator == "ma"
        assert condition.period == 250
        assert condition.target == "low"


class TestPositionSizing:
    """Tests for position sizing"""

    def test_position_sizing_defaults(self):
        """Test default position sizing"""
        yaml_str = """
strategy:
  name: Position Sizing Test
  version: 1.0.0

  entry_conditions:
    operator: AND
    conditions:
      - type: indicator
        name: turnover_rate
        comparison: ">"
        value: 5.0

  exit_conditions:
    operator: OR
    conditions:
      - type: take_profit
        method: percentage
        value: 10.0
"""
        strategy = parse_strategy(yaml_str)

        assert strategy.position_sizing.method == "equal_weight"
        assert strategy.position_sizing.max_positions == 20

    def test_custom_position_sizing(self):
        """Test custom position sizing"""
        yaml_str = """
strategy:
  name: Custom Position Sizing
  version: 1.0.0

  entry_conditions:
    operator: AND
    conditions:
      - type: indicator
        name: turnover_rate
        comparison: ">"
        value: 5.0

  exit_conditions:
    operator: OR
    conditions:
      - type: take_profit
        method: percentage
        value: 10.0

  position_sizing:
    method: equal_weight
    max_positions: 15
    risk_per_trade: 2.0
"""
        strategy = parse_strategy(yaml_str)

        assert strategy.position_sizing.max_positions == 15
        assert strategy.position_sizing.risk_per_trade == 2.0


class TestRiskManagement:
    """Tests for risk management"""

    def test_risk_management_present(self):
        """Test risk management configuration"""
        yaml_str = """
strategy:
  name: Risk Management Test
  version: 1.0.0

  entry_conditions:
    operator: AND
    conditions:
      - type: indicator
        name: turnover_rate
        comparison: ">"
        value: 5.0

  exit_conditions:
    operator: OR
    conditions:
      - type: take_profit
        method: percentage
        value: 10.0

  risk_management:
    max_portfolio_drawdown: 15.0
    max_position_size: 8.0
    stop_trading_on_drawdown: true
"""
        strategy = parse_strategy(yaml_str)

        assert strategy.risk_management is not None
        assert strategy.risk_management.max_portfolio_drawdown == 15.0
        assert strategy.risk_management.max_position_size == 8.0
        assert strategy.risk_management.stop_trading_on_drawdown is True


class TestStrategyBuilder:
    """Tests for StrategyBuilder integration"""

    def test_load_from_yaml_file(self):
        """Test loading strategy from YAML file"""
        from pathlib import Path

        from app.backtest.signals import StrategyBuilder

        # Check if example strategy file exists
        strategy_path = Path("strategies/tail_scraper.yaml")

        if strategy_path.exists():
            strategy = StrategyBuilder.from_yaml_file(strategy_path)

            assert strategy.name == "Tail Scraper"
            assert strategy.version == "2.0.0"
            assert "volume_ratio_min" in strategy.parameters
        else:
            pytest.skip("Strategy file not found")
