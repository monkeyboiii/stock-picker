"""
Natural Language to Strategy DSL Converter

Converts natural language strategy descriptions into structured
strategy DSL (YAML/JSON format).

Uses pattern matching and keyword extraction to build strategy configurations.
"""

import re
from typing import Any, Dict, List, Optional

from loguru import logger


class NLStrategyConverter:
    """Convert natural language to strategy DSL"""

    def __init__(self):
        """Initialize converter with pattern matchers"""
        self.indicator_patterns = {
            "ma": r"(\d+)[\s-]*(?:day|d)[\s-]*(?:moving average|MA|ma|M\.A\.)",
            "rsi": r"(?:RSI|rsi)(?:[\s<>=]*(\d+))?",
            "macd": r"(?:MACD|macd)",
            "volume": r"(?:volume|trading volume|high volume)",
        }

        self.condition_patterns = {
            "above": r"(?:above|over|greater than|>)",
            "below": r"(?:below|under|less than|<)",
            "cross_above": r"(?:cross above|breaks above|crosses over)",
            "cross_below": r"(?:cross below|breaks below|falls below)",
        }

        self.risk_patterns = {
            "take_profit": r"(?:take profit|tp)[\s:at]*(\d+(?:\.\d+)?)\s*%",
            "stop_loss": r"(?:stop loss|sl)[\s:at]*(\d+(?:\.\d+)?)\s*%",
            "hold_days": r"(?:hold for|holding period)[\s:]*(\d+)\s*days?",
        }

    def convert(self, description: str) -> Dict[str, Any]:
        """
        Convert natural language description to strategy DSL

        Args:
            description: Natural language strategy description

        Returns:
            Strategy configuration dictionary
        """
        logger.info(f"Converting strategy description: {description}")

        # Initialize strategy config
        strategy = {
            "name": "Auto-generated Strategy",
            "description": description,
            "entry_conditions": [],
            "exit_conditions": [],
            "risk_management": {},
            "position_sizing": {
                "max_position_pct": 10.0,
                "max_positions": 10,
            },
        }

        # Extract indicators
        indicators = self._extract_indicators(description)
        if indicators:
            strategy["indicators"] = indicators

        # Extract entry conditions
        entry_conditions = self._extract_entry_conditions(description)
        if entry_conditions:
            strategy["entry_conditions"] = entry_conditions

        # Extract exit conditions (risk management)
        exit_conditions = self._extract_exit_conditions(description)
        if exit_conditions:
            strategy["exit_conditions"] = exit_conditions

        # Extract risk management parameters
        risk_params = self._extract_risk_management(description)
        if risk_params:
            strategy["risk_management"] = risk_params

        # If no specific conditions found, create default strategy
        if not strategy["entry_conditions"]:
            strategy["entry_conditions"] = self._create_default_entry_conditions()

        if not strategy["exit_conditions"]:
            strategy["exit_conditions"] = self._create_default_exit_conditions()

        logger.debug(f"Generated strategy config: {strategy}")
        return strategy

    def _extract_indicators(self, text: str) -> Dict[str, Any]:
        """Extract indicator definitions from text"""
        indicators = {}

        # Check for MA (moving average)
        ma_match = re.search(self.indicator_patterns["ma"], text, re.IGNORECASE)
        if ma_match:
            period = int(ma_match.group(1))
            indicators[f"ma_{period}"] = {
                "type": "ma",
                "period": period,
            }

        # Check for RSI
        rsi_match = re.search(self.indicator_patterns["rsi"], text, re.IGNORECASE)
        if rsi_match:
            threshold = int(rsi_match.group(1)) if rsi_match.group(1) else 30
            indicators["rsi_14"] = {
                "type": "rsi",
                "period": 14,
            }

        # Check for volume
        if re.search(self.indicator_patterns["volume"], text, re.IGNORECASE):
            indicators["volume_avg"] = {
                "type": "sma",
                "field": "volume",
                "period": 20,
            }

        return indicators

    def _extract_entry_conditions(self, text: str) -> List[Dict[str, Any]]:
        """Extract entry conditions from text"""
        conditions = []

        # MA breakout condition
        if re.search(r"(?:break|cross).*(?:above|over).*(?:ma|moving average)", text, re.IGNORECASE):
            # Find MA period
            ma_match = re.search(self.indicator_patterns["ma"], text, re.IGNORECASE)
            if ma_match:
                period = int(ma_match.group(1))
                conditions.append({
                    "type": "technical",
                    "indicator": f"ma_{period}",
                    "operator": "cross_above",
                    "field": "close",
                })

        # Price above MA
        elif re.search(r"(?:price|close).*(?:above|over|>).*(?:ma|moving average)", text, re.IGNORECASE):
            ma_match = re.search(self.indicator_patterns["ma"], text, re.IGNORECASE)
            if ma_match:
                period = int(ma_match.group(1))
                conditions.append({
                    "type": "technical",
                    "indicator": f"ma_{period}",
                    "operator": ">",
                    "field": "close",
                })

        # Volume condition
        if re.search(r"(?:high|strong|above average).*volume", text, re.IGNORECASE):
            conditions.append({
                "type": "technical",
                "indicator": "volume_avg",
                "operator": ">",
                "field": "volume",
                "multiplier": 1.5,
            })

        # RSI oversold condition
        if re.search(r"rsi.*(?:below|<|less than).*(?:30|oversold)", text, re.IGNORECASE):
            conditions.append({
                "type": "technical",
                "indicator": "rsi_14",
                "operator": "<",
                "value": 30,
            })

        # RSI overbought condition
        if re.search(r"rsi.*(?:above|>|greater than).*(?:70|overbought)", text, re.IGNORECASE):
            conditions.append({
                "type": "technical",
                "indicator": "rsi_14",
                "operator": ">",
                "value": 70,
            })

        return conditions

    def _extract_exit_conditions(self, text: str) -> List[Dict[str, Any]]:
        """Extract exit conditions from text"""
        conditions = []

        # Take profit
        tp_match = re.search(self.risk_patterns["take_profit"], text, re.IGNORECASE)
        if tp_match:
            tp_pct = float(tp_match.group(1))
            conditions.append({
                "type": "risk_management",
                "action": "take_profit",
                "threshold_pct": tp_pct,
            })

        # Stop loss
        sl_match = re.search(self.risk_patterns["stop_loss"], text, re.IGNORECASE)
        if sl_match:
            sl_pct = float(sl_match.group(1))
            conditions.append({
                "type": "risk_management",
                "action": "stop_loss",
                "threshold_pct": -sl_pct,  # Negative for loss
            })

        # Time-based exit
        hold_match = re.search(self.risk_patterns["hold_days"], text, re.IGNORECASE)
        if hold_match:
            days = int(hold_match.group(1))
            conditions.append({
                "type": "time_limit",
                "days": days,
            })

        return conditions

    def _extract_risk_management(self, text: str) -> Dict[str, Any]:
        """Extract risk management parameters"""
        risk_params = {}

        # Take profit
        tp_match = re.search(self.risk_patterns["take_profit"], text, re.IGNORECASE)
        if tp_match:
            risk_params["take_profit_pct"] = float(tp_match.group(1))

        # Stop loss
        sl_match = re.search(self.risk_patterns["stop_loss"], text, re.IGNORECASE)
        if sl_match:
            risk_params["stop_loss_pct"] = float(sl_match.group(1))

        # Holding period
        hold_match = re.search(self.risk_patterns["hold_days"], text, re.IGNORECASE)
        if hold_match:
            risk_params["max_holding_days"] = int(hold_match.group(1))

        return risk_params

    def _create_default_entry_conditions(self) -> List[Dict[str, Any]]:
        """Create default entry conditions if none found"""
        return [
            {
                "type": "technical",
                "description": "Default: Price above 250-day MA with volume",
                "indicator": "ma_250",
                "operator": ">",
                "field": "close",
            }
        ]

    def _create_default_exit_conditions(self) -> List[Dict[str, Any]]:
        """Create default exit conditions if none found"""
        return [
            {
                "type": "risk_management",
                "action": "take_profit",
                "threshold_pct": 20.0,
            },
            {
                "type": "risk_management",
                "action": "stop_loss",
                "threshold_pct": -10.0,
            },
            {
                "type": "time_limit",
                "days": 30,
            }
        ]


# Example usage
if __name__ == "__main__":
    converter = NLStrategyConverter()

    # Test conversions
    test_descriptions = [
        "Buy stocks when they break above their 250-day moving average with high volume, hold for 30 days, take profit at 20% or stop loss at 10%",
        "Enter when RSI is below 30 (oversold), exit when RSI is above 70 (overbought)",
        "Simple MA crossover: buy when price crosses above 50-day MA, sell after 20% profit or 5% loss",
    ]

    for desc in test_descriptions:
        print(f"\nDescription: {desc}")
        print(f"Strategy: {converter.convert(desc)}")
