"""
Signal Engine - Entry/Exit Condition Evaluation

This module handles:
- Scanning for entry signals (stocks matching entry conditions)
- Evaluating exit conditions for open positions
- Interfacing with existing filter logic (tail_scraper)
"""

from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from loguru import logger
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.db.models import Stock, StockDaily
from app.filter.tail_scraper import build_stmt_postgresql_lateral


class SignalEngine:
    """
    Evaluates entry and exit signals for backtesting.

    For now, uses the existing tail_scraper logic for entry signals.
    Future versions will support custom strategy definitions.
    """

    def __init__(self, strategy_definition: Optional[Dict] = None):
        """
        Initialize SignalEngine

        Args:
            strategy_definition: Optional strategy definition dict
                                 (for future use with custom strategies)
        """
        self.strategy_definition = strategy_definition or {}

        # Extract exit parameters from strategy definition
        self.take_profit_pct = self.strategy_definition.get("take_profit_pct", Decimal("10.0"))
        self.stop_loss_pct = self.strategy_definition.get("stop_loss_pct", Decimal("-5.0"))
        self.max_holding_days = self.strategy_definition.get("max_holding_days", 30)

    def scan_entry_signals(
        self,
        trade_day: date,
        engine: Engine,
    ) -> List[Dict[str, Any]]:
        """
        Scan for stocks matching entry conditions

        Args:
            trade_day: Trade date to scan
            engine: Database engine

        Returns:
            List of candidate stocks with metadata
        """
        # Use existing tail_scraper logic for now
        stmt = build_stmt_postgresql_lateral(trade_day)

        with Session(engine) as session:
            result = session.execute(stmt).fetchall()

            candidates = []
            for row in result:
                candidate = {
                    "code": row.code,
                    "name": row.name if hasattr(row, "name") else "",
                    "price": row.close if hasattr(row, "close") else Decimal(0),
                    "signal": {
                        "type": "tail_scraper",
                        "trade_day": trade_day.isoformat(),
                        "conditions": "T2-T8 filters applied",
                    },
                    "collection_name": row.collection_name if hasattr(row, "collection_name") else None,
                }
                candidates.append(candidate)

            logger.debug(f"Found {len(candidates)} entry signals for {trade_day}")
            return candidates

    def check_exit(
        self,
        position: Any,  # Position object from engine.py
        current_price: Decimal,
        current_date: date,
        engine: Engine,
    ) -> Optional[str]:
        """
        Check if position should be exited

        Args:
            position: Position object
            current_price: Current stock price
            current_date: Current date
            engine: Database engine

        Returns:
            Exit reason string if exit triggered, None otherwise
            Possible values: 'take_profit', 'stop_loss', 'time_limit', 'backtest_end'
        """

        # Calculate current return
        pnl_pct = ((current_price - position.entry_price) / position.entry_price) * 100

        # Check take profit
        if pnl_pct >= self.take_profit_pct:
            logger.debug(
                f"{position.stock_code}: Take profit triggered "
                f"({pnl_pct:.2f}% >= {self.take_profit_pct}%)"
            )
            return "take_profit"

        # Check stop loss
        if pnl_pct <= self.stop_loss_pct:
            logger.debug(
                f"{position.stock_code}: Stop loss triggered "
                f"({pnl_pct:.2f}% <= {self.stop_loss_pct}%)"
            )
            return "stop_loss"

        # Check time limit
        holding_days = (current_date - position.entry_date).days
        if holding_days >= self.max_holding_days:
            logger.debug(
                f"{position.stock_code}: Time limit triggered "
                f"({holding_days} >= {self.max_holding_days} days)"
            )
            return "time_limit"

        # No exit condition met
        return None

    def get_current_price(
        self,
        stock_code: str,
        trade_day: date,
        engine: Engine,
    ) -> Optional[Decimal]:
        """
        Get stock price for a given day

        Args:
            stock_code: Stock code
            trade_day: Trade date
            engine: Database engine

        Returns:
            Close price or None if not found
        """
        with Session(engine) as session:
            stmt = select(StockDaily.close).where(
                StockDaily.code == stock_code,
                StockDaily.trade_day == trade_day,
            )
            result = session.execute(stmt).scalar_one_or_none()
            return result

    def get_current_prices_batch(
        self,
        stock_codes: List[str],
        trade_day: date,
        engine: Engine,
    ) -> Dict[str, Decimal]:
        """
        Get prices for multiple stocks in one query

        Args:
            stock_codes: List of stock codes
            trade_day: Trade date
            engine: Database engine

        Returns:
            Dict mapping stock_code -> price
        """
        if not stock_codes:
            return {}

        with Session(engine) as session:
            stmt = select(
                StockDaily.code,
                StockDaily.close,
            ).where(
                StockDaily.code.in_(stock_codes),
                StockDaily.trade_day == trade_day,
            )
            results = session.execute(stmt).fetchall()

            return {row.code: row.close for row in results}


class StrategyBuilder:
    """
    Helper class to build strategy definitions programmatically.

    This will be expanded to support the full strategy DSL from the design doc.
    """

    @staticmethod
    def tail_scraper_strategy(
        take_profit_pct: float = 10.0,
        stop_loss_pct: float = -5.0,
        max_holding_days: int = 30,
    ) -> Dict:
        """
        Build the default tail_scraper strategy definition

        Args:
            take_profit_pct: Take profit percentage (positive)
            stop_loss_pct: Stop loss percentage (negative)
            max_holding_days: Maximum holding period

        Returns:
            Strategy definition dict
        """
        return {
            "name": "Tail Scraper",
            "version": "1.0.0",
            "description": "Original tail scraper strategy with T2-T8 filters",
            "take_profit_pct": take_profit_pct,
            "stop_loss_pct": stop_loss_pct,
            "max_holding_days": max_holding_days,
            "entry_conditions": {
                "type": "tail_scraper",
                "filters": ["T2", "T3", "T4", "T6", "T7", "T8"],
            },
            "exit_conditions": {
                "take_profit": take_profit_pct,
                "stop_loss": stop_loss_pct,
                "time_limit": max_holding_days,
            },
        }
