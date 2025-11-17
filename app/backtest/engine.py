"""
Backtesting Engine - Core Components

This module contains the core classes for backtesting:
- Position: Represents an open stock position
- Portfolio: Manages portfolio state during backtest
- BacktestEngine: Main backtest execution engine
"""

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

import numpy as np
from loguru import logger
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.backtest.analytics import PerformanceAnalyzer, PerformanceMetrics
from app.backtest.signals import SignalEngine
from app.constant.schedule import is_stock_market_open, next_trade_day
from app.db.models import BacktestRun, PortfolioSnapshot, Strategy, StockDaily, Trade


@dataclass
class Position:
    """
    Represents an open stock position in the backtest.

    Tracks entry details and calculates P&L with realistic costs
    (commission and slippage).
    """

    stock_code: str
    stock_name: str
    entry_date: date
    entry_price: Decimal
    shares: int
    entry_signal: Dict[str, Any] = field(default_factory=dict)

    @property
    def position_value(self) -> Decimal:
        """Total value of the position at entry"""
        return self.entry_price * self.shares

    def calculate_pnl(
        self,
        current_price: Decimal,
        commission_rate: Decimal = Decimal("0.0003"),
        slippage_rate: Decimal = Decimal("0.001"),
    ) -> Dict[str, Decimal]:
        """
        Calculate P&L with costs

        Args:
            current_price: Current stock price
            commission_rate: Commission as decimal (e.g., 0.0003 = 0.03%)
            slippage_rate: Slippage as decimal (e.g., 0.001 = 0.1%)

        Returns:
            Dict with gross_pnl, commission, slippage, net_pnl, return_pct
        """
        # Gross P&L
        gross_pnl = (current_price - self.entry_price) * self.shares

        # Entry costs
        entry_commission = self.entry_price * self.shares * commission_rate
        entry_slippage = self.entry_price * self.shares * slippage_rate

        # Exit costs
        exit_commission = current_price * self.shares * commission_rate
        exit_slippage = current_price * self.shares * slippage_rate

        # Total costs
        total_commission = entry_commission + exit_commission
        total_slippage = entry_slippage + exit_slippage

        # Net P&L
        net_pnl = gross_pnl - total_commission - total_slippage

        # Return percentage
        return_pct = (net_pnl / self.position_value) * 100 if self.position_value > 0 else Decimal(0)

        return {
            "gross_pnl": gross_pnl,
            "commission": total_commission,
            "slippage": total_slippage,
            "net_pnl": net_pnl,
            "return_pct": return_pct,
        }


class Portfolio:
    """
    Manages portfolio state during backtest.

    Tracks cash, positions, and equity curve.
    """

    def __init__(
        self,
        initial_capital: Decimal,
        commission_rate: Decimal = Decimal("0.0003"),
        slippage_rate: Decimal = Decimal("0.001"),
    ):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate

        # Open positions: {stock_code: Position}
        self.positions: Dict[str, Position] = {}

        # Closed positions for trade log
        self.closed_positions: List[Dict] = []

        # Equity curve for daily snapshots
        self.equity_curve: List[Dict] = []

    @property
    def holdings_value(self) -> Decimal:
        """Total value of open positions at current prices"""
        # Note: This returns entry value. For current value, call update_holdings_value()
        return sum(pos.position_value for pos in self.positions.values())

    @property
    def total_value(self) -> Decimal:
        """Total portfolio value (cash + holdings)"""
        return self.cash + self.holdings_value

    @property
    def total_return(self) -> Decimal:
        """Portfolio return as percentage"""
        if self.initial_capital == 0:
            return Decimal(0)
        return ((self.total_value - self.initial_capital) / self.initial_capital) * 100

    def open_position(
        self,
        stock_code: str,
        stock_name: str,
        entry_date: date,
        entry_price: Decimal,
        shares: int,
        entry_signal: Dict,
    ) -> None:
        """
        Open a new position

        Args:
            stock_code: Stock code
            stock_name: Stock name
            entry_date: Entry date
            entry_price: Entry price
            shares: Number of shares
            entry_signal: Entry signal metadata

        Raises:
            ValueError: If insufficient cash
        """
        position = Position(
            stock_code=stock_code,
            stock_name=stock_name,
            entry_date=entry_date,
            entry_price=entry_price,
            shares=shares,
            entry_signal=entry_signal,
        )

        # Calculate cost including entry costs
        entry_commission = entry_price * shares * self.commission_rate
        entry_slippage = entry_price * shares * self.slippage_rate
        total_cost = position.position_value + entry_commission + entry_slippage

        if total_cost > self.cash:
            raise ValueError(f"Insufficient cash: {self.cash} < {total_cost}")

        self.cash -= total_cost
        self.positions[stock_code] = position

        logger.debug(
            f"Opened position: {stock_code} {shares} shares @ {entry_price} "
            f"(cost: {total_cost}, cash remaining: {self.cash})"
        )

    def close_position(
        self,
        stock_code: str,
        exit_date: date,
        exit_price: Decimal,
        exit_reason: str,
    ) -> Dict:
        """
        Close an existing position

        Args:
            stock_code: Stock code
            exit_date: Exit date
            exit_price: Exit price
            exit_reason: Reason for exit (take_profit, stop_loss, etc.)

        Returns:
            Trade record dict

        Raises:
            ValueError: If no open position for stock_code
        """
        if stock_code not in self.positions:
            raise ValueError(f"No open position for {stock_code}")

        position = self.positions.pop(stock_code)

        # Calculate P&L
        pnl_data = position.calculate_pnl(
            exit_price,
            self.commission_rate,
            self.slippage_rate,
        )

        # Return cash (proceeds - exit costs)
        exit_commission = exit_price * position.shares * self.commission_rate
        exit_slippage = exit_price * position.shares * self.slippage_rate
        proceeds = exit_price * position.shares - exit_commission - exit_slippage
        self.cash += proceeds

        # Record trade
        holding_days = (exit_date - position.entry_date).days
        trade = {
            "stock_code": stock_code,
            "stock_name": position.stock_name,
            "entry_date": position.entry_date,
            "entry_price": position.entry_price,
            "exit_date": exit_date,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "shares": position.shares,
            "position_value": position.position_value,
            "holding_days": holding_days,
            **pnl_data,
        }

        self.closed_positions.append(trade)

        logger.debug(
            f"Closed position: {stock_code} {position.shares} shares @ {exit_price} "
            f"(P&L: {pnl_data['net_pnl']}, cash: {self.cash})"
        )

        return trade

    def snapshot(self, snapshot_date: date, current_prices: Dict[str, Decimal] = None) -> Dict:
        """
        Take daily portfolio snapshot

        Args:
            snapshot_date: Date of snapshot
            current_prices: Optional dict of current prices for open positions

        Returns:
            Snapshot dict
        """
        # Calculate current holdings value if prices provided
        holdings_val = Decimal(0)
        if current_prices:
            for code, position in self.positions.items():
                if code in current_prices:
                    holdings_val += current_prices[code] * position.shares
        else:
            holdings_val = self.holdings_value

        total_val = self.cash + holdings_val
        cumulative_ret = ((total_val - self.initial_capital) / self.initial_capital) * 100

        # Calculate daily return
        daily_ret = Decimal(0)
        if len(self.equity_curve) > 0:
            prev_val = self.equity_curve[-1]["total_value"]
            if prev_val > 0:
                daily_ret = ((total_val - prev_val) / prev_val) * 100

        snapshot = {
            "snapshot_date": snapshot_date,
            "cash": self.cash,
            "holdings_value": holdings_val,
            "total_value": total_val,
            "daily_return": daily_ret,
            "cumulative_return": cumulative_ret,
            "open_positions": len(self.positions),
            "total_positions_closed": len(self.closed_positions),
        }

        self.equity_curve.append(snapshot)
        return snapshot


class BacktestResult:
    """
    Stores comprehensive backtest results and performance metrics
    """

    def __init__(
        self,
        trades: List[Dict],
        equity_curve: List[Dict],
        initial_capital: Decimal,
        trading_days: int = None,
    ):
        self.trades = trades
        self.equity_curve = equity_curve
        self.initial_capital = initial_capital

        # Estimate trading days if not provided
        if trading_days is None:
            trading_days = len(equity_curve) if equity_curve else 1

        # Use PerformanceAnalyzer for comprehensive metrics
        self.performance_metrics: PerformanceMetrics = PerformanceAnalyzer.calculate(
            trades=trades,
            equity_curve=equity_curve,
            initial_capital=initial_capital,
            trading_days=trading_days,
        )

        # Set commonly accessed attributes for backward compatibility
        self.total_trades = self.performance_metrics.total_trades
        self.winning_trades = sum(1 for t in trades if t["net_pnl"] > 0)
        self.losing_trades = sum(1 for t in trades if t["net_pnl"] < 0)
        self.win_rate = float(self.performance_metrics.win_rate) * 100
        self.total_return = float(self.performance_metrics.total_return)
        self.sharpe_ratio = float(self.performance_metrics.sharpe_ratio)
        self.sortino_ratio = float(self.performance_metrics.sortino_ratio)
        self.calmar_ratio = float(self.performance_metrics.calmar_ratio)
        self.max_drawdown = float(self.performance_metrics.max_drawdown)
        self.profit_factor = float(self.performance_metrics.profit_factor)
        self.avg_win = float(self.performance_metrics.avg_win)
        self.avg_loss = float(self.performance_metrics.avg_loss)
        self.avg_holding_days = float(self.performance_metrics.avg_holding_period)

    def summary(self) -> Dict:
        """Return summary metrics as dict with enhanced analytics"""
        return {
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": round(self.win_rate, 2),
            "total_return": round(float(self.total_return), 2),
            "annualized_return": round(float(self.performance_metrics.annualized_return), 2),
            "sharpe_ratio": round(float(self.sharpe_ratio), 2),
            "sortino_ratio": round(float(self.sortino_ratio), 2),
            "calmar_ratio": round(float(self.calmar_ratio), 2),
            "max_drawdown": round(float(self.max_drawdown), 2),
            "avg_drawdown": round(float(self.performance_metrics.avg_drawdown), 2),
            "volatility": round(float(self.performance_metrics.volatility), 2),
            "profit_factor": round(float(self.profit_factor), 2),
            "avg_win": round(float(self.avg_win), 2),
            "avg_loss": round(float(self.avg_loss), 2),
            "largest_win": round(float(self.performance_metrics.largest_win), 2),
            "largest_loss": round(float(self.performance_metrics.largest_loss), 2),
            "expectancy": round(float(self.performance_metrics.expectancy), 2),
            "max_consecutive_wins": self.performance_metrics.max_consecutive_wins,
            "max_consecutive_losses": self.performance_metrics.max_consecutive_losses,
            "avg_holding_days": round(self.avg_holding_days, 1),
            "turnover_rate": round(float(self.performance_metrics.turnover_rate), 1),
        }

    def __str__(self) -> str:
        """String representation"""
        s = self.summary()
        return (
            f"Backtest Results:\n"
            f"  Total Trades: {s['total_trades']}\n"
            f"  Win Rate: {s['win_rate']}%\n"
            f"  Total Return: {s['total_return']}%\n"
            f"  Sharpe Ratio: {s['sharpe_ratio']}\n"
            f"  Max Drawdown: {s['max_drawdown']}%\n"
            f"  Profit Factor: {s['profit_factor']}\n"
            f"  Avg Win: ¥{s['avg_win']}\n"
            f"  Avg Loss: ¥{s['avg_loss']}\n"
            f"  Avg Holding Days: {s['avg_holding_days']}\n"
        )


class BacktestEngine:
    """
    Main backtesting execution engine.

    Orchestrates the entire backtest process:
    1. Initialize portfolio
    2. Iterate through time
    3. Check exits and entries
    4. Record results
    """

    def __init__(
        self,
        strategy_definition: Dict,
        start_date: date,
        end_date: date,
        initial_capital: Decimal = Decimal("1000000"),
        commission_rate: Decimal = Decimal("0.0003"),
        slippage_rate: Decimal = Decimal("0.001"),
        max_positions: int = 20,
    ):
        """
        Initialize BacktestEngine

        Args:
            strategy_definition: Strategy definition dict
            start_date: Backtest start date
            end_date: Backtest end date
            initial_capital: Starting capital (default 1M CNY)
            commission_rate: Trading commission rate (default 0.03%)
            slippage_rate: Slippage rate (default 0.1%)
            max_positions: Maximum concurrent positions (default 20)
        """
        self.strategy_definition = strategy_definition
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.commission_rate = commission_rate
        self.slippage_rate = slippage_rate
        self.max_positions = max_positions

        # Initialize components
        self.portfolio = Portfolio(initial_capital, commission_rate, slippage_rate)
        self.signal_engine = SignalEngine(strategy_definition)

    def run(self, engine: Engine) -> BacktestResult:
        """
        Execute backtest

        Args:
            engine: Database engine

        Returns:
            BacktestResult object with comprehensive metrics
        """
        logger.info(
            f"Starting backtest from {self.start_date} to {self.end_date} "
            f"with {self.initial_capital} initial capital"
        )

        current_date = self.start_date
        day_count = 0

        while current_date <= self.end_date:
            # Skip non-trading days
            if not is_stock_market_open(current_date):
                current_date = next_trade_day(current_date, inclusive=False)
                continue

            day_count += 1
            logger.debug(f"Processing {current_date} (day {day_count})")

            # Step 1: Process exits for open positions
            self._process_exits(current_date, engine)

            # Step 2: Process entries (scan for new signals)
            self._process_entries(current_date, engine)

            # Step 3: Take daily snapshot
            current_prices = self._get_current_prices(current_date, engine)
            self.portfolio.snapshot(current_date, current_prices)

            # Move to next trading day
            current_date = next_trade_day(current_date, inclusive=False)

        # Final step: Close all remaining positions
        logger.info(f"Closing {len(self.portfolio.positions)} remaining positions")
        self._close_all_positions(self.end_date, engine)

        # Calculate final results
        result = BacktestResult(
            trades=self.portfolio.closed_positions,
            equity_curve=self.portfolio.equity_curve,
            initial_capital=self.initial_capital,
        )

        logger.success(f"Backtest completed: {result.total_trades} trades executed")
        logger.info(f"\n{result}")

        return result

    def _process_exits(self, current_date: date, engine: Engine) -> None:
        """
        Check and execute exit conditions for open positions

        Args:
            current_date: Current trading date
            engine: Database engine
        """
        for stock_code, position in list(self.portfolio.positions.items()):
            # Get current price
            current_price = self.signal_engine.get_current_price(
                stock_code, current_date, engine
            )

            if not current_price:
                logger.warning(f"No price data for {stock_code} on {current_date}")
                continue

            # Evaluate exit conditions
            exit_reason = self.signal_engine.check_exit(
                position, current_price, current_date, engine
            )

            if exit_reason:
                self.portfolio.close_position(
                    stock_code, current_date, current_price, exit_reason
                )

    def _process_entries(self, current_date: date, engine: Engine) -> None:
        """
        Find and execute entry signals

        Args:
            current_date: Current trading date
            engine: Database engine
        """
        # Check how many positions we can still open
        open_slots = self.max_positions - len(self.portfolio.positions)

        if open_slots <= 0:
            logger.debug(f"Max positions reached ({self.max_positions})")
            return

        # Get candidate stocks
        candidates = self.signal_engine.scan_entry_signals(current_date, engine)

        if not candidates:
            logger.debug(f"No entry signals found for {current_date}")
            return

        # Calculate capital per position (equal weighting)
        if self.portfolio.cash <= 0:
            logger.debug("No cash available for new positions")
            return

        capital_per_position = self.portfolio.cash / open_slots

        # Open positions for top candidates (up to open_slots)
        opened_count = 0
        for candidate in candidates[:open_slots]:
            # Calculate shares to buy
            shares = int(capital_per_position / candidate["price"])

            if shares == 0:
                logger.debug(
                    f"Insufficient capital for {candidate['code']} @ {candidate['price']}"
                )
                continue

            try:
                self.portfolio.open_position(
                    stock_code=candidate["code"],
                    stock_name=candidate["name"],
                    entry_date=current_date,
                    entry_price=candidate["price"],
                    shares=shares,
                    entry_signal=candidate["signal"],
                )
                opened_count += 1
            except ValueError as e:
                # Insufficient cash
                logger.warning(f"Failed to open position: {e}")
                break

        if opened_count > 0:
            logger.debug(f"Opened {opened_count} new positions on {current_date}")

    def _close_all_positions(self, final_date: date, engine: Engine) -> None:
        """
        Close all remaining positions at backtest end

        Args:
            final_date: Final trading date
            engine: Database engine
        """
        for stock_code in list(self.portfolio.positions.keys()):
            price = self.signal_engine.get_current_price(stock_code, final_date, engine)

            if price:
                self.portfolio.close_position(
                    stock_code, final_date, price, "backtest_end"
                )
            else:
                logger.warning(
                    f"No price data for {stock_code} on {final_date}, "
                    f"position not closed"
                )

    def _get_current_prices(self, trade_day: date, engine: Engine) -> Dict[str, Decimal]:
        """
        Get current prices for all open positions

        Args:
            trade_day: Trading date
            engine: Database engine

        Returns:
            Dict mapping stock_code -> price
        """
        stock_codes = list(self.portfolio.positions.keys())
        return self.signal_engine.get_current_prices_batch(stock_codes, trade_day, engine)

    def save_to_database(
        self,
        engine: Engine,
        strategy_id: str,
        result: BacktestResult,
    ) -> str:
        """
        Save backtest results to database

        Args:
            engine: Database engine
            strategy_id: Strategy UUID
            result: BacktestResult object

        Returns:
            Backtest run ID
        """
        from datetime import datetime

        with Session(engine) as session:
            # Create backtest run record
            backtest_run = BacktestRun(
                strategy_id=strategy_id,
                start_date=self.start_date,
                end_date=self.end_date,
                initial_capital=self.initial_capital,
                commission_rate=self.commission_rate,
                slippage_rate=self.slippage_rate,
                total_return=Decimal(str(result.total_return)),
                annualized_return=Decimal(str(result.performance_metrics.annualized_return)),
                sharpe_ratio=Decimal(str(result.sharpe_ratio)),
                sortino_ratio=Decimal(str(result.sortino_ratio)),
                calmar_ratio=Decimal(str(result.calmar_ratio)),
                max_drawdown=Decimal(str(result.max_drawdown)),
                volatility=Decimal(str(result.performance_metrics.volatility)),
                win_rate=Decimal(str(result.win_rate)),
                profit_factor=Decimal(str(result.profit_factor)),
                total_trades=result.total_trades,
                status="completed",
                started_at=datetime.now(),
                completed_at=datetime.now(),
            )
            session.add(backtest_run)
            session.flush()  # Get the ID

            backtest_run_id = backtest_run.id

            # Save trades
            for trade_data in result.trades:
                trade = Trade(
                    backtest_run_id=backtest_run_id,
                    stock_code=trade_data["stock_code"],
                    stock_name=trade_data["stock_name"],
                    entry_date=trade_data["entry_date"],
                    entry_price=trade_data["entry_price"],
                    exit_date=trade_data["exit_date"],
                    exit_price=trade_data["exit_price"],
                    exit_reason=trade_data["exit_reason"],
                    shares=trade_data["shares"],
                    position_value=trade_data["position_value"],
                    gross_pnl=trade_data["gross_pnl"],
                    commission=trade_data["commission"],
                    slippage=trade_data["slippage"],
                    net_pnl=trade_data["net_pnl"],
                    return_pct=trade_data["return_pct"],
                    holding_days=trade_data["holding_days"],
                )
                session.add(trade)

            # Save portfolio snapshots
            for snapshot_data in result.equity_curve:
                snapshot = PortfolioSnapshot(
                    backtest_run_id=backtest_run_id,
                    snapshot_date=snapshot_data["snapshot_date"],
                    cash=snapshot_data["cash"],
                    holdings_value=snapshot_data["holdings_value"],
                    total_value=snapshot_data["total_value"],
                    daily_return=snapshot_data["daily_return"],
                    cumulative_return=snapshot_data["cumulative_return"],
                    open_positions=snapshot_data["open_positions"],
                    total_positions_closed=snapshot_data["total_positions_closed"],
                )
                session.add(snapshot)

            session.commit()
            logger.success(f"Saved backtest results to database (run_id: {backtest_run_id})")

            return backtest_run_id
