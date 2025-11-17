"""
Performance Analytics - Comprehensive performance metrics calculation

This module provides advanced performance metrics for backtesting:
- Risk-adjusted returns (Sharpe, Sortino, Calmar)
- Drawdown analysis
- Trade statistics
- Streaks and holding period analysis
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List

import numpy as np
from loguru import logger


@dataclass
class PerformanceMetrics:
    """Comprehensive performance metrics"""

    # Returns
    total_return: Decimal
    annualized_return: Decimal
    cumulative_returns: List[Decimal]

    # Risk-adjusted
    sharpe_ratio: Decimal
    sortino_ratio: Decimal
    calmar_ratio: Decimal

    # Risk
    max_drawdown: Decimal
    avg_drawdown: Decimal
    max_drawdown_duration: int  # days
    volatility: Decimal

    # Trade statistics
    total_trades: int
    win_rate: Decimal
    profit_factor: Decimal
    avg_win: Decimal
    avg_loss: Decimal
    largest_win: Decimal
    largest_loss: Decimal
    expectancy: Decimal

    # Streaks
    max_consecutive_wins: int
    max_consecutive_losses: int

    # Time-based
    avg_holding_period: Decimal  # days
    turnover_rate: Decimal  # trades per year

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "total_return": float(self.total_return),
            "annualized_return": float(self.annualized_return),
            "cumulative_returns": [float(r) for r in self.cumulative_returns],
            "sharpe_ratio": float(self.sharpe_ratio),
            "sortino_ratio": float(self.sortino_ratio),
            "calmar_ratio": float(self.calmar_ratio),
            "max_drawdown": float(self.max_drawdown),
            "avg_drawdown": float(self.avg_drawdown),
            "max_drawdown_duration": self.max_drawdown_duration,
            "volatility": float(self.volatility),
            "total_trades": self.total_trades,
            "win_rate": float(self.win_rate),
            "profit_factor": float(self.profit_factor),
            "avg_win": float(self.avg_win),
            "avg_loss": float(self.avg_loss),
            "largest_win": float(self.largest_win),
            "largest_loss": float(self.largest_loss),
            "expectancy": float(self.expectancy),
            "max_consecutive_wins": self.max_consecutive_wins,
            "max_consecutive_losses": self.max_consecutive_losses,
            "avg_holding_period": float(self.avg_holding_period),
            "turnover_rate": float(self.turnover_rate),
        }


class PerformanceAnalyzer:
    """Calculate comprehensive performance metrics"""

    @staticmethod
    def calculate(
        trades: List[Dict],
        equity_curve: List[Dict],
        initial_capital: Decimal,
        trading_days: int,
    ) -> PerformanceMetrics:
        """
        Calculate all performance metrics

        Args:
            trades: List of trade dictionaries with net_pnl, return_pct, holding_days
            equity_curve: List of daily portfolio snapshots
            initial_capital: Starting capital
            trading_days: Number of trading days in backtest period

        Returns:
            PerformanceMetrics object with all calculated metrics
        """
        logger.debug(
            f"Calculating performance metrics for {len(trades)} trades, {len(equity_curve)} days"
        )

        # Basic trade statistics
        total_trades = len(trades)
        if total_trades == 0:
            logger.warning("No trades found, returning zero metrics")
            return PerformanceAnalyzer._zero_metrics()

        winning_trades = [t for t in trades if Decimal(str(t["net_pnl"])) > 0]
        losing_trades = [t for t in trades if Decimal(str(t["net_pnl"])) < 0]

        win_rate = (
            Decimal(len(winning_trades)) / Decimal(total_trades)
            if total_trades > 0
            else Decimal(0)
        )

        # P&L statistics
        gross_profit = (
            sum(Decimal(str(t["net_pnl"])) for t in winning_trades)
            if winning_trades
            else Decimal(0)
        )
        gross_loss = (
            abs(sum(Decimal(str(t["net_pnl"])) for t in losing_trades))
            if losing_trades
            else Decimal(0)
        )

        profit_factor = (
            gross_profit / gross_loss if gross_loss > 0 else Decimal("inf")
        )

        avg_win = gross_profit / len(winning_trades) if winning_trades else Decimal(0)
        avg_loss = gross_loss / len(losing_trades) if losing_trades else Decimal(0)

        largest_win = (
            max(Decimal(str(t["return_pct"])) for t in winning_trades)
            if winning_trades
            else Decimal(0)
        )
        largest_loss = (
            min(Decimal(str(t["return_pct"])) for t in losing_trades)
            if losing_trades
            else Decimal(0)
        )

        expectancy = (win_rate * avg_win) - ((Decimal(1) - win_rate) * avg_loss)

        # Returns
        if equity_curve and len(equity_curve) > 0:
            final_value = Decimal(str(equity_curve[-1]["total_value"]))
        else:
            final_value = initial_capital

        total_return = ((final_value - initial_capital) / initial_capital) * 100

        years = Decimal(trading_days) / Decimal(252)
        if years > 0 and final_value > 0 and initial_capital > 0:
            annualized_return = (
                (final_value / initial_capital) ** (Decimal(1) / years) - 1
            ) * 100
        else:
            annualized_return = Decimal(0)

        # Daily returns
        daily_returns = []
        if len(equity_curve) > 1:
            for i in range(1, len(equity_curve)):
                prev_value = Decimal(str(equity_curve[i - 1]["total_value"]))
                curr_value = Decimal(str(equity_curve[i]["total_value"]))
                if prev_value > 0:
                    daily_return = float((curr_value / prev_value) - 1)
                    daily_returns.append(daily_return)

        # Volatility (annualized)
        volatility = (
            Decimal(str(np.std(daily_returns))) * Decimal(str(np.sqrt(252))) * 100
            if daily_returns
            else Decimal(0)
        )

        # Sharpe ratio (assumes 0 risk-free rate)
        mean_return = (
            Decimal(str(np.mean(daily_returns))) if daily_returns else Decimal(0)
        )
        std_return = (
            Decimal(str(np.std(daily_returns))) if daily_returns else Decimal(0)
        )
        sharpe_ratio = (
            (mean_return / std_return) * Decimal(str(np.sqrt(252)))
            if std_return > 0
            else Decimal(0)
        )

        # Sortino ratio (downside deviation)
        downside_returns = [r for r in daily_returns if r < 0]
        downside_deviation = (
            Decimal(str(np.std(downside_returns))) if downside_returns else Decimal(0)
        )
        sortino_ratio = (
            (mean_return / downside_deviation) * Decimal(str(np.sqrt(252)))
            if downside_deviation > 0
            else Decimal(0)
        )

        # Drawdown analysis
        peak = Decimal(str(equity_curve[0]["total_value"])) if equity_curve else initial_capital
        max_drawdown = Decimal(0)
        max_dd_duration = 0
        current_dd_duration = 0
        total_drawdown = Decimal(0)
        num_drawdowns = 0

        for snapshot in equity_curve:
            curr_value = Decimal(str(snapshot["total_value"]))

            if curr_value > peak:
                peak = curr_value
                if current_dd_duration > 0:
                    num_drawdowns += 1
                current_dd_duration = 0
            else:
                current_dd_duration += 1

            drawdown = (peak - curr_value) / peak * 100 if peak > 0 else Decimal(0)
            total_drawdown += drawdown

            if drawdown > max_drawdown:
                max_drawdown = drawdown
                max_dd_duration = max(max_dd_duration, current_dd_duration)

        avg_drawdown = (
            total_drawdown / len(equity_curve) if equity_curve else Decimal(0)
        )

        # Calmar ratio
        calmar_ratio = (
            annualized_return / max_drawdown if max_drawdown > 0 else Decimal(0)
        )

        # Streaks
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        current_win_streak = 0
        current_loss_streak = 0

        for trade in trades:
            if Decimal(str(trade["net_pnl"])) > 0:
                current_win_streak += 1
                current_loss_streak = 0
                max_consecutive_wins = max(max_consecutive_wins, current_win_streak)
            else:
                current_loss_streak += 1
                current_win_streak = 0
                max_consecutive_losses = max(
                    max_consecutive_losses, current_loss_streak
                )

        # Holding period
        avg_holding_period = (
            Decimal(sum(t["holding_days"] for t in trades)) / Decimal(total_trades)
            if total_trades > 0
            else Decimal(0)
        )

        # Turnover rate (trades per year)
        turnover_rate = Decimal(total_trades) / years if years > 0 else Decimal(0)

        # Cumulative returns
        cumulative_returns = (
            [Decimal(str(s["cumulative_return"])) for s in equity_curve]
            if equity_curve
            else [Decimal(0)]
        )

        logger.debug(
            f"Metrics calculated: Return={total_return:.2f}%, Sharpe={sharpe_ratio:.2f}, MaxDD={max_drawdown:.2f}%"
        )

        return PerformanceMetrics(
            total_return=total_return,
            annualized_return=annualized_return,
            cumulative_returns=cumulative_returns,
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            calmar_ratio=calmar_ratio,
            max_drawdown=max_drawdown,
            avg_drawdown=avg_drawdown,
            max_drawdown_duration=max_dd_duration,
            volatility=volatility,
            total_trades=total_trades,
            win_rate=win_rate,
            profit_factor=profit_factor,
            avg_win=avg_win,
            avg_loss=avg_loss,
            largest_win=largest_win,
            largest_loss=largest_loss,
            expectancy=expectancy,
            max_consecutive_wins=max_consecutive_wins,
            max_consecutive_losses=max_consecutive_losses,
            avg_holding_period=avg_holding_period,
            turnover_rate=turnover_rate,
        )

    @staticmethod
    def _zero_metrics() -> PerformanceMetrics:
        """Return zero metrics when no trades"""
        return PerformanceMetrics(
            total_return=Decimal(0),
            annualized_return=Decimal(0),
            cumulative_returns=[Decimal(0)],
            sharpe_ratio=Decimal(0),
            sortino_ratio=Decimal(0),
            calmar_ratio=Decimal(0),
            max_drawdown=Decimal(0),
            avg_drawdown=Decimal(0),
            max_drawdown_duration=0,
            volatility=Decimal(0),
            total_trades=0,
            win_rate=Decimal(0),
            profit_factor=Decimal(0),
            avg_win=Decimal(0),
            avg_loss=Decimal(0),
            largest_win=Decimal(0),
            largest_loss=Decimal(0),
            expectancy=Decimal(0),
            max_consecutive_wins=0,
            max_consecutive_losses=0,
            avg_holding_period=Decimal(0),
            turnover_rate=Decimal(0),
        )
