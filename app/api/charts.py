"""
Chart Data Generators - Generate frontend-ready visualization data

This module provides chart data in formats optimized for frontend libraries:
- Equity curve charts
- Monthly returns heatmap
- Trade distribution histogram
- Drawdown charts
- Multi-strategy comparison charts
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Optional

import numpy as np
from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import BacktestRun, PortfolioSnapshot, Trade


class ChartDataGenerator:
    """Generate chart data for various visualizations"""

    @staticmethod
    def equity_curve(
        run_id: str,
        session: Session,
        include_drawdown: bool = True,
        benchmark_run_id: Optional[str] = None,
    ) -> Dict:
        """
        Generate equity curve chart data

        Args:
            run_id: Backtest run ID
            session: Database session
            include_drawdown: Include drawdown series
            benchmark_run_id: Optional benchmark run ID for comparison

        Returns:
            Dictionary with dates, portfolio_value, drawdown, and optional benchmark
        """
        logger.debug(f"Generating equity curve for run {run_id}")

        # Fetch run info
        run = session.get(BacktestRun, run_id)
        if not run:
            raise ValueError(f"Backtest run not found: {run_id}")

        # Fetch portfolio snapshots
        stmt = (
            select(PortfolioSnapshot)
            .where(PortfolioSnapshot.backtest_run_id == run_id)
            .order_by(PortfolioSnapshot.snapshot_date)
        )
        snapshots = session.execute(stmt).scalars().all()

        if not snapshots:
            logger.warning(f"No portfolio snapshots found for run {run_id}")
            return {
                "chart_type": "equity_curve",
                "data": {
                    "dates": [],
                    "portfolio_value": [],
                    "drawdown": [],
                },
                "config": {
                    "title": "Portfolio Equity Curve",
                    "y_axis_label": "Portfolio Value (¥)",
                    "show_drawdown": include_drawdown,
                },
            }

        # Extract data
        dates = [s.snapshot_date.isoformat() for s in snapshots]
        portfolio_values = [float(s.total_value) for s in snapshots]

        # Calculate drawdown if requested
        drawdowns = []
        if include_drawdown:
            peak = portfolio_values[0]
            for value in portfolio_values:
                if value > peak:
                    peak = value
                drawdown = ((peak - value) / peak * 100) if peak > 0 else 0.0
                drawdowns.append(-drawdown)  # Negative for display

        # Fetch benchmark if requested
        benchmark_values = []
        if benchmark_run_id:
            benchmark_run = session.get(BacktestRun, benchmark_run_id)
            if benchmark_run:
                stmt_bench = (
                    select(PortfolioSnapshot)
                    .where(PortfolioSnapshot.backtest_run_id == benchmark_run_id)
                    .order_by(PortfolioSnapshot.snapshot_date)
                )
                bench_snapshots = session.execute(stmt_bench).scalars().all()
                benchmark_values = [float(s.total_value) for s in bench_snapshots]

        result = {
            "chart_type": "equity_curve",
            "data": {
                "dates": dates,
                "portfolio_value": portfolio_values,
            },
            "config": {
                "title": "Portfolio Equity Curve",
                "y_axis_label": "Portfolio Value (¥)",
                "show_drawdown": include_drawdown,
            },
        }

        if include_drawdown:
            result["data"]["drawdown"] = drawdowns

        if benchmark_values:
            result["data"]["benchmark"] = benchmark_values

        logger.debug(f"Equity curve generated with {len(dates)} data points")
        return result

    @staticmethod
    def monthly_returns(run_id: str, session: Session) -> Dict:
        """
        Generate monthly returns heatmap data

        Args:
            run_id: Backtest run ID
            session: Database session

        Returns:
            Dictionary with monthly returns organized by year
        """
        logger.debug(f"Generating monthly returns for run {run_id}")

        # Fetch portfolio snapshots
        stmt = (
            select(PortfolioSnapshot)
            .where(PortfolioSnapshot.backtest_run_id == run_id)
            .order_by(PortfolioSnapshot.snapshot_date)
        )
        snapshots = session.execute(stmt).scalars().all()

        if not snapshots:
            return {
                "chart_type": "monthly_returns",
                "data": {},
                "config": {
                    "title": "Monthly Returns (%)",
                    "colormap": "RdYlGn",
                },
            }

        # Group by year and month
        monthly_data = {}
        current_year = None
        current_month = None
        month_start_value = None

        for snapshot in snapshots:
            year = snapshot.snapshot_date.year
            month = snapshot.snapshot_date.month

            # Initialize year if needed
            if year not in monthly_data:
                monthly_data[year] = [None] * 12

            # Track month start
            if year != current_year or month != current_month:
                if current_year and current_month and month_start_value:
                    # Calculate previous month return
                    prev_month_idx = current_month - 1
                    # Find last value of previous month
                    # (simplified - using start of this month)
                    pass

                current_year = year
                current_month = month
                month_start_value = float(snapshot.total_value)

        # Calculate returns for each month
        returns_by_year = {}
        for year in sorted(monthly_data.keys()):
            # Fetch snapshots for this year
            year_snapshots = [
                s for s in snapshots if s.snapshot_date.year == year
            ]

            monthly_returns = [None] * 12
            for month in range(1, 13):
                month_snaps = [
                    s for s in year_snapshots if s.snapshot_date.month == month
                ]

                if len(month_snaps) >= 2:
                    start_value = float(month_snaps[0].total_value)
                    end_value = float(month_snaps[-1].total_value)
                    monthly_return = ((end_value - start_value) / start_value * 100) if start_value > 0 else 0.0
                    monthly_returns[month - 1] = round(monthly_return, 2)

            returns_by_year[year] = monthly_returns

        result = {
            "chart_type": "monthly_returns",
            "data": returns_by_year,
            "config": {
                "title": "Monthly Returns (%)",
                "colormap": "RdYlGn",
            },
        }

        logger.debug(f"Monthly returns generated for {len(returns_by_year)} years")
        return result

    @staticmethod
    def trade_distribution(run_id: str, session: Session, bins: int = 20) -> Dict:
        """
        Generate trade return distribution histogram

        Args:
            run_id: Backtest run ID
            session: Database session
            bins: Number of histogram bins

        Returns:
            Dictionary with bins and frequencies
        """
        logger.debug(f"Generating trade distribution for run {run_id}")

        # Fetch trades
        stmt = select(Trade).where(Trade.backtest_run_id == run_id)
        trades = session.execute(stmt).scalars().all()

        if not trades:
            return {
                "chart_type": "trade_distribution",
                "data": {
                    "bins": [],
                    "frequencies": [],
                },
                "config": {
                    "title": "Distribution of Trade Returns",
                    "x_axis_label": "Return (%)",
                    "y_axis_label": "Frequency",
                },
            }

        # Extract return percentages
        returns = [float(t.return_pct) for t in trades]

        # Create histogram
        frequencies, bin_edges = np.histogram(returns, bins=bins)

        # Format bins as ranges
        bin_labels = []
        for i in range(len(bin_edges) - 1):
            bin_labels.append(round(bin_edges[i], 2))

        result = {
            "chart_type": "trade_distribution",
            "data": {
                "bins": bin_labels,
                "frequencies": frequencies.tolist(),
            },
            "config": {
                "title": "Distribution of Trade Returns",
                "x_axis_label": "Return (%)",
                "y_axis_label": "Frequency",
            },
            "statistics": {
                "mean": round(float(np.mean(returns)), 2),
                "median": round(float(np.median(returns)), 2),
                "std": round(float(np.std(returns)), 2),
                "min": round(float(np.min(returns)), 2),
                "max": round(float(np.max(returns)), 2),
            },
        }

        logger.debug(f"Trade distribution generated with {bins} bins, {len(trades)} trades")
        return result

    @staticmethod
    def drawdown_chart(run_id: str, session: Session) -> Dict:
        """
        Generate drawdown chart data

        Args:
            run_id: Backtest run ID
            session: Database session

        Returns:
            Dictionary with dates and drawdown percentages
        """
        logger.debug(f"Generating drawdown chart for run {run_id}")

        # Fetch portfolio snapshots
        stmt = (
            select(PortfolioSnapshot)
            .where(PortfolioSnapshot.backtest_run_id == run_id)
            .order_by(PortfolioSnapshot.snapshot_date)
        )
        snapshots = session.execute(stmt).scalars().all()

        if not snapshots:
            return {
                "chart_type": "drawdown",
                "data": {
                    "dates": [],
                    "drawdown": [],
                },
                "config": {
                    "title": "Portfolio Drawdown",
                    "y_axis_label": "Drawdown (%)",
                },
            }

        # Calculate drawdown
        dates = []
        drawdowns = []
        peak = float(snapshots[0].total_value)

        for snapshot in snapshots:
            value = float(snapshot.total_value)

            if value > peak:
                peak = value

            drawdown = ((peak - value) / peak * 100) if peak > 0 else 0.0

            dates.append(snapshot.snapshot_date.isoformat())
            drawdowns.append(-drawdown)  # Negative for display

        # Find max drawdown period
        max_dd = min(drawdowns)
        max_dd_idx = drawdowns.index(max_dd)

        result = {
            "chart_type": "drawdown",
            "data": {
                "dates": dates,
                "drawdown": drawdowns,
            },
            "config": {
                "title": "Portfolio Drawdown",
                "y_axis_label": "Drawdown (%)",
            },
            "max_drawdown": {
                "value": round(max_dd, 2),
                "date": dates[max_dd_idx],
            },
        }

        logger.debug(f"Drawdown chart generated, max DD: {max_dd:.2f}%")
        return result

    @staticmethod
    def comparison_chart(
        run_ids: List[str],
        session: Session,
        metric: str = "total_value",
    ) -> Dict:
        """
        Generate multi-strategy comparison chart

        Args:
            run_ids: List of backtest run IDs
            session: Database session
            metric: Metric to compare ('total_value', 'cumulative_return', etc.)

        Returns:
            Dictionary with dates and series for each strategy
        """
        logger.debug(f"Generating comparison chart for {len(run_ids)} runs")

        series_data = {}

        for run_id in run_ids:
            run = session.get(BacktestRun, run_id)
            if not run:
                continue

            # Fetch snapshots
            stmt = (
                select(PortfolioSnapshot)
                .where(PortfolioSnapshot.backtest_run_id == run_id)
                .order_by(PortfolioSnapshot.snapshot_date)
            )
            snapshots = session.execute(stmt).scalars().all()

            if not snapshots:
                continue

            # Extract metric values
            dates = [s.snapshot_date.isoformat() for s in snapshots]

            if metric == "total_value":
                values = [float(s.total_value) for s in snapshots]
            elif metric == "cumulative_return":
                values = [float(s.cumulative_return) for s in snapshots]
            elif metric == "drawdown":
                values = [float(s.drawdown) for s in snapshots]
            else:
                values = [float(s.total_value) for s in snapshots]

            # Get strategy name
            from app.db.models import Strategy

            strategy = session.get(Strategy, run.strategy_id)
            strategy_name = strategy.name if strategy else f"Strategy {run_id[:8]}"

            series_data[strategy_name] = {
                "run_id": run_id,
                "dates": dates,
                "values": values,
            }

        result = {
            "chart_type": "comparison",
            "data": {
                "series": series_data,
            },
            "config": {
                "title": f"Strategy Comparison - {metric.replace('_', ' ').title()}",
                "x_axis_label": "Date",
                "y_axis_label": metric.replace("_", " ").title(),
            },
        }

        logger.debug(f"Comparison chart generated for {len(series_data)} strategies")
        return result
