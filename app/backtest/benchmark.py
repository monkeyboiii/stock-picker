"""
Benchmark Integration - Compare strategies against market indices

This module provides:
- Index data fetching (CSI 300, SSE 50, etc.)
- Strategy vs. benchmark comparison
- Alpha/Beta calculation
- Risk-adjusted performance metrics
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Dict, List, Optional

import numpy as np
from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import CollectionDaily, PortfolioSnapshot


@dataclass
class BenchmarkComparison:
    """Result of comparing strategy against benchmark"""

    strategy_return: Decimal
    benchmark_return: Decimal
    outperformance: Decimal  # Strategy - benchmark
    alpha: Decimal  # Excess return vs. benchmark
    beta: Decimal  # Volatility relative to benchmark
    correlation: Decimal  # Correlation with benchmark
    information_ratio: Decimal  # Outperformance / tracking error

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "strategy_return": float(self.strategy_return),
            "benchmark_return": float(self.benchmark_return),
            "outperformance": float(self.outperformance),
            "alpha": float(self.alpha),
            "beta": float(self.beta),
            "correlation": float(self.correlation),
            "information_ratio": float(self.information_ratio),
        }


class BenchmarkIntegration:
    """Compare strategies against market benchmarks"""

    # Common Chinese market indices
    BENCHMARKS = {
        "CSI300": "399300",  # CSI 300 Index
        "SSE50": "000016",  # SSE 50 Index
        "SSE_COMPOSITE": "000001",  # Shanghai Composite
        "SZSE_COMPONENT": "399001",  # Shenzhen Component
    }

    @staticmethod
    def compare_with_benchmark(
        run_id: str,
        benchmark_code: str,
        start_date: date,
        end_date: date,
        session: Session,
    ) -> BenchmarkComparison:
        """
        Compare backtest run against market benchmark

        Args:
            run_id: Backtest run ID
            benchmark_code: Benchmark index code (e.g., "399300" for CSI 300)
            start_date: Comparison start date
            end_date: Comparison end date
            session: Database session

        Returns:
            BenchmarkComparison with alpha, beta, and other metrics
        """
        logger.info(
            f"Comparing run {run_id} with benchmark {benchmark_code} "
            f"from {start_date} to {end_date}"
        )

        # Fetch strategy portfolio snapshots
        stmt_strategy = (
            select(PortfolioSnapshot)
            .where(
                PortfolioSnapshot.backtest_run_id == run_id,
                PortfolioSnapshot.snapshot_date >= start_date,
                PortfolioSnapshot.snapshot_date <= end_date,
            )
            .order_by(PortfolioSnapshot.snapshot_date)
        )
        strategy_snapshots = session.execute(stmt_strategy).scalars().all()

        if not strategy_snapshots:
            raise ValueError(f"No portfolio snapshots found for run {run_id}")

        # Fetch benchmark data
        stmt_benchmark = (
            select(CollectionDaily)
            .where(
                CollectionDaily.code == benchmark_code,
                CollectionDaily.trade_day >= start_date,
                CollectionDaily.trade_day <= end_date,
            )
            .order_by(CollectionDaily.trade_day)
        )
        benchmark_data = session.execute(stmt_benchmark).scalars().all()

        if not benchmark_data:
            raise ValueError(
                f"No benchmark data found for code {benchmark_code}. "
                f"Ensure index data is loaded in collection_daily table."
            )

        # Align dates (use strategy dates as reference)
        strategy_dates = [s.snapshot_date for s in strategy_snapshots]
        aligned_benchmark = BenchmarkIntegration._align_benchmark_data(
            strategy_dates, benchmark_data
        )

        if len(aligned_benchmark) != len(strategy_snapshots):
            logger.warning(
                f"Date alignment mismatch: {len(strategy_snapshots)} strategy days, "
                f"{len(aligned_benchmark)} benchmark days"
            )

        # Calculate daily returns
        strategy_returns = []
        for i in range(1, len(strategy_snapshots)):
            prev_val = Decimal(str(strategy_snapshots[i - 1].total_value))
            curr_val = Decimal(str(strategy_snapshots[i].total_value))
            ret = float((curr_val / prev_val) - 1) if prev_val > 0 else 0.0
            strategy_returns.append(ret)

        benchmark_returns = []
        for i in range(1, len(aligned_benchmark)):
            prev_price = Decimal(str(aligned_benchmark[i - 1]["price"]))
            curr_price = Decimal(str(aligned_benchmark[i]["price"]))
            ret = float((curr_price / prev_price) - 1) if prev_price > 0 else 0.0
            benchmark_returns.append(ret)

        # Ensure same length
        min_len = min(len(strategy_returns), len(benchmark_returns))
        strategy_returns = strategy_returns[:min_len]
        benchmark_returns = benchmark_returns[:min_len]

        if len(strategy_returns) == 0:
            raise ValueError("Insufficient data for comparison")

        # Total returns
        strategy_total_return = Decimal(
            str(
                (
                    float(strategy_snapshots[-1].total_value)
                    / float(strategy_snapshots[0].total_value)
                    - 1
                )
                * 100
            )
        )

        benchmark_total_return = Decimal(
            str(
                (
                    float(aligned_benchmark[-1]["price"])
                    / float(aligned_benchmark[0]["price"])
                    - 1
                )
                * 100
            )
        )

        outperformance = strategy_total_return - benchmark_total_return

        # Alpha and Beta calculation
        # Beta = Cov(strategy, benchmark) / Var(benchmark)
        # Alpha = Strategy return - (Risk-free rate + Beta * (Benchmark return - Risk-free rate))
        # Assuming risk-free rate = 0 for simplicity

        strategy_arr = np.array(strategy_returns)
        benchmark_arr = np.array(benchmark_returns)

        covariance = float(np.cov(strategy_arr, benchmark_arr)[0][1])
        benchmark_variance = float(np.var(benchmark_arr))

        beta = Decimal(
            str(covariance / benchmark_variance) if benchmark_variance > 0 else 0.0
        )

        # Alpha (annualized excess return)
        mean_strategy_return = float(np.mean(strategy_arr))
        mean_benchmark_return = float(np.mean(benchmark_arr))

        alpha_daily = mean_strategy_return - (beta * Decimal(str(mean_benchmark_return)))
        alpha_annualized = alpha_daily * Decimal(252) * 100  # Annualized percentage

        # Correlation
        correlation = Decimal(str(float(np.corrcoef(strategy_arr, benchmark_arr)[0][1])))

        # Information Ratio (outperformance / tracking error)
        tracking_error = Decimal(str(float(np.std(strategy_arr - benchmark_arr)) * np.sqrt(252) * 100))
        information_ratio = (
            outperformance / tracking_error if tracking_error > 0 else Decimal(0)
        )

        result = BenchmarkComparison(
            strategy_return=strategy_total_return,
            benchmark_return=benchmark_total_return,
            outperformance=outperformance,
            alpha=alpha_annualized,
            beta=beta,
            correlation=correlation,
            information_ratio=information_ratio,
        )

        logger.info(
            f"Benchmark comparison complete: Alpha={alpha_annualized:.2f}%, Beta={beta:.2f}, "
            f"Outperformance={outperformance:.2f}%"
        )

        return result

    @staticmethod
    def _align_benchmark_data(
        strategy_dates: List[date], benchmark_data: List[CollectionDaily]
    ) -> List[Dict]:
        """
        Align benchmark data with strategy dates

        Args:
            strategy_dates: List of dates from strategy snapshots
            benchmark_data: List of CollectionDaily objects

        Returns:
            List of aligned benchmark data dictionaries
        """
        # Create lookup map
        benchmark_map = {b.trade_day: b for b in benchmark_data}

        aligned = []
        for strat_date in strategy_dates:
            # Try exact match
            if strat_date in benchmark_map:
                aligned.append(
                    {
                        "date": strat_date,
                        "price": benchmark_map[strat_date].price,
                        "change_rate": benchmark_map[strat_date].change_rate,
                    }
                )
            else:
                # Use previous available date (carry forward)
                prev_date = max(
                    (d for d in benchmark_map.keys() if d < strat_date), default=None
                )
                if prev_date:
                    aligned.append(
                        {
                            "date": strat_date,
                            "price": benchmark_map[prev_date].price,
                            "change_rate": benchmark_map[prev_date].change_rate,
                        }
                    )
                else:
                    # Use next available date (if no previous)
                    next_date = min(
                        (d for d in benchmark_map.keys() if d > strat_date),
                        default=None,
                    )
                    if next_date:
                        aligned.append(
                            {
                                "date": strat_date,
                                "price": benchmark_map[next_date].price,
                                "change_rate": benchmark_map[next_date].change_rate,
                            }
                        )

        return aligned

    @staticmethod
    def get_available_benchmarks() -> Dict[str, str]:
        """
        Get list of available benchmark indices

        Returns:
            Dictionary of benchmark names to codes
        """
        return BenchmarkIntegration.BENCHMARKS.copy()

    @staticmethod
    def fetch_benchmark_data(
        benchmark_code: str,
        start_date: date,
        end_date: date,
        session: Session,
    ) -> List[Dict]:
        """
        Fetch benchmark index data for a date range

        Args:
            benchmark_code: Benchmark index code
            start_date: Start date
            end_date: End date
            session: Database session

        Returns:
            List of benchmark data dictionaries
        """
        stmt = (
            select(CollectionDaily)
            .where(
                CollectionDaily.code == benchmark_code,
                CollectionDaily.trade_day >= start_date,
                CollectionDaily.trade_day <= end_date,
            )
            .order_by(CollectionDaily.trade_day)
        )

        results = session.execute(stmt).scalars().all()

        return [
            {
                "date": r.trade_day.isoformat(),
                "price": float(r.price),
                "change": float(r.change) if r.change else 0.0,
                "change_rate": float(r.change_rate) if r.change_rate else 0.0,
                "volume": float(r.capital) if r.capital else 0.0,
            }
            for r in results
        ]
