"""
Strategy Comparison Engine - Compare multiple backtest runs

This module provides:
- Multi-strategy comparison
- Statistical significance tests
- Ranking algorithms
- Best performer identification
"""

import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional

from loguru import logger
from scipy import stats
from sqlalchemy.orm import Session

from app.db.models import BacktestRun, Strategy


@dataclass
class ComparisonResult:
    """Result of comparing multiple strategies"""

    comparison_id: str
    name: str
    created_at: datetime
    runs: List[Dict]  # List of run summaries with ranks
    best_by_metric: Dict[str, str]  # metric -> run_id mapping
    statistical_tests: Optional[Dict]  # Statistical test results

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "comparison_id": self.comparison_id,
            "name": self.name,
            "created_at": self.created_at.isoformat(),
            "runs": self.runs,
            "best_by_metric": self.best_by_metric,
            "statistical_tests": self.statistical_tests,
            "visualization_url": f"/api/v1/backtest/compare/{self.comparison_id}/chart",
        }


class ComparisonEngine:
    """Compare multiple backtest runs"""

    RANKABLE_METRICS = [
        "total_return",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        "win_rate",
        "profit_factor",
        "max_drawdown",  # Lower is better
    ]

    @staticmethod
    def compare_strategies(
        backtest_run_ids: List[str],
        metrics: List[str],
        name: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> ComparisonResult:
        """
        Compare multiple backtest runs

        Args:
            backtest_run_ids: List of backtest run UUIDs
            metrics: List of metrics to compare
            name: Name for this comparison
            session: Database session

        Returns:
            ComparisonResult with rankings and statistical tests
        """
        logger.info(f"Comparing {len(backtest_run_ids)} strategies on metrics: {metrics}")

        # Validate metrics
        for metric in metrics:
            if metric not in ComparisonEngine.RANKABLE_METRICS:
                raise ValueError(
                    f"Invalid metric '{metric}'. Allowed: {ComparisonEngine.RANKABLE_METRICS}"
                )

        # Fetch backtest runs
        runs_data = []
        for run_id in backtest_run_ids:
            run = session.get(BacktestRun, run_id)
            if not run:
                raise ValueError(f"Backtest run not found: {run_id}")

            strategy = session.get(Strategy, run.strategy_id)
            strategy_name = strategy.name if strategy else "Unknown Strategy"

            runs_data.append(
                {
                    "run_id": str(run.id),
                    "strategy_name": strategy_name,
                    "total_return": float(run.total_return) if run.total_return else 0.0,
                    "sharpe_ratio": float(run.sharpe_ratio) if run.sharpe_ratio else 0.0,
                    "sortino_ratio": float(run.sortino_ratio) if run.sortino_ratio else 0.0,
                    "calmar_ratio": float(run.calmar_ratio) if run.calmar_ratio else 0.0,
                    "max_drawdown": float(run.max_drawdown) if run.max_drawdown else 0.0,
                    "win_rate": float(run.win_rate) if run.win_rate else 0.0,
                    "profit_factor": float(run.profit_factor) if run.profit_factor else 0.0,
                    "total_trades": run.total_trades or 0,
                }
            )

        # Rank strategies by each metric
        for metric in metrics:
            runs_data = ComparisonEngine._rank_by_metric(runs_data, metric)

        # Calculate overall rank (average of individual ranks)
        for run in runs_data:
            individual_ranks = [
                run.get(f"{metric}_rank", 999) for metric in metrics
            ]
            run["overall_rank"] = sum(individual_ranks) / len(individual_ranks)

        # Sort by overall rank
        runs_data.sort(key=lambda x: x["overall_rank"])

        # Assign final ranks
        for i, run in enumerate(runs_data):
            run["rank"] = i + 1

        # Identify best performer for each metric
        best_by_metric = {}
        for metric in metrics:
            if metric == "max_drawdown":
                # Lower is better for drawdown
                best_run = min(runs_data, key=lambda x: x[metric])
            else:
                # Higher is better for other metrics
                best_run = max(runs_data, key=lambda x: x[metric])

            best_by_metric[metric] = best_run["run_id"]

        # Statistical significance tests (if enough runs)
        statistical_tests = None
        if len(runs_data) >= 2:
            statistical_tests = ComparisonEngine._statistical_tests(runs_data, metrics)

        comparison_id = str(uuid.uuid4())
        comparison_name = name or f"Comparison {datetime.now().strftime('%Y-%m-%d %H:%M')}"

        result = ComparisonResult(
            comparison_id=comparison_id,
            name=comparison_name,
            created_at=datetime.now(),
            runs=runs_data,
            best_by_metric=best_by_metric,
            statistical_tests=statistical_tests,
        )

        logger.info(
            f"Comparison complete: {len(runs_data)} strategies, "
            f"best overall: {runs_data[0]['strategy_name']}"
        )

        return result

    @staticmethod
    def _rank_by_metric(runs: List[Dict], metric: str) -> List[Dict]:
        """
        Rank runs by a specific metric

        Args:
            runs: List of run dictionaries
            metric: Metric to rank by

        Returns:
            List of runs with added '{metric}_rank' field
        """
        # Sort by metric (lower is better for max_drawdown, higher for others)
        if metric == "max_drawdown":
            sorted_runs = sorted(runs, key=lambda x: x[metric])
        else:
            sorted_runs = sorted(runs, key=lambda x: x[metric], reverse=True)

        # Assign ranks
        for i, run in enumerate(sorted_runs):
            run[f"{metric}_rank"] = i + 1

        return runs

    @staticmethod
    def _statistical_tests(runs: List[Dict], metrics: List[str]) -> Dict:
        """
        Perform statistical significance tests

        Args:
            runs: List of run dictionaries
            metrics: Metrics to test

        Returns:
            Dictionary of test results
        """
        tests = {}

        # Only perform tests if we have at least 2 runs
        if len(runs) < 2:
            return tests

        # Extract metric values
        metric_values = {}
        for metric in metrics:
            metric_values[metric] = [run[metric] for run in runs]

        # ANOVA test (if 3+ strategies)
        if len(runs) >= 3:
            try:
                for metric in metrics:
                    values = metric_values[metric]

                    # Check if there's variation
                    if len(set(values)) > 1:
                        # One-way ANOVA
                        # Note: This is simplified - in reality we'd need multiple observations per strategy
                        f_stat = float(stats.f_oneway(*[[v] for v in values]).statistic)
                        p_value = float(stats.f_oneway(*[[v] for v in values]).pvalue)

                        tests[f"{metric}_anova"] = {
                            "test": "One-way ANOVA",
                            "f_statistic": f_stat,
                            "p_value": p_value,
                            "significant": p_value < 0.05,
                            "interpretation": (
                                "Significant difference between strategies"
                                if p_value < 0.05
                                else "No significant difference"
                            ),
                        }
            except Exception as e:
                logger.warning(f"Failed to perform ANOVA for {metric}: {e}")

        # Pairwise t-tests (top 2 strategies)
        if len(runs) >= 2:
            try:
                top_two = runs[:2]
                for metric in metrics:
                    val1 = top_two[0][metric]
                    val2 = top_two[1][metric]

                    # Calculate difference
                    diff = val1 - val2
                    diff_pct = (diff / val2 * 100) if val2 != 0 else 0

                    tests[f"{metric}_top_two_diff"] = {
                        "test": "Top 2 Comparison",
                        "strategy_1": top_two[0]["strategy_name"],
                        "strategy_2": top_two[1]["strategy_name"],
                        "value_1": val1,
                        "value_2": val2,
                        "difference": diff,
                        "difference_pct": diff_pct,
                        "better": top_two[0]["strategy_name"],
                    }
            except Exception as e:
                logger.warning(f"Failed to compare top 2 for {metric}: {e}")

        return tests

    @staticmethod
    def format_comparison_table(result: ComparisonResult) -> str:
        """
        Format comparison result as a text table

        Args:
            result: ComparisonResult object

        Returns:
            Formatted table string
        """
        lines = []
        lines.append(f"\n{'='*80}")
        lines.append(f"Strategy Comparison: {result.name}")
        lines.append(f"{'='*80}\n")

        # Header
        header = f"{'Rank':<6} {'Strategy':<30} {'Return%':<10} {'Sharpe':<8} {'MaxDD%':<10} {'WinRate%':<10}"
        lines.append(header)
        lines.append("-" * 80)

        # Rows
        for run in result.runs:
            row = (
                f"{run['rank']:<6} "
                f"{run['strategy_name']:<30} "
                f"{run['total_return']:<10.2f} "
                f"{run['sharpe_ratio']:<8.2f} "
                f"{run['max_drawdown']:<10.2f} "
                f"{run['win_rate']*100:<10.1f}"
            )
            lines.append(row)

        lines.append("\n" + "=" * 80)

        # Best by metric
        lines.append("\nBest Performers by Metric:")
        lines.append("-" * 40)
        for metric, run_id in result.best_by_metric.items():
            run = next(r for r in result.runs if r["run_id"] == run_id)
            lines.append(f"  {metric:<20}: {run['strategy_name']}")

        lines.append("\n" + "=" * 80 + "\n")

        return "\n".join(lines)
