"""Risk metrics - Sharpe, Sortino, Calmar, Max Drawdown"""

from typing import List

import numpy as np
import pandas as pd


def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.0) -> float:
    """
    Calculate Sharpe Ratio

    Args:
        returns: List of period returns (e.g., daily returns)
        risk_free_rate: Risk-free rate (annualized, default: 0.0)

    Returns:
        Sharpe ratio value
    """
    if not returns or len(returns) < 2:
        return 0.0

    returns_array = np.array(returns)
    excess_returns = returns_array - (risk_free_rate / 252)  # Assuming daily returns

    mean_excess_return = np.mean(excess_returns)
    std_excess_return = np.std(excess_returns, ddof=1)

    if std_excess_return == 0:
        return 0.0

    # Annualize (assuming 252 trading days)
    sharpe = (mean_excess_return / std_excess_return) * np.sqrt(252)

    return float(sharpe)


def calculate_sortino_ratio(
    returns: List[float], risk_free_rate: float = 0.0, target_return: float = 0.0
) -> float:
    """
    Calculate Sortino Ratio (uses downside deviation instead of standard deviation)

    Args:
        returns: List of period returns
        risk_free_rate: Risk-free rate (annualized, default: 0.0)
        target_return: Target return (default: 0.0)

    Returns:
        Sortino ratio value
    """
    if not returns or len(returns) < 2:
        return 0.0

    returns_array = np.array(returns)
    excess_returns = returns_array - (risk_free_rate / 252)

    # Calculate downside deviation (only negative returns)
    downside_returns = excess_returns[excess_returns < target_return]

    if len(downside_returns) < 2:
        return 0.0

    downside_std = np.std(downside_returns, ddof=1)

    if downside_std == 0 or np.isnan(downside_std):
        return 0.0

    mean_excess_return = np.mean(excess_returns)

    # Annualize
    sortino = (mean_excess_return / downside_std) * np.sqrt(252)

    # Handle NaN result
    if np.isnan(sortino):
        return 0.0

    return float(sortino)


def calculate_max_drawdown(equity_curve: List[float]) -> float:
    """
    Calculate Maximum Drawdown

    Args:
        equity_curve: List of portfolio values over time

    Returns:
        Maximum drawdown as a percentage (negative value)
    """
    if not equity_curve or len(equity_curve) < 2:
        return 0.0

    equity = pd.Series(equity_curve)

    # Calculate running maximum
    running_max = equity.expanding().max()

    # Calculate drawdown
    drawdown = (equity - running_max) / running_max

    # Return maximum drawdown (most negative value)
    max_dd = drawdown.min()

    return float(max_dd)


def calculate_calmar_ratio(returns: List[float], equity_curve: List[float]) -> float:
    """
    Calculate Calmar Ratio (annualized return / max drawdown)

    Args:
        returns: List of period returns
        equity_curve: List of portfolio values over time

    Returns:
        Calmar ratio value
    """
    if not returns or not equity_curve or len(returns) < 2:
        return 0.0

    # Calculate annualized return
    total_return = (equity_curve[-1] - equity_curve[0]) / equity_curve[0]
    periods = len(returns)
    annualized_return = (1 + total_return) ** (252 / periods) - 1

    # Calculate max drawdown
    max_dd = calculate_max_drawdown(equity_curve)

    if max_dd == 0:
        return 0.0

    calmar = annualized_return / abs(max_dd)

    return float(calmar)
