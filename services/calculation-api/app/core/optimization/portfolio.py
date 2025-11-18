"""Portfolio optimization - Mean-variance optimization, efficient frontier"""

from typing import Dict, List

import numpy as np
import pandas as pd
from scipy.optimize import minimize


def calculate_portfolio_metrics(
    weights: List[float], returns: List[List[float]]
) -> Dict[str, float]:
    """
    Calculate portfolio return, volatility, and Sharpe ratio

    Args:
        weights: Asset weights (must sum to 1.0)
        returns: List of return series for each asset

    Returns:
        Dictionary with 'return', 'volatility', and 'sharpe_ratio'
    """
    # Convert to numpy arrays
    weights_array = np.array(weights)
    returns_df = pd.DataFrame(returns).T

    # Calculate mean returns and covariance matrix
    mean_returns = returns_df.mean().values
    cov_matrix = returns_df.cov().values

    # Portfolio return (annualized, assuming daily returns)
    portfolio_return = float(np.dot(weights_array, mean_returns) * 252)

    # Portfolio volatility (annualized)
    portfolio_variance = np.dot(weights_array.T, np.dot(cov_matrix, weights_array))
    portfolio_volatility = float(np.sqrt(portfolio_variance) * np.sqrt(252))

    # Sharpe ratio (assuming risk-free rate = 0)
    sharpe_ratio = (
        float(portfolio_return / portfolio_volatility) if portfolio_volatility > 0 else 0.0
    )

    return {
        "return": portfolio_return,
        "volatility": portfolio_volatility,
        "sharpe_ratio": sharpe_ratio,
    }


def optimize_portfolio(
    returns: List[List[float]],
    method: str = "max_sharpe",
    target_return: float | None = None,
    allow_short: bool = False,
) -> Dict[str, any]:
    """
    Optimize portfolio weights

    Args:
        returns: List of return series for each asset
        method: Optimization method - 'max_sharpe', 'min_volatility', 'target_return'
        target_return: Target return (required if method='target_return')
        allow_short: Allow short selling (negative weights)

    Returns:
        Dictionary with optimal 'weights' and portfolio 'metrics'
    """
    returns_df = pd.DataFrame(returns).T
    n_assets = len(returns)

    # Calculate mean returns and covariance matrix
    mean_returns = returns_df.mean().values
    cov_matrix = returns_df.cov().values

    # Objective functions
    def portfolio_return(weights):
        return np.dot(weights, mean_returns) * 252

    def portfolio_volatility(weights):
        return np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights))) * np.sqrt(252)

    def negative_sharpe(weights):
        ret = portfolio_return(weights)
        vol = portfolio_volatility(weights)
        return -ret / vol if vol > 0 else 0

    # Constraints and bounds
    constraints = [{"type": "eq", "fun": lambda x: np.sum(x) - 1}]  # Weights sum to 1

    if method == "target_return" and target_return is not None:
        constraints.append({"type": "eq", "fun": lambda x: portfolio_return(x) - target_return})

    if allow_short:
        bounds = tuple((-1, 1) for _ in range(n_assets))
    else:
        bounds = tuple((0, 1) for _ in range(n_assets))

    # Initial guess (equal weights)
    x0 = np.array([1.0 / n_assets] * n_assets)

    # Optimize
    if method == "max_sharpe":
        result = minimize(negative_sharpe, x0, method="SLSQP", bounds=bounds, constraints=constraints)
    elif method == "min_volatility":
        result = minimize(
            portfolio_volatility, x0, method="SLSQP", bounds=bounds, constraints=constraints
        )
    elif method == "target_return":
        result = minimize(
            portfolio_volatility, x0, method="SLSQP", bounds=bounds, constraints=constraints
        )
    else:
        raise ValueError(f"Unknown optimization method: {method}")

    if not result.success:
        raise ValueError(f"Optimization failed: {result.message}")

    optimal_weights = result.x.tolist()
    metrics = calculate_portfolio_metrics(optimal_weights, returns)

    return {
        "weights": optimal_weights,
        "metrics": metrics,
    }


def calculate_efficient_frontier(
    returns: List[List[float]], num_points: int = 50, allow_short: bool = False
) -> Dict[str, any]:
    """
    Calculate efficient frontier

    Args:
        returns: List of return series for each asset
        num_points: Number of points on the frontier
        allow_short: Allow short selling

    Returns:
        Dictionary with 'frontier' (list of portfolio metrics) and 'max_sharpe' portfolio
    """
    returns_df = pd.DataFrame(returns).T

    # Calculate range of returns
    mean_returns = returns_df.mean().values
    min_return = float(np.min(mean_returns) * 252)
    max_return = float(np.max(mean_returns) * 252)

    # Generate target returns
    target_returns = np.linspace(min_return, max_return, num_points)

    # Calculate frontier points
    frontier_portfolios = []

    for target_return in target_returns:
        try:
            result = optimize_portfolio(
                returns, method="target_return", target_return=target_return, allow_short=allow_short
            )
            frontier_portfolios.append(
                {
                    "return": result["metrics"]["return"],
                    "volatility": result["metrics"]["volatility"],
                    "sharpe_ratio": result["metrics"]["sharpe_ratio"],
                }
            )
        except ValueError:
            # Skip if optimization fails for this target return
            continue

    # Calculate max Sharpe portfolio
    max_sharpe_portfolio = optimize_portfolio(returns, method="max_sharpe", allow_short=allow_short)

    return {
        "frontier": frontier_portfolios,
        "max_sharpe_portfolio": {
            "weights": max_sharpe_portfolio["weights"],
            "metrics": max_sharpe_portfolio["metrics"],
        },
    }
