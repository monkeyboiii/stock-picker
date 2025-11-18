"""Value at Risk (VaR) and Conditional VaR (CVaR) calculations"""

from typing import List

import numpy as np
from scipy import stats


def calculate_var(
    returns: List[float], confidence_level: float = 0.95, method: str = "historical"
) -> float:
    """
    Calculate Value at Risk (VaR)

    Args:
        returns: List of period returns
        confidence_level: Confidence level (default: 0.95 for 95%)
        method: Calculation method - 'historical', 'parametric', or 'cornish_fisher'

    Returns:
        VaR value (negative number representing potential loss)
    """
    if not returns or len(returns) < 2:
        return 0.0

    returns_array = np.array(returns)

    if method == "historical":
        # Historical VaR: Use empirical percentile
        var = np.percentile(returns_array, (1 - confidence_level) * 100)

    elif method == "parametric":
        # Parametric VaR: Assume normal distribution
        mean = np.mean(returns_array)
        std = np.std(returns_array, ddof=1)
        var = stats.norm.ppf(1 - confidence_level, mean, std)

    elif method == "cornish_fisher":
        # Cornish-Fisher VaR: Adjust for skewness and kurtosis
        mean = np.mean(returns_array)
        std = np.std(returns_array, ddof=1)
        skew = stats.skew(returns_array)
        kurt = stats.kurtosis(returns_array)

        # Standard normal quantile
        z = stats.norm.ppf(1 - confidence_level)

        # Cornish-Fisher expansion
        z_cf = (
            z
            + (z**2 - 1) * skew / 6
            + (z**3 - 3 * z) * kurt / 24
            - (2 * z**3 - 5 * z) * skew**2 / 36
        )

        var = mean + z_cf * std

    else:
        raise ValueError(f"Unknown VaR method: {method}")

    return float(var)


def calculate_cvar(
    returns: List[float], confidence_level: float = 0.95, method: str = "historical"
) -> float:
    """
    Calculate Conditional Value at Risk (CVaR) / Expected Shortfall

    Args:
        returns: List of period returns
        confidence_level: Confidence level (default: 0.95)
        method: Calculation method - 'historical' or 'parametric'

    Returns:
        CVaR value (average loss beyond VaR)
    """
    if not returns or len(returns) < 2:
        return 0.0

    returns_array = np.array(returns)

    if method == "historical":
        # Historical CVaR: Average of all returns below VaR
        var = calculate_var(returns, confidence_level, method="historical")
        cvar = np.mean(returns_array[returns_array <= var])

    elif method == "parametric":
        # Parametric CVaR: Conditional expectation under normal distribution
        mean = np.mean(returns_array)
        std = np.std(returns_array, ddof=1)

        # Standard normal quantile
        z = stats.norm.ppf(1 - confidence_level)

        # Conditional expectation
        cvar = mean - std * stats.norm.pdf(z) / (1 - confidence_level)

    else:
        raise ValueError(f"Unknown CVaR method: {method}")

    return float(cvar)
