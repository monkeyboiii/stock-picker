"""Monte Carlo simulation for portfolio risk analysis"""

from typing import Dict, List

import numpy as np
from scipy import stats


def run_monte_carlo_simulation(
    initial_value: float,
    mean_return: float,
    std_return: float,
    periods: int = 252,
    simulations: int = 10000,
    random_seed: int | None = None,
) -> Dict[str, any]:
    """
    Run Monte Carlo simulation for portfolio value

    Args:
        initial_value: Initial portfolio value
        mean_return: Expected mean return (daily)
        std_return: Standard deviation of returns (daily)
        periods: Number of periods to simulate (default: 252 days)
        simulations: Number of simulation runs (default: 10,000)
        random_seed: Random seed for reproducibility (optional)

    Returns:
        Dictionary containing:
        - final_values: List of final portfolio values for each simulation
        - percentiles: Dictionary of percentile values (5%, 25%, 50%, 75%, 95%)
        - mean_final_value: Average final portfolio value
        - std_final_value: Standard deviation of final values
        - paths: Sample paths (first 100 simulations for visualization)
    """
    if random_seed is not None:
        np.random.seed(random_seed)

    # Generate random returns using geometric Brownian motion
    dt = 1  # Daily time step
    drift = mean_return - 0.5 * std_return**2
    diffusion = std_return * np.sqrt(dt)

    # Generate random shocks
    random_shocks = np.random.normal(0, 1, size=(simulations, periods))

    # Calculate returns
    returns = drift * dt + diffusion * random_shocks

    # Calculate cumulative returns
    cumulative_returns = np.cumsum(returns, axis=1)

    # Calculate portfolio paths
    portfolio_paths = initial_value * np.exp(cumulative_returns)

    # Extract final values
    final_values = portfolio_paths[:, -1].tolist()

    # Calculate statistics
    percentiles = {
        "5": float(np.percentile(final_values, 5)),
        "25": float(np.percentile(final_values, 25)),
        "50": float(np.percentile(final_values, 50)),
        "75": float(np.percentile(final_values, 75)),
        "95": float(np.percentile(final_values, 95)),
    }

    mean_final_value = float(np.mean(final_values))
    std_final_value = float(np.std(final_values, ddof=1))

    # Get sample paths (first 100) for visualization
    sample_paths = portfolio_paths[:100, :].tolist()

    return {
        "final_values": final_values,
        "percentiles": percentiles,
        "mean_final_value": mean_final_value,
        "std_final_value": std_final_value,
        "sample_paths": sample_paths,
    }
