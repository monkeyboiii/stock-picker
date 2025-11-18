"""Momentum indicators - RSI, MACD, Stochastic"""

from typing import Dict, List

import pandas as pd


def calculate_rsi(prices: List[float], period: int = 14) -> List[float]:
    """
    Calculate Relative Strength Index (RSI)

    Args:
        prices: List of price values
        period: RSI period (default: 14)

    Returns:
        List of RSI values (0-100, NaN for insufficient data)
    """
    df = pd.DataFrame({"price": prices})

    # Calculate price changes
    delta = df["price"].diff()

    # Separate gains and losses
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # Calculate average gain and loss
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    # Calculate RS and RSI
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi.tolist()


def calculate_macd(
    prices: List[float], fast_period: int = 12, slow_period: int = 26, signal_period: int = 9
) -> Dict[str, List[float]]:
    """
    Calculate Moving Average Convergence Divergence (MACD)

    Args:
        prices: List of price values
        fast_period: Fast EMA period (default: 12)
        slow_period: Slow EMA period (default: 26)
        signal_period: Signal line EMA period (default: 9)

    Returns:
        Dictionary with 'macd', 'signal', and 'histogram' lists
    """
    df = pd.DataFrame({"price": prices})

    # Calculate fast and slow EMAs
    fast_ema = df["price"].ewm(span=fast_period, adjust=False).mean()
    slow_ema = df["price"].ewm(span=slow_period, adjust=False).mean()

    # Calculate MACD line
    macd = fast_ema - slow_ema

    # Calculate signal line
    signal = macd.ewm(span=signal_period, adjust=False).mean()

    # Calculate histogram
    histogram = macd - signal

    return {
        "macd": macd.tolist(),
        "signal": signal.tolist(),
        "histogram": histogram.tolist(),
    }


def calculate_stochastic(
    high: List[float],
    low: List[float],
    close: List[float],
    k_period: int = 14,
    d_period: int = 3,
) -> Dict[str, List[float]]:
    """
    Calculate Stochastic Oscillator

    Args:
        high: List of high prices
        low: List of low prices
        close: List of close prices
        k_period: %K period (default: 14)
        d_period: %D period (default: 3)

    Returns:
        Dictionary with '%K' and '%D' lists
    """
    df = pd.DataFrame({"high": high, "low": low, "close": close})

    # Calculate %K
    lowest_low = df["low"].rolling(window=k_period).min()
    highest_high = df["high"].rolling(window=k_period).max()

    k = 100 * (df["close"] - lowest_low) / (highest_high - lowest_low)

    # Calculate %D (SMA of %K)
    d = k.rolling(window=d_period).mean()

    return {
        "k": k.tolist(),
        "d": d.tolist(),
    }
