"""Volatility indicators - Bollinger Bands, ATR"""

from typing import Dict, List

import pandas as pd


def calculate_bollinger_bands(
    prices: List[float], period: int = 20, std_dev: float = 2.0
) -> Dict[str, List[float]]:
    """
    Calculate Bollinger Bands

    Args:
        prices: List of price values
        period: Moving average period (default: 20)
        std_dev: Number of standard deviations (default: 2.0)

    Returns:
        Dictionary with 'upper', 'middle', and 'lower' bands
    """
    df = pd.DataFrame({"price": prices})

    # Calculate middle band (SMA)
    middle = df["price"].rolling(window=period).mean()

    # Calculate standard deviation
    std = df["price"].rolling(window=period).std()

    # Calculate upper and lower bands
    upper = middle + (std * std_dev)
    lower = middle - (std * std_dev)

    return {
        "upper": upper.tolist(),
        "middle": middle.tolist(),
        "lower": lower.tolist(),
    }


def calculate_atr(
    high: List[float], low: List[float], close: List[float], period: int = 14
) -> List[float]:
    """
    Calculate Average True Range (ATR)

    Args:
        high: List of high prices
        low: List of low prices
        close: List of close prices
        period: ATR period (default: 14)

    Returns:
        List of ATR values (NaN for insufficient data)
    """
    df = pd.DataFrame({"high": high, "low": low, "close": close})

    # Calculate True Range components
    df["h_l"] = df["high"] - df["low"]
    df["h_pc"] = abs(df["high"] - df["close"].shift(1))
    df["l_pc"] = abs(df["low"] - df["close"].shift(1))

    # True Range is the maximum of the three
    df["tr"] = df[["h_l", "h_pc", "l_pc"]].max(axis=1)

    # Calculate ATR (EMA of True Range)
    atr = df["tr"].ewm(span=period, adjust=False).mean()

    return atr.tolist()
