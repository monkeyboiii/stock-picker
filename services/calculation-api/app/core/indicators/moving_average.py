"""Moving Average indicators - SMA, EMA, WMA"""

from typing import List

import numpy as np
import pandas as pd


def calculate_ma(prices: List[float], period: int = 20) -> List[float]:
    """
    Calculate Simple Moving Average (SMA)

    Args:
        prices: List of price values
        period: Moving average period (default: 20)

    Returns:
        List of SMA values (NaN for insufficient data points)
    """
    df = pd.DataFrame({"price": prices})
    df["ma"] = df["price"].rolling(window=period).mean()
    return df["ma"].tolist()


def calculate_ema(prices: List[float], period: int = 20) -> List[float]:
    """
    Calculate Exponential Moving Average (EMA)

    Args:
        prices: List of price values
        period: EMA period (default: 20)

    Returns:
        List of EMA values (NaN for insufficient data points)
    """
    df = pd.DataFrame({"price": prices})
    df["ema"] = df["price"].ewm(span=period, adjust=False).mean()
    return df["ema"].tolist()


def calculate_wma(prices: List[float], period: int = 20) -> List[float]:
    """
    Calculate Weighted Moving Average (WMA)

    Args:
        prices: List of price values
        period: WMA period (default: 20)

    Returns:
        List of WMA values (NaN for insufficient data points)
    """
    weights = np.arange(1, period + 1)
    df = pd.DataFrame({"price": prices})

    def wma(x):
        if len(x) < period:
            return np.nan
        return np.dot(x[-period:], weights) / weights.sum()

    df["wma"] = df["price"].rolling(window=period).apply(wma, raw=True)
    return df["wma"].tolist()
