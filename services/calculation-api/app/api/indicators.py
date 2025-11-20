"""API endpoints for technical indicators"""

import math
from typing import List

from fastapi import APIRouter, HTTPException
from loguru import logger
from pydantic import BaseModel, Field

from app.core.indicators import (
    calculate_atr,
    calculate_bollinger_bands,
    calculate_ema,
    calculate_ma,
    calculate_macd,
    calculate_rsi,
    calculate_stochastic,
)

router = APIRouter()


def clean_nan(values: List[float]) -> List[float | None]:
    """Convert NaN values to None for JSON serialization"""
    return [None if (isinstance(v, float) and math.isnan(v)) else v for v in values]


def clean_dict_nan(data: dict) -> dict:
    """Convert NaN values to None in dict for JSON serialization"""
    return {k: clean_nan(v) if isinstance(v, list) else v for k, v in data.items()}


# Request/Response models
class MARequest(BaseModel):
    """Moving Average request"""

    prices: List[float] = Field(..., description="List of price values", min_length=1)
    period: int = Field(20, description="Moving average period", ge=1)


class MAResponse(BaseModel):
    """Moving Average response"""

    values: List[float | None] = Field(..., description="Moving average values")


class RSIRequest(BaseModel):
    """RSI request"""

    prices: List[float] = Field(..., description="List of price values", min_length=2)
    period: int = Field(14, description="RSI period", ge=1)


class RSIResponse(BaseModel):
    """RSI response"""

    values: List[float | None] = Field(..., description="RSI values (0-100)")


class MACDRequest(BaseModel):
    """MACD request"""

    prices: List[float] = Field(..., description="List of price values", min_length=2)
    fast_period: int = Field(12, description="Fast EMA period", ge=1)
    slow_period: int = Field(26, description="Slow EMA period", ge=1)
    signal_period: int = Field(9, description="Signal line period", ge=1)


class MACDResponse(BaseModel):
    """MACD response"""

    macd: List[float | None] = Field(..., description="MACD line values")
    signal: List[float | None] = Field(..., description="Signal line values")
    histogram: List[float | None] = Field(..., description="Histogram values")


class BollingerBandsResponse(BaseModel):
    """Bollinger Bands response"""

    upper: List[float | None] = Field(..., description="Upper band values")
    middle: List[float | None] = Field(..., description="Middle band values")
    lower: List[float | None] = Field(..., description="Lower band values")


class BollingerBandsRequest(BaseModel):
    """Bollinger Bands request"""

    prices: List[float] = Field(..., description="List of price values", min_length=2)
    period: int = Field(20, description="Moving average period", ge=1)
    std_dev: float = Field(2.0, description="Standard deviations", ge=0.1)


class StochasticRequest(BaseModel):
    """Stochastic Oscillator request"""

    high: List[float] = Field(..., description="High prices", min_length=2)
    low: List[float] = Field(..., description="Low prices", min_length=2)
    close: List[float] = Field(..., description="Close prices", min_length=2)
    k_period: int = Field(14, description="%K period", ge=1)
    d_period: int = Field(3, description="%D period", ge=1)


class StochasticResponse(BaseModel):
    """Stochastic Oscillator response"""

    k: List[float | None] = Field(..., description="%K values")
    d: List[float | None] = Field(..., description="%D values")


class ATRRequest(BaseModel):
    """ATR request"""

    high: List[float] = Field(..., description="High prices", min_length=2)
    low: List[float] = Field(..., description="Low prices", min_length=2)
    close: List[float] = Field(..., description="Close prices", min_length=2)
    period: int = Field(14, description="ATR period", ge=1)


class ATRResponse(BaseModel):
    """ATR response"""

    values: List[float | None] = Field(..., description="ATR values")


# API endpoints
@router.post("/ma", response_model=MAResponse)
async def moving_average(request: MARequest):
    """Calculate Simple Moving Average (SMA)"""
    try:
        values = calculate_ma(request.prices, request.period)
        return MAResponse(values=clean_nan(values))
    except Exception as e:
        logger.error(f"Error calculating MA: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ema", response_model=MAResponse)
async def exponential_moving_average(request: MARequest):
    """Calculate Exponential Moving Average (EMA)"""
    try:
        values = calculate_ema(request.prices, request.period)
        return MAResponse(values=clean_nan(values))
    except Exception as e:
        logger.error(f"Error calculating EMA: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rsi", response_model=RSIResponse)
async def relative_strength_index(request: RSIRequest):
    """Calculate Relative Strength Index (RSI)"""
    try:
        values = calculate_rsi(request.prices, request.period)
        return RSIResponse(values=clean_nan(values))
    except Exception as e:
        logger.error(f"Error calculating RSI: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/macd", response_model=MACDResponse)
async def macd(request: MACDRequest):
    """Calculate Moving Average Convergence Divergence (MACD)"""
    try:
        result = calculate_macd(
            request.prices, request.fast_period, request.slow_period, request.signal_period
        )
        return MACDResponse(**clean_dict_nan(result))
    except Exception as e:
        logger.error(f"Error calculating MACD: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bollinger", response_model=BollingerBandsResponse)
async def bollinger_bands(request: BollingerBandsRequest):
    """Calculate Bollinger Bands"""
    try:
        result = calculate_bollinger_bands(request.prices, request.period, request.std_dev)
        return BollingerBandsResponse(**clean_dict_nan(result))
    except Exception as e:
        logger.error(f"Error calculating Bollinger Bands: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stochastic", response_model=StochasticResponse)
async def stochastic(request: StochasticRequest):
    """Calculate Stochastic Oscillator"""
    try:
        result = calculate_stochastic(
            request.high, request.low, request.close, request.k_period, request.d_period
        )
        return StochasticResponse(**clean_dict_nan(result))
    except Exception as e:
        logger.error(f"Error calculating Stochastic: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/atr", response_model=ATRResponse)
async def average_true_range(request: ATRRequest):
    """Calculate Average True Range (ATR)"""
    try:
        values = calculate_atr(request.high, request.low, request.close, request.period)
        return ATRResponse(values=clean_nan(values))
    except Exception as e:
        logger.error(f"Error calculating ATR: {e}")
        raise HTTPException(status_code=500, detail=str(e))
