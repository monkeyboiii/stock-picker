"""API endpoints for risk analytics"""

from typing import Dict, List

from fastapi import APIRouter, HTTPException
from loguru import logger
from pydantic import BaseModel, Field

from app.core.risk import (
    calculate_calmar_ratio,
    calculate_cvar,
    calculate_max_drawdown,
    calculate_sharpe_ratio,
    calculate_sortino_ratio,
    calculate_var,
    run_monte_carlo_simulation,
)

router = APIRouter()


# Request/Response models
class SharpeRequest(BaseModel):
    """Sharpe ratio request"""

    returns: List[float] = Field(..., description="Period returns", min_length=2)
    risk_free_rate: float = Field(0.0, description="Annual risk-free rate")


class SharpeResponse(BaseModel):
    """Sharpe ratio response"""

    sharpe_ratio: float = Field(..., description="Sharpe ratio value")


class SortinoRequest(BaseModel):
    """Sortino ratio request"""

    returns: List[float] = Field(..., description="Period returns", min_length=2)
    risk_free_rate: float = Field(0.0, description="Annual risk-free rate")
    target_return: float = Field(0.0, description="Target return")


class SortinoResponse(BaseModel):
    """Sortino ratio response"""

    sortino_ratio: float = Field(..., description="Sortino ratio value")


class MaxDrawdownRequest(BaseModel):
    """Max drawdown request"""

    equity_curve: List[float] = Field(..., description="Portfolio values over time", min_length=2)


class MaxDrawdownResponse(BaseModel):
    """Max drawdown response"""

    max_drawdown: float = Field(..., description="Maximum drawdown (negative percentage)")


class CalmarRequest(BaseModel):
    """Calmar ratio request"""

    returns: List[float] = Field(..., description="Period returns", min_length=2)
    equity_curve: List[float] = Field(..., description="Portfolio values over time", min_length=2)


class CalmarResponse(BaseModel):
    """Calmar ratio response"""

    calmar_ratio: float = Field(..., description="Calmar ratio value")


class VaRRequest(BaseModel):
    """Value at Risk request"""

    returns: List[float] = Field(..., description="Period returns", min_length=2)
    confidence_level: float = Field(0.95, description="Confidence level (0-1)", ge=0.0, le=1.0)
    method: str = Field(
        "historical", description="Calculation method: historical, parametric, cornish_fisher"
    )


class VaRResponse(BaseModel):
    """Value at Risk response"""

    var: float = Field(..., description="VaR value (negative = potential loss)")


class CVaRRequest(BaseModel):
    """Conditional VaR request"""

    returns: List[float] = Field(..., description="Period returns", min_length=2)
    confidence_level: float = Field(0.95, description="Confidence level (0-1)", ge=0.0, le=1.0)
    method: str = Field("historical", description="Calculation method: historical, parametric")


class CVaRResponse(BaseModel):
    """Conditional VaR response"""

    cvar: float = Field(..., description="CVaR value (expected shortfall)")


class MonteCarloRequest(BaseModel):
    """Monte Carlo simulation request"""

    initial_value: float = Field(..., description="Initial portfolio value", gt=0)
    mean_return: float = Field(..., description="Expected mean daily return")
    std_return: float = Field(..., description="Standard deviation of daily returns", gt=0)
    periods: int = Field(252, description="Number of periods to simulate", ge=1)
    simulations: int = Field(10000, description="Number of simulations", ge=1, le=100000)
    random_seed: int | None = Field(None, description="Random seed for reproducibility")


class MonteCarloResponse(BaseModel):
    """Monte Carlo simulation response"""

    final_values: List[float] = Field(..., description="Final values for each simulation")
    percentiles: Dict[str, float] = Field(..., description="Percentile values")
    mean_final_value: float = Field(..., description="Average final value")
    std_final_value: float = Field(..., description="Standard deviation of final values")
    sample_paths: List[List[float]] = Field(..., description="Sample simulation paths")


# API endpoints
@router.post("/sharpe", response_model=SharpeResponse)
async def sharpe_ratio(request: SharpeRequest):
    """Calculate Sharpe Ratio"""
    try:
        ratio = calculate_sharpe_ratio(request.returns, request.risk_free_rate)
        return SharpeResponse(sharpe_ratio=ratio)
    except Exception as e:
        logger.error(f"Error calculating Sharpe ratio: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sortino", response_model=SortinoResponse)
async def sortino_ratio(request: SortinoRequest):
    """Calculate Sortino Ratio"""
    try:
        ratio = calculate_sortino_ratio(
            request.returns, request.risk_free_rate, request.target_return
        )
        return SortinoResponse(sortino_ratio=ratio)
    except Exception as e:
        logger.error(f"Error calculating Sortino ratio: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/max-drawdown", response_model=MaxDrawdownResponse)
async def max_drawdown(request: MaxDrawdownRequest):
    """Calculate Maximum Drawdown"""
    try:
        mdd = calculate_max_drawdown(request.equity_curve)
        return MaxDrawdownResponse(max_drawdown=mdd)
    except Exception as e:
        logger.error(f"Error calculating max drawdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/calmar", response_model=CalmarResponse)
async def calmar_ratio(request: CalmarRequest):
    """Calculate Calmar Ratio"""
    try:
        ratio = calculate_calmar_ratio(request.returns, request.equity_curve)
        return CalmarResponse(calmar_ratio=ratio)
    except Exception as e:
        logger.error(f"Error calculating Calmar ratio: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/var", response_model=VaRResponse)
async def value_at_risk(request: VaRRequest):
    """Calculate Value at Risk (VaR)"""
    try:
        var = calculate_var(request.returns, request.confidence_level, request.method)
        return VaRResponse(var=var)
    except Exception as e:
        logger.error(f"Error calculating VaR: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cvar", response_model=CVaRResponse)
async def conditional_var(request: CVaRRequest):
    """Calculate Conditional Value at Risk (CVaR)"""
    try:
        cvar = calculate_cvar(request.returns, request.confidence_level, request.method)
        return CVaRResponse(cvar=cvar)
    except Exception as e:
        logger.error(f"Error calculating CVaR: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/monte-carlo", response_model=MonteCarloResponse)
async def monte_carlo(request: MonteCarloRequest):
    """Run Monte Carlo simulation for portfolio risk analysis"""
    try:
        result = run_monte_carlo_simulation(
            initial_value=request.initial_value,
            mean_return=request.mean_return,
            std_return=request.std_return,
            periods=request.periods,
            simulations=request.simulations,
            random_seed=request.random_seed,
        )
        return MonteCarloResponse(**result)
    except Exception as e:
        logger.error(f"Error running Monte Carlo simulation: {e}")
        raise HTTPException(status_code=500, detail=str(e))
