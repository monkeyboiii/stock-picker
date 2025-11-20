"""API endpoints for portfolio optimization"""

from typing import Dict, List

from fastapi import APIRouter, HTTPException
from loguru import logger
from pydantic import BaseModel, Field

from app.core.optimization import (
    calculate_efficient_frontier,
    calculate_portfolio_metrics,
    optimize_portfolio,
)

router = APIRouter()


# Request/Response models
class PortfolioMetricsRequest(BaseModel):
    """Portfolio metrics request"""

    weights: List[float] = Field(..., description="Asset weights (must sum to 1.0)", min_length=1)
    returns: List[List[float]] = Field(..., description="Return series for each asset", min_length=1)


class PortfolioMetricsResponse(BaseModel):
    """Portfolio metrics response"""

    return_value: float = Field(..., alias="return", description="Annualized portfolio return")
    volatility: float = Field(..., description="Annualized portfolio volatility")
    sharpe_ratio: float = Field(..., description="Sharpe ratio")

    model_config = {"populate_by_name": True}


class OptimizePortfolioRequest(BaseModel):
    """Optimize portfolio request"""

    returns: List[List[float]] = Field(..., description="Return series for each asset", min_length=1)
    method: str = Field(
        "max_sharpe", description="Method: max_sharpe, min_volatility, target_return"
    )
    target_return: float | None = Field(None, description="Target return (for target_return method)")
    allow_short: bool = Field(False, description="Allow short selling (negative weights)")


class OptimizePortfolioResponse(BaseModel):
    """Optimize portfolio response"""

    weights: List[float] = Field(..., description="Optimal asset weights")
    metrics: PortfolioMetricsResponse = Field(..., description="Portfolio metrics")


class EfficientFrontierRequest(BaseModel):
    """Efficient frontier request"""

    returns: List[List[float]] = Field(..., description="Return series for each asset", min_length=1)
    num_points: int = Field(50, description="Number of points on frontier", ge=10, le=200)
    allow_short: bool = Field(False, description="Allow short selling")


class FrontierPoint(BaseModel):
    """A point on the efficient frontier"""

    return_value: float = Field(..., alias="return", description="Portfolio return")
    volatility: float = Field(..., description="Portfolio volatility")
    sharpe_ratio: float = Field(..., description="Sharpe ratio")

    model_config = {"populate_by_name": True}


class MaxSharpePortfolio(BaseModel):
    """Maximum Sharpe ratio portfolio"""

    weights: List[float] = Field(..., description="Optimal weights")
    metrics: PortfolioMetricsResponse = Field(..., description="Portfolio metrics")


class EfficientFrontierResponse(BaseModel):
    """Efficient frontier response"""

    frontier: List[FrontierPoint] = Field(..., description="Points on the efficient frontier")
    max_sharpe_portfolio: MaxSharpePortfolio = Field(..., description="Maximum Sharpe portfolio")


# API endpoints
@router.post("/metrics", response_model=PortfolioMetricsResponse)
async def portfolio_metrics(request: PortfolioMetricsRequest):
    """Calculate portfolio return, volatility, and Sharpe ratio"""
    try:
        # Validate weights sum to 1.0
        weights_sum = sum(request.weights)
        if abs(weights_sum - 1.0) > 1e-6:
            raise ValueError(f"Weights must sum to 1.0, got {weights_sum}")

        # Validate dimensions match
        if len(request.weights) != len(request.returns):
            raise ValueError(
                f"Number of weights ({len(request.weights)}) must match number of assets ({len(request.returns)})"
            )

        result = calculate_portfolio_metrics(request.weights, request.returns)
        return PortfolioMetricsResponse(
            return_value=result["return"],
            volatility=result["volatility"],
            sharpe_ratio=result["sharpe_ratio"],
        )
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error calculating portfolio metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/optimize", response_model=OptimizePortfolioResponse)
async def optimize(request: OptimizePortfolioRequest):
    """Optimize portfolio weights"""
    try:
        # Validate target_return for target_return method
        if request.method == "target_return" and request.target_return is None:
            raise ValueError("target_return is required for target_return method")

        result = optimize_portfolio(
            returns=request.returns,
            method=request.method,
            target_return=request.target_return,
            allow_short=request.allow_short,
        )

        return OptimizePortfolioResponse(
            weights=result["weights"],
            metrics=PortfolioMetricsResponse(
                return_value=result["metrics"]["return"],
                volatility=result["metrics"]["volatility"],
                sharpe_ratio=result["metrics"]["sharpe_ratio"],
            ),
        )
    except ValueError as e:
        logger.error(f"Validation/optimization error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error optimizing portfolio: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/efficient-frontier", response_model=EfficientFrontierResponse)
async def efficient_frontier(request: EfficientFrontierRequest):
    """Calculate efficient frontier"""
    try:
        result = calculate_efficient_frontier(
            returns=request.returns, num_points=request.num_points, allow_short=request.allow_short
        )

        frontier_points = [
            FrontierPoint(
                return_value=point["return"],
                volatility=point["volatility"],
                sharpe_ratio=point["sharpe_ratio"],
            )
            for point in result["frontier"]
        ]

        max_sharpe = result["max_sharpe_portfolio"]

        return EfficientFrontierResponse(
            frontier=frontier_points,
            max_sharpe_portfolio=MaxSharpePortfolio(
                weights=max_sharpe["weights"],
                metrics=PortfolioMetricsResponse(
                    return_value=max_sharpe["metrics"]["return"],
                    volatility=max_sharpe["metrics"]["volatility"],
                    sharpe_ratio=max_sharpe["metrics"]["sharpe_ratio"],
                ),
            ),
        )
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error calculating efficient frontier: {e}")
        raise HTTPException(status_code=500, detail=str(e))
