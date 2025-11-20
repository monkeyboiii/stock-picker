"""
Backtest API Routes

Endpoints for creating and managing backtest runs.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path, Query, status
from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.dependencies import get_db, validate_pagination
from app.api.models import (
    BacktestRunDetail,
    BacktestRunRequest,
    BacktestRunsResponse,
    BacktestRunSummary,
    PortfolioSnapshotResponse,
    TradesResponse,
    TradeResponse,
)
from app.api.charts import ChartDataGenerator
from app.backtest.benchmark import BenchmarkIntegration
from app.backtest.comparison import ComparisonEngine
from app.backtest.engine import BacktestEngine
from app.backtest.strategy import get_strategy
from app.cache.redis_cache import get_cache, cache_key
from app.db.models import BacktestRun, ComparisonBacktestRun, PortfolioSnapshot, Strategy, StrategyComparison, Trade
from app.strategy import parse_strategy, validate_strategy

router = APIRouter()

# Initialize cache
cache = get_cache()


def run_backtest_background(
    run_id: str,
    strategy_def,
    start_date: date,
    end_date: date,
    initial_capital: Decimal,
    commission_rate: Decimal,
    slippage_rate: Decimal,
    max_positions: int,
):
    """
    Background task to execute backtest

    Args:
        run_id: Backtest run ID
        strategy_def: Strategy definition
        start_date: Start date
        end_date: End date
        initial_capital: Initial capital
        commission_rate: Commission rate
        slippage_rate: Slippage rate
        max_positions: Max positions
    """
    from app.db.engine import engine_from_env

    engine = engine_from_env()

    try:
        logger.info(f"Starting backtest {run_id} in background")

        # Update status to running
        with Session(engine) as session:
            run = session.get(BacktestRun, run_id)
            if run:
                run.status = "running"
                run.started_at = datetime.now()
                session.commit()

        # Create and run backtest
        backtest_engine = BacktestEngine(
            strategy_definition=strategy_def,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            commission_rate=commission_rate,
            slippage_rate=slippage_rate,
            max_positions=max_positions,
        )

        result = backtest_engine.run(engine)

        # Save results
        strategy_id = None
        with Session(engine) as session:
            run = session.get(BacktestRun, run_id)
            if run:
                strategy_id = run.strategy_id

        if strategy_id:
            backtest_engine.save_to_database(engine, strategy_id, result)

        # Invalidate cache for this run (status changed to completed)
        cache.invalidate_pattern(f"backtest:run:{run_id}*")
        cache.invalidate_pattern(f"backtest:results:{run_id}*")

        logger.success(f"Backtest {run_id} completed successfully")

    except Exception as e:
        logger.error(f"Backtest {run_id} failed: {e}")

        # Update status to failed
        with Session(engine) as session:
            run = session.get(BacktestRun, run_id)
            if run:
                run.status = "failed"
                run.error_message = str(e)
                run.completed_at = datetime.now()
                session.commit()

        # Invalidate cache for this run (status changed to failed)
        cache.invalidate_pattern(f"backtest:run:{run_id}*")
        cache.invalidate_pattern(f"backtest:results:{run_id}*")


@router.post("/backtest/run", response_model=BacktestRunSummary, status_code=status.HTTP_202_ACCEPTED)
async def create_backtest_run(
    request: BacktestRunRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Create and queue a new backtest run

    The backtest will execute asynchronously in the background.
    Check the status using GET /backtest/runs/{run_id}

    Returns:
        BacktestRunSummary with status "pending"
    """

    # Get or create strategy
    if request.strategy_id:
        # Use existing strategy
        strategy = get_strategy(db.get_bind(), strategy_id=request.strategy_id)
        if not strategy:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Strategy not found: {request.strategy_id}"
            )
        strategy_def = parse_strategy(strategy.definition)
        strategy_id = strategy.id

    elif request.strategy_file_content:
        # Parse strategy from content
        try:
            strategy_def = parse_strategy(request.strategy_file_content)
            validate_strategy(strategy_def)

            # Create strategy in database
            from app.backtest.strategy import create_strategy
            strategy_id = create_strategy(
                engine=db.get_bind(),
                name=strategy_def.name,
                version=strategy_def.version,
                definition=strategy_def.model_dump(),
                description=strategy_def.description,
                author=strategy_def.author,
            )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid strategy: {e}"
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Either strategy_id or strategy_file_content must be provided"
        )

    # Create backtest run record
    backtest_run = BacktestRun(
        strategy_id=strategy_id,
        start_date=request.start_date,
        end_date=request.end_date,
        initial_capital=Decimal(str(request.initial_capital)),
        commission_rate=Decimal(str(request.commission_rate)),
        slippage_rate=Decimal(str(request.slippage_rate)),
        status="pending",
    )

    db.add(backtest_run)
    db.commit()
    db.refresh(backtest_run)

    # Queue background task
    background_tasks.add_task(
        run_backtest_background,
        run_id=backtest_run.id,
        strategy_def=strategy_def,
        start_date=request.start_date,
        end_date=request.end_date,
        initial_capital=Decimal(str(request.initial_capital)),
        commission_rate=Decimal(str(request.commission_rate)),
        slippage_rate=Decimal(str(request.slippage_rate)),
        max_positions=request.max_positions,
    )

    logger.info(f"Queued backtest run: {backtest_run.id}")

    return BacktestRunSummary.model_validate(backtest_run)


@router.get("/backtest/runs", response_model=BacktestRunsResponse)
async def list_backtest_runs(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    status_filter: Optional[str] = Query(None, description="Filter by status"),
    db: Session = Depends(get_db)
):
    """
    List all backtest runs with pagination

    Supports filtering by status (pending, running, completed, failed)
    """
    pagination = validate_pagination(page, page_size)

    # Build query
    query = select(BacktestRun).order_by(BacktestRun.created_at.desc())

    if status_filter:
        query = query.where(BacktestRun.status == status_filter)

    # Get total count
    total = db.execute(select(func.count()).select_from(query.subquery())).scalar()

    # Get paginated results
    query = query.offset(pagination["offset"]).limit(pagination["limit"])
    runs = db.execute(query).scalars().all()

    return BacktestRunsResponse(
        items=[BacktestRunSummary.model_validate(run) for run in runs],
        total=total,
        page=pagination["page"],
        page_size=pagination["page_size"],
        total_pages=(total + pagination["page_size"] - 1) // pagination["page_size"]
    )


@router.get("/backtest/runs/{run_id}", response_model=BacktestRunDetail)
async def get_backtest_run(
    run_id: str = Path(..., description="Backtest run ID"),
    db: Session = Depends(get_db)
):
    """
    Get detailed information about a specific backtest run (with caching)

    Returns:
        BacktestRunDetail with complete results and configuration

    Cache TTL: 1 hour (refreshed on backtest completion/update)
    """
    # Try cache first
    cache_key_str = cache_key("backtest_run", run_id=run_id)
    cached = cache.get(cache_key_str)

    if cached:
        logger.debug(f"Cache hit for backtest run: {run_id}")
        return BacktestRunDetail(**cached)

    # Cache miss - fetch from database
    logger.debug(f"Cache miss for backtest run: {run_id}")
    run = db.get(BacktestRun, run_id)

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Backtest run not found: {run_id}"
        )

    # Convert to response model
    response = BacktestRunDetail.model_validate(run)

    # Cache the result (only if completed or failed - don't cache pending/running)
    if run.status in ["completed", "failed"]:
        cache.set(cache_key_str, response.model_dump(), ttl=3600)  # 1 hour TTL
        logger.debug(f"Cached backtest run: {run_id}")

    return response


@router.get("/backtest/runs/{run_id}/trades", response_model=TradesResponse)
async def get_backtest_trades(
    run_id: str = Path(..., description="Backtest run ID"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db)
):
    """
    Get all trades for a specific backtest run

    Returns paginated list of trades ordered by entry date.
    """
    # Verify run exists
    run = db.get(BacktestRun, run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Backtest run not found: {run_id}"
        )

    pagination = validate_pagination(page, page_size)

    # Build query
    query = (
        select(Trade)
        .where(Trade.backtest_run_id == run_id)
        .order_by(Trade.entry_date.desc())
    )

    # Get total count
    total = db.execute(
        select(func.count()).select_from(query.subquery())
    ).scalar()

    # Get paginated results
    query = query.offset(pagination["offset"]).limit(pagination["limit"])
    trades = db.execute(query).scalars().all()

    return TradesResponse(
        items=[TradeResponse.model_validate(trade) for trade in trades],
        total=total,
        page=pagination["page"],
        page_size=pagination["page_size"],
        total_pages=(total + pagination["page_size"] - 1) // pagination["page_size"]
    )


@router.get("/backtest/runs/{run_id}/equity-curve", response_model=List[PortfolioSnapshotResponse])
async def get_equity_curve(
    run_id: str = Path(..., description="Backtest run ID"),
    db: Session = Depends(get_db)
):
    """
    Get equity curve (portfolio snapshots) for a specific backtest run

    Returns all snapshots ordered by date.
    This is typically used for visualization and does not support pagination.
    """
    # Verify run exists
    run = db.get(BacktestRun, run_id)
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Backtest run not found: {run_id}"
        )

    # Get all snapshots
    query = (
        select(PortfolioSnapshot)
        .where(PortfolioSnapshot.backtest_run_id == run_id)
        .order_by(PortfolioSnapshot.snapshot_date)
    )

    snapshots = db.execute(query).scalars().all()

    return [PortfolioSnapshotResponse.model_validate(snap) for snap in snapshots]


@router.delete("/backtest/runs/{run_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_backtest_run(
    run_id: str = Path(..., description="Backtest run ID"),
    db: Session = Depends(get_db)
):
    """
    Delete a backtest run and all associated data

    This will cascade delete all trades and portfolio snapshots.
    Also invalidates all related cache entries.
    """
    run = db.get(BacktestRun, run_id)

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Backtest run not found: {run_id}"
        )

    db.delete(run)
    db.commit()

    # Invalidate all cache entries for this run
    cache.delete(cache_key("backtest_run", run_id=run_id))
    cache.delete(cache_key("analytics", run_id=run_id))
    cache.invalidate_pattern(f"backtest:results:{run_id}*")
    cache.invalidate_pattern(f"chart:{run_id}:*")

    logger.info(f"Deleted backtest run and invalidated cache: {run_id}")


# ============================================================================
# Phase 4: Analytics & Visualization Endpoints
# ============================================================================


@router.get("/backtest/runs/{run_id}/metrics")
async def get_enhanced_metrics(
    run_id: str = Path(..., description="Backtest run ID"),
    db: Session = Depends(get_db)
):
    """
    Get enhanced performance metrics for a backtest run (with caching)

    Returns comprehensive analytics including Sortino ratio, Calmar ratio,
    volatility, and other advanced metrics.

    Cache TTL: 1 hour (refreshed on backtest completion/update)
    """
    # Try cache first
    cache_key_str = cache_key("analytics", run_id=run_id)
    cached = cache.get(cache_key_str)

    if cached:
        logger.debug(f"Cache hit for metrics: {run_id}")
        return cached

    # Cache miss - fetch from database
    logger.debug(f"Cache miss for metrics: {run_id}")
    run = db.get(BacktestRun, run_id)

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Backtest run not found: {run_id}"
        )

    # Build metrics response
    metrics = {
        "run_id": str(run.id),
        "metrics": {
            "total_return": float(run.total_return) if run.total_return else 0.0,
            "annualized_return": float(run.annualized_return) if run.annualized_return else 0.0,
            "sharpe_ratio": float(run.sharpe_ratio) if run.sharpe_ratio else 0.0,
            "sortino_ratio": float(run.sortino_ratio) if run.sortino_ratio else 0.0,
            "calmar_ratio": float(run.calmar_ratio) if run.calmar_ratio else 0.0,
            "max_drawdown": float(run.max_drawdown) if run.max_drawdown else 0.0,
            "volatility": float(run.volatility) if run.volatility else 0.0,
            "win_rate": float(run.win_rate) if run.win_rate else 0.0,
            "profit_factor": float(run.profit_factor) if run.profit_factor else 0.0,
            "total_trades": run.total_trades or 0,
        }
    }

    # Cache if backtest is completed
    if run.status in ["completed", "failed"]:
        cache.set(cache_key_str, metrics, ttl=3600)  # 1 hour TTL
        logger.debug(f"Cached metrics: {run_id}")

    return metrics


@router.post("/backtest/compare")
async def create_comparison(
    backtest_run_ids: List[str],
    metrics: List[str] = Query(["total_return", "sharpe_ratio", "max_drawdown"]),
    name: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Compare multiple backtest runs

    Creates a comparison analysis with rankings and statistical tests.

    Args:
        backtest_run_ids: List of backtest run UUIDs to compare
        metrics: List of metrics to compare
        name: Optional name for this comparison

    Returns:
        Comparison results with rankings and statistical significance
    """
    if len(backtest_run_ids) < 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least 2 backtest runs required for comparison"
        )

    try:
        # Use ComparisonEngine to perform comparison
        comparison_result = ComparisonEngine.compare_strategies(
            backtest_run_ids=backtest_run_ids,
            metrics=metrics,
            name=name,
            session=db
        )

        # Store comparison in database
        comparison = StrategyComparison(
            id=comparison_result.comparison_id,
            name=comparison_result.name,
            num_strategies=len(backtest_run_ids),
            metrics_compared=metrics,
            comparison_results=comparison_result.to_dict(),
            statistical_tests=comparison_result.statistical_tests
        )
        db.add(comparison)

        # Add junction table records
        for i, run_data in enumerate(comparison_result.runs):
            junction = ComparisonBacktestRun(
                comparison_id=comparison_result.comparison_id,
                backtest_run_id=run_data["run_id"],
                rank=run_data["rank"]
            )
            db.add(junction)

        db.commit()

        logger.info(f"Created comparison: {comparison_result.comparison_id}")

        return comparison_result.to_dict()

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/backtest/compare/{comparison_id}")
async def get_comparison(
    comparison_id: str = Path(..., description="Comparison ID"),
    db: Session = Depends(get_db)
):
    """
    Get comparison results (with caching)

    Retrieves a previously created comparison with all rankings and statistics.

    Cache TTL: 1 hour
    """
    # Try cache first
    cache_key_str = cache_key("comparison", comparison_id=comparison_id)
    cached = cache.get(cache_key_str)

    if cached:
        logger.debug(f"Cache hit for comparison: {comparison_id}")
        return cached

    # Cache miss - fetch from database
    logger.debug(f"Cache miss for comparison: {comparison_id}")
    comparison = db.get(StrategyComparison, comparison_id)

    if not comparison:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Comparison not found: {comparison_id}"
        )

    result = comparison.to_dict()

    # Cache the result
    cache.set(cache_key_str, result, ttl=3600)  # 1 hour TTL
    logger.debug(f"Cached comparison: {comparison_id}")

    return result


@router.get("/backtest/runs/{run_id}/charts/equity-curve")
async def get_equity_curve_chart(
    run_id: str = Path(..., description="Backtest run ID"),
    include_drawdown: bool = Query(True, description="Include drawdown series"),
    benchmark_run_id: Optional[str] = Query(None, description="Benchmark run ID for comparison"),
    db: Session = Depends(get_db)
):
    """
    Get equity curve chart data

    Returns formatted data for rendering equity curve charts on the frontend.
    """
    try:
        chart_data = ChartDataGenerator.equity_curve(
            run_id=run_id,
            session=db,
            include_drawdown=include_drawdown,
            benchmark_run_id=benchmark_run_id
        )
        return chart_data
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.get("/backtest/runs/{run_id}/charts/monthly-returns")
async def get_monthly_returns_chart(
    run_id: str = Path(..., description="Backtest run ID"),
    db: Session = Depends(get_db)
):
    """
    Get monthly returns heatmap data

    Returns monthly returns organized by year for heatmap visualization.
    """
    try:
        chart_data = ChartDataGenerator.monthly_returns(
            run_id=run_id,
            session=db
        )
        return chart_data
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.get("/backtest/runs/{run_id}/charts/trade-distribution")
async def get_trade_distribution_chart(
    run_id: str = Path(..., description="Backtest run ID"),
    bins: int = Query(20, ge=5, le=50, description="Number of histogram bins"),
    db: Session = Depends(get_db)
):
    """
    Get trade return distribution chart data

    Returns histogram data showing the distribution of trade returns.
    """
    try:
        chart_data = ChartDataGenerator.trade_distribution(
            run_id=run_id,
            session=db,
            bins=bins
        )
        return chart_data
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.get("/backtest/runs/{run_id}/charts/drawdown")
async def get_drawdown_chart(
    run_id: str = Path(..., description="Backtest run ID"),
    db: Session = Depends(get_db)
):
    """
    Get drawdown chart data

    Returns portfolio drawdown over time.
    """
    try:
        chart_data = ChartDataGenerator.drawdown_chart(
            run_id=run_id,
            session=db
        )
        return chart_data
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.get("/backtest/compare/{comparison_id}/chart")
async def get_comparison_chart(
    comparison_id: str = Path(..., description="Comparison ID"),
    metric: str = Query("total_value", description="Metric to compare"),
    db: Session = Depends(get_db)
):
    """
    Get multi-strategy comparison chart

    Returns chart data comparing multiple strategies on a selected metric.
    """
    # Get comparison
    comparison = db.get(StrategyComparison, comparison_id)

    if not comparison:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Comparison not found: {comparison_id}"
        )

    # Extract run IDs
    run_ids = [run["run_id"] for run in comparison.comparison_results["runs"]]

    try:
        chart_data = ChartDataGenerator.comparison_chart(
            run_ids=run_ids,
            session=db,
            metric=metric
        )
        return chart_data
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )


@router.get("/backtest/runs/{run_id}/benchmark-comparison")
async def compare_with_benchmark(
    run_id: str = Path(..., description="Backtest run ID"),
    benchmark_code: str = Query("399300", description="Benchmark index code (e.g., 399300 for CSI 300)"),
    db: Session = Depends(get_db)
):
    """
    Compare backtest run against market benchmark

    Calculates alpha, beta, correlation, and information ratio vs. benchmark index.

    Common benchmarks:
    - 399300: CSI 300 Index
    - 000016: SSE 50 Index
    - 000001: Shanghai Composite
    - 399001: Shenzhen Component
    """
    # Get run
    run = db.get(BacktestRun, run_id)

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Backtest run not found: {run_id}"
        )

    try:
        benchmark_comparison = BenchmarkIntegration.compare_with_benchmark(
            run_id=run_id,
            benchmark_code=benchmark_code,
            start_date=run.start_date,
            end_date=run.end_date,
            session=db
        )

        return {
            "run_id": str(run.id),
            "benchmark_code": benchmark_code,
            "comparison": benchmark_comparison.to_dict()
        }

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
