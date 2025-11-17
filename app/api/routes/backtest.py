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
from app.backtest.engine import BacktestEngine
from app.backtest.strategy import get_strategy
from app.db.models import BacktestRun, PortfolioSnapshot, Strategy, Trade
from app.strategy import parse_strategy, validate_strategy

router = APIRouter()


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
    Get detailed information about a specific backtest run

    Returns:
        BacktestRunDetail with complete results and configuration
    """
    run = db.get(BacktestRun, run_id)

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Backtest run not found: {run_id}"
        )

    return BacktestRunDetail.model_validate(run)


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
    """
    run = db.get(BacktestRun, run_id)

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Backtest run not found: {run_id}"
        )

    db.delete(run)
    db.commit()

    logger.info(f"Deleted backtest run: {run_id}")
