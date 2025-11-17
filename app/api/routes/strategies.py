"""
Strategy API Routes

Endpoints for managing trading strategies.
"""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, UploadFile, status
from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.api.dependencies import get_db, validate_pagination
from app.api.models import (
    StrategiesResponse,
    StrategyCreateRequest,
    StrategyResponse,
    StrategySummary,
)
from app.backtest.strategy import create_strategy, get_strategy, list_strategies
from app.cache.redis_cache import get_cache, cache_key
from app.db.models import Strategy
from app.strategy import parse_strategy, validate_strategy

router = APIRouter()

# Initialize cache
cache = get_cache()


@router.post("/strategies", response_model=StrategyResponse, status_code=status.HTTP_201_CREATED)
async def create_new_strategy(
    request: StrategyCreateRequest,
    db: Session = Depends(get_db)
):
    """
    Create a new strategy

    The definition should be a valid strategy definition dictionary
    (can be converted from YAML/JSON).
    """
    try:
        # Validate strategy definition
        strategy_def = parse_strategy(request.definition)
        validate_strategy(strategy_def)

        # Create in database
        strategy_id = create_strategy(
            engine=db.get_bind(),
            name=request.name,
            version=request.version,
            definition=request.definition,
            author=request.author,
            description=request.description,
        )

        # Retrieve created strategy
        strategy = get_strategy(db.get_bind(), strategy_id=strategy_id)

        if not strategy:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve created strategy"
            )

        return StrategyResponse.model_validate(strategy)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid strategy: {e}"
        )


@router.post("/strategies/upload", response_model=StrategyResponse, status_code=status.HTTP_201_CREATED)
async def upload_strategy_file(
    file: UploadFile,
    db: Session = Depends(get_db)
):
    """
    Upload a strategy from YAML or JSON file

    Accepts .yaml, .yml, or .json files.
    """
    # Validate file extension
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Filename is required"
        )

    if not file.filename.endswith(('.yaml', '.yml', '.json')):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be .yaml, .yml, or .json"
        )

    try:
        # Read file content
        content = await file.read()
        content_str = content.decode('utf-8')

        # Parse and validate strategy
        strategy_def = parse_strategy(content_str)
        validate_strategy(strategy_def)

        # Create in database
        strategy_id = create_strategy(
            engine=db.get_bind(),
            name=strategy_def.name,
            version=strategy_def.version,
            definition=strategy_def.model_dump(),
            author=strategy_def.author,
            description=strategy_def.description,
        )

        # Retrieve created strategy
        strategy = get_strategy(db.get_bind(), strategy_id=strategy_id)

        if not strategy:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve created strategy"
            )

        logger.info(f"Uploaded strategy: {strategy.name} v{strategy.version}")

        return StrategyResponse.model_validate(strategy)

    except UnicodeDecodeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be UTF-8 encoded"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid strategy file: {e}"
        )


@router.get("/strategies", response_model=StrategiesResponse)
async def list_all_strategies(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    active_only: bool = Query(True, description="Only return active strategies"),
    name: Optional[str] = Query(None, description="Filter by name (partial match)"),
    db: Session = Depends(get_db)
):
    """
    List all strategies with pagination

    Supports filtering by active status and name.
    """
    pagination = validate_pagination(page, page_size)

    # Build query
    query = select(Strategy).order_by(Strategy.created_at.desc())

    if active_only:
        query = query.where(Strategy.is_active == True)

    if name:
        query = query.where(Strategy.name.ilike(f"%{name}%"))

    # Get total count
    total = db.execute(select(func.count()).select_from(query.subquery())).scalar()

    # Get paginated results
    query = query.offset(pagination["offset"]).limit(pagination["limit"])
    strategies = db.execute(query).scalars().all()

    return StrategiesResponse(
        items=[StrategySummary.model_validate(s) for s in strategies],
        total=total,
        page=pagination["page"],
        page_size=pagination["page_size"],
        total_pages=(total + pagination["page_size"] - 1) // pagination["page_size"]
    )


@router.get("/strategies/{strategy_id}", response_model=StrategyResponse)
async def get_strategy_by_id(
    strategy_id: str = Path(..., description="Strategy ID"),
    db: Session = Depends(get_db)
):
    """
    Get detailed information about a specific strategy (with caching)

    Returns the complete strategy definition.

    Cache TTL: 1 hour (refreshed on strategy updates)
    """
    # Try cache first
    cache_key_str = cache_key("strategy", strategy_id=strategy_id)
    cached = cache.get(cache_key_str)

    if cached:
        logger.debug(f"Cache hit for strategy: {strategy_id}")
        return StrategyResponse(**cached)

    # Cache miss - fetch from database
    logger.debug(f"Cache miss for strategy: {strategy_id}")
    strategy = db.get(Strategy, strategy_id)

    if not strategy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Strategy not found: {strategy_id}"
        )

    # Convert to response model
    response = StrategyResponse.model_validate(strategy)

    # Cache the result
    cache.set(cache_key_str, response.model_dump(), ttl=3600)  # 1 hour TTL
    logger.debug(f"Cached strategy: {strategy_id}")

    return response


@router.patch("/strategies/{strategy_id}/deactivate", response_model=StrategyResponse)
async def deactivate_strategy(
    strategy_id: str = Path(..., description="Strategy ID"),
    db: Session = Depends(get_db)
):
    """
    Deactivate a strategy (soft delete)

    The strategy will no longer appear in default listings
    but remains in the database for historical backtests.
    Also invalidates cache.
    """
    strategy = db.get(Strategy, strategy_id)

    if not strategy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Strategy not found: {strategy_id}"
        )

    strategy.is_active = False
    db.commit()
    db.refresh(strategy)

    # Invalidate cache
    cache.delete(cache_key("strategy", strategy_id=strategy_id))

    logger.info(f"Deactivated strategy and invalidated cache: {strategy.name} v{strategy.version}")

    return StrategyResponse.model_validate(strategy)


@router.patch("/strategies/{strategy_id}/activate", response_model=StrategyResponse)
async def activate_strategy(
    strategy_id: str = Path(..., description="Strategy ID"),
    db: Session = Depends(get_db)
):
    """
    Reactivate a previously deactivated strategy

    Also invalidates cache.
    """
    strategy = db.get(Strategy, strategy_id)

    if not strategy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Strategy not found: {strategy_id}"
        )

    strategy.is_active = True
    db.commit()
    db.refresh(strategy)

    # Invalidate cache
    cache.delete(cache_key("strategy", strategy_id=strategy_id))

    logger.info(f"Activated strategy and invalidated cache: {strategy.name} v{strategy.version}")

    return StrategyResponse.model_validate(strategy)


@router.delete("/strategies/{strategy_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_strategy(
    strategy_id: str = Path(..., description="Strategy ID"),
    db: Session = Depends(get_db)
):
    """
    Permanently delete a strategy

    WARNING: This will fail if there are associated backtest runs.
    Consider using PATCH /strategies/{id}/deactivate instead.
    Also invalidates cache.
    """
    strategy = db.get(Strategy, strategy_id)

    if not strategy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Strategy not found: {strategy_id}"
        )

    try:
        db.delete(strategy)
        db.commit()

        # Invalidate cache
        cache.delete(cache_key("strategy", strategy_id=strategy_id))

        logger.info(f"Deleted strategy and invalidated cache: {strategy.name} v{strategy.version}")
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot delete strategy: {e}. Consider deactivating instead."
        )
