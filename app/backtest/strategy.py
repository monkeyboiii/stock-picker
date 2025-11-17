"""
Strategy Management - CRUD operations for strategies

This module provides functions to:
- Create and store strategies in the database
- Load strategies from the database
- Manage strategy versions
"""

from typing import Dict, Optional
from uuid import uuid4

from loguru import logger
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.backtest.signals import StrategyBuilder
from app.db.models import Strategy


def create_strategy(
    engine: Engine,
    name: str,
    version: str,
    definition: Dict,
    author: Optional[str] = None,
    description: Optional[str] = None,
) -> str:
    """
    Create a new strategy in the database

    Args:
        engine: Database engine
        name: Strategy name
        version: Strategy version
        definition: Strategy definition dict
        author: Strategy author
        description: Strategy description

    Returns:
        Strategy ID (UUID)
    """
    with Session(engine) as session:
        strategy = Strategy(
            name=name,
            version=version,
            definition=definition,
            author=author,
            description=description,
            is_active=True,
        )
        session.add(strategy)
        session.commit()
        session.refresh(strategy)

        strategy_id = strategy.id
        logger.success(f"Created strategy: {name} v{version} (ID: {strategy_id})")
        return strategy_id


def get_strategy(
    engine: Engine,
    strategy_id: Optional[str] = None,
    name: Optional[str] = None,
    version: Optional[str] = None,
) -> Optional[Strategy]:
    """
    Get a strategy from the database

    Args:
        engine: Database engine
        strategy_id: Strategy UUID (if provided, name/version ignored)
        name: Strategy name
        version: Strategy version

    Returns:
        Strategy object or None if not found
    """
    with Session(engine) as session:
        if strategy_id:
            stmt = select(Strategy).where(Strategy.id == strategy_id)
        elif name and version:
            stmt = select(Strategy).where(
                Strategy.name == name,
                Strategy.version == version,
            )
        elif name:
            # Get latest version
            stmt = (
                select(Strategy)
                .where(Strategy.name == name)
                .order_by(Strategy.created_at.desc())
                .limit(1)
            )
        else:
            raise ValueError("Must provide either strategy_id or name")

        result = session.execute(stmt).scalar_one_or_none()
        return result


def get_or_create_tail_scraper_strategy(engine: Engine) -> str:
    """
    Get or create the default tail_scraper strategy

    Args:
        engine: Database engine

    Returns:
        Strategy ID (UUID)
    """
    # Check if it already exists
    existing = get_strategy(engine, name="Tail Scraper", version="1.0.0")
    if existing:
        logger.info(f"Using existing Tail Scraper strategy (ID: {existing.id})")
        return existing.id

    # Create it
    definition = StrategyBuilder.tail_scraper_strategy()
    strategy_id = create_strategy(
        engine=engine,
        name="Tail Scraper",
        version="1.0.0",
        definition=definition,
        description="Original tail scraper strategy with T2-T8 filters",
        author="system",
    )
    return strategy_id


def list_strategies(engine: Engine, active_only: bool = True) -> list[Strategy]:
    """
    List all strategies

    Args:
        engine: Database engine
        active_only: Only return active strategies

    Returns:
        List of Strategy objects
    """
    with Session(engine) as session:
        stmt = select(Strategy).order_by(Strategy.created_at.desc())

        if active_only:
            stmt = stmt.where(Strategy.is_active == True)

        results = session.execute(stmt).scalars().all()
        return list(results)


def deactivate_strategy(engine: Engine, strategy_id: str) -> None:
    """
    Deactivate a strategy (soft delete)

    Args:
        engine: Database engine
        strategy_id: Strategy UUID
    """
    with Session(engine) as session:
        strategy = session.get(Strategy, strategy_id)
        if strategy:
            strategy.is_active = False
            session.commit()
            logger.info(f"Deactivated strategy: {strategy.name} v{strategy.version}")
        else:
            logger.warning(f"Strategy not found: {strategy_id}")
