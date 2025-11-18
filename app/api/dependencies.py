"""
API Dependencies - Shared dependencies for FastAPI endpoints

Provides database sessions, authentication, and other common dependencies.
"""

from typing import Generator

from fastapi import Depends, HTTPException, status
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from loguru import logger

from app.db.engine import engine_from_env


# Global engine instance (created once at startup via lifespan)
# This will be set by the FastAPI lifespan event in main.py
_engine: Engine | None = None


def init_engine() -> Engine:
    """
    Initialize database engine with proper connection pooling

    This should be called once during application startup.

    Returns:
        SQLAlchemy Engine instance configured for production use
    """
    global _engine
    if _engine is None:
        logger.info("Initializing database engine with connection pooling")
        _engine = engine_from_env(
            echo=False,
            pool_size=20,           # Number of connections to maintain
            max_overflow=40,        # Additional connections when pool is full
            pool_pre_ping=True,     # Verify connections before using
            pool_recycle=3600,      # Recycle connections after 1 hour
            pool_timeout=30,        # Wait up to 30s for connection
        )
        logger.success(f"Database engine initialized: {_engine.url.database}")
    return _engine


def get_engine() -> Engine:
    """
    Get database engine instance

    Returns:
        SQLAlchemy Engine instance

    Raises:
        RuntimeError: If engine not initialized
    """
    if _engine is None:
        # Fallback: auto-initialize if not done via lifespan
        logger.warning("Engine not initialized via lifespan, auto-initializing now")
        return init_engine()
    return _engine


def dispose_engine() -> None:
    """
    Dispose database engine and cleanup connections

    This should be called during application shutdown.
    """
    global _engine
    if _engine is not None:
        logger.info("Disposing database engine")
        _engine.dispose()
        _engine = None


def get_db() -> Generator[Session, None, None]:
    """
    Dependency that provides a database session with proper transaction handling

    Yields:
        SQLAlchemy Session

    Features:
        - Auto-commit on success
        - Auto-rollback on exception
        - Proper session cleanup
        - Detached instance support

    Usage:
        @app.get("/items")
        def read_items(db: Session = Depends(get_db)):
            return db.query(Item).all()
    """
    engine = get_engine()
    session = Session(
        engine,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False  # Prevent lazy-load issues with detached instances
    )
    try:
        yield session
        session.commit()  # Auto-commit on success
    except Exception as e:
        session.rollback()  # Auto-rollback on error
        logger.error(f"Database session error, rolling back: {e}")
        raise
    finally:
        session.close()


def validate_pagination(page: int = 1, page_size: int = 20) -> dict:
    """
    Validate and return pagination parameters

    Args:
        page: Page number (1-indexed)
        page_size: Items per page

    Returns:
        Dict with offset and limit

    Raises:
        HTTPException: If parameters are invalid
    """
    if page < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Page must be >= 1"
        )

    if page_size < 1 or page_size > 100:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Page size must be between 1 and 100"
        )

    return {
        "offset": (page - 1) * page_size,
        "limit": page_size,
        "page": page,
        "page_size": page_size
    }


# Future: Authentication dependencies
# def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
#     """Verify JWT token and return current user"""
#     pass
