"""
API Dependencies - Shared dependencies for FastAPI endpoints

Provides database sessions, authentication, and other common dependencies.
"""

from typing import Generator

from fastapi import Depends, HTTPException, status
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.db.engine import engine_from_env


# Global engine instance (created once at startup)
_engine: Engine | None = None


def get_engine() -> Engine:
    """
    Get or create database engine

    Returns:
        SQLAlchemy Engine instance
    """
    global _engine
    if _engine is None:
        _engine = engine_from_env(echo=False)
    return _engine


def get_db() -> Generator[Session, None, None]:
    """
    Dependency that provides a database session

    Yields:
        SQLAlchemy Session

    Usage:
        @app.get("/items")
        def read_items(db: Session = Depends(get_db)):
            return db.query(Item).all()
    """
    engine = get_engine()
    session = Session(engine)
    try:
        yield session
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
