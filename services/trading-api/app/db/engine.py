import os

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import OperationalError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


load_dotenv(override=True)


@retry(
    retry=retry_if_exception_type((OperationalError, ConnectionError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
def engine_from_env(**kwargs) -> Engine:
    """
    Create database engine from environment variables with connection pooling and retry logic.

    Default pooling parameters (can be overridden via kwargs):
    - pool_size: 10 (base connections)
    - max_overflow: 20 (extra connections under load)
    - pool_timeout: 30 seconds
    - pool_recycle: 3600 seconds (1 hour)
    - pool_pre_ping: True (verify connections)

    RESILIENCE: Retries up to 5 times with exponential backoff (1s, 2s, 4s, 8s, 10s)
    Handles transient network issues and database startup delays.
    """
    url = URL.create(
        drivername= os.getenv("DB_DRIVER")          or 'postgresql',
        username=   os.getenv("POSTGRES_USERNAME")  or 'postgres',
        password=   os.getenv("POSTGRES_PASSWORD")  or 'postgres',
        host=       os.getenv("POSTGRES_HOST")      or 'localhost',
        port=   int(os.getenv("POSTGRES_PORT")      or '5432'),
        database=   os.getenv("POSTGRES_DATABASE")
    )

    if url.drivername != 'postgresql':
        raise Exception("Only support postgresql atm")

    # Default connection pooling parameters for production reliability
    pool_defaults = {
        'pool_size': 10,
        'max_overflow': 20,
        'pool_timeout': 30,
        'pool_recycle': 3600,
        'pool_pre_ping': True,
    }
    # Merge defaults with user-provided kwargs (kwargs take precedence)
    engine_kwargs = {**pool_defaults, **kwargs}

    logger.info("Creating database engine with connection pooling...")
    engine = create_engine(url, **engine_kwargs)

    # Test connection immediately to trigger retry if database is unavailable
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    logger.info("Database engine created successfully")
    return engine


def engine_mock(**kwargs):
    return create_engine('sqlite:///:memory:', **kwargs)
