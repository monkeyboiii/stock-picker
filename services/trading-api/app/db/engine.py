import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine


load_dotenv(override=True)


def engine_from_env(**kwargs) -> Engine:
    """
    Create database engine from environment variables with connection pooling.

    Default pooling parameters (can be overridden via kwargs):
    - pool_size: 10 (base connections)
    - max_overflow: 20 (extra connections under load)
    - pool_timeout: 30 seconds
    - pool_recycle: 3600 seconds (1 hour)
    - pool_pre_ping: True (verify connections)
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

    return create_engine(url, **engine_kwargs)


def engine_mock(**kwargs):
    return create_engine('sqlite:///:memory:', **kwargs)
