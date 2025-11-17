"""
Shared pytest fixtures and configuration for all tests.
"""
import pytest
from datetime import date, datetime
from decimal import Decimal
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.db.models import MetadataBase, Market, Stock, Collection, StockDaily, FeedDaily
from app.constant.collection import CollectionType


# --- Mock Engine and Session ---

class MockDialect:
    """Mock SQLAlchemy dialect for testing."""
    def __init__(self, name):
        self.name = name


class MockEngine:
    """Mock SQLAlchemy engine for testing."""
    def __init__(self, dialect_name="sqlite"):
        self.dialect = MockDialect(dialect_name)


class MockSession:
    """Mock SQLAlchemy session for testing."""
    def __init__(self):
        self.merges = []
        self.commit_called = False
        self.rollback_called = False
        self.executed_statements = []

    def merge(self, obj):
        self.merges.append(obj)
        return obj

    def commit(self):
        self.commit_called = True

    def rollback(self):
        self.rollback_called = True

    def execute(self, stmt):
        self.executed_statements.append(stmt)
        return MockResult()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class MockResult:
    """Mock result object for database queries."""
    def __init__(self, data=None):
        self.data = data or []

    def scalars(self):
        return self

    def all(self):
        return self.data

    def first(self):
        return self.data[0] if self.data else None


class ExceptionMockSession(MockSession):
    """Mock session that raises exceptions on commit."""
    def commit(self):
        raise Exception("Mock commit failure")


# --- Database Fixtures ---

@pytest.fixture
def in_memory_engine():
    """Create an in-memory SQLite engine for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    MetadataBase.metadata.create_all(engine)
    return engine


@pytest.fixture
def db_session(in_memory_engine):
    """Create a database session for testing."""
    session = Session(in_memory_engine)
    yield session
    session.close()


@pytest.fixture
def mock_engine():
    """Create a mock engine."""
    return MockEngine()


@pytest.fixture
def mock_session():
    """Create a mock session."""
    return MockSession()


# --- Sample Data Fixtures ---

@pytest.fixture
def sample_market():
    """Create a sample Market object."""
    from datetime import time
    return Market(
        id=1,
        name="Shanghai Stock Exchange",
        name_short="SSE",
        country="China",
        open=time(9, 30),
        close=time(15, 0),
        currency="CNY"
    )


@pytest.fixture
def sample_stock():
    """Create a sample Stock object."""
    return Stock(
        code="600000",
        name="浦发银行",
        market_id=1
    )


@pytest.fixture
def sample_collection():
    """Create a sample Collection object."""
    return Collection(
        code="BK0001",
        name="Banking",
        type=CollectionType.INDUSTRY_BOARD
    )


@pytest.fixture
def sample_stock_daily():
    """Create a sample StockDaily object."""
    return StockDaily(
        code="600000",
        trade_day=date(2025, 11, 17),
        open=Decimal("10.50"),
        high=Decimal("11.00"),
        low=Decimal("10.30"),
        close=Decimal("10.80"),
        volume=1000000,
        turnover=10800000,
        capital=50000000000,
        circulation_capital=30000000000,
        quantity_relative_ratio=1.2,
        turnover_rate=3.5,
        ma_250=Decimal("10.00")
    )


@pytest.fixture
def sample_feed_daily():
    """Create a sample FeedDaily object."""
    return FeedDaily(
        code="600000",
        trade_day=date(2025, 11, 17),
        filter_id=1,
        name="浦发银行",
        collection_name="Banking",
        collection_performance=2.5,
        previous_close=Decimal("10.00"),
        close=Decimal("10.80"),
        previous_volume=800000,
        volume=1000000,
        gain=8.0,
        volume_gain=25.0
    )


@pytest.fixture
def sample_dataframe():
    """Create a sample pandas DataFrame for testing."""
    return pd.DataFrame({
        'code': ['600000', '600001'],
        'name': ['浦发银行', '邯郸钢铁'],
        'trade_day': [date(2025, 11, 17), date(2025, 11, 17)],
        'close': [10.80, 5.50],
        'volume': [1000000, 500000],
        'gain': [8.0, 5.0],
    })


@pytest.fixture
def stock_daily_dataframe():
    """Create a sample stock daily DataFrame (AKShare format)."""
    return pd.DataFrame({
        '代码': ['600000', '600001'],
        '今开': [10.50, 5.40],
        '最高': [11.00, 5.60],
        '最低': [10.30, 5.35],
        '最新价': [10.80, 5.50],
        '成交量': [1000000, 500000],
        '成交额': [10800000, 2750000],
        '总市值': [50000000000, 10000000000],
        '流通市值': [30000000000, 8000000000],
        '量比': [1.2, 0.9],
        '换手率': [3.5, 6.25],
    })


# --- Test Utilities ---

@pytest.fixture
def freeze_time():
    """Fixture to freeze datetime.now() for testing."""
    frozen_time = datetime(2025, 11, 17, 15, 30, 0)

    def _get_frozen_time():
        return frozen_time

    return _get_frozen_time


@pytest.fixture
def temp_env_vars(monkeypatch):
    """Fixture to set temporary environment variables."""
    def _set_env(**kwargs):
        for key, value in kwargs.items():
            monkeypatch.setenv(key, value)

    return _set_env


# --- Parametrize Helpers ---

def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (deselect with '-m \"not integration\"')"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
