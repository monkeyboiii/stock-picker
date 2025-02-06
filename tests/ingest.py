import pytest
import pandas as pd
from datetime import date
from decimal import Decimal

from app.utils.ingest import refresh_stock_daily

# --- Dummy Classes for Session and Engine ---

class DummySession:
    def __init__(self):
        self.merges = []
        self.commit_called = False
        self.rollback_called = False

    def merge(self, obj):
        self.merges.append(obj)

    def commit(self):
        self.commit_called = True

    def rollback(self):
        self.rollback_called = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class ExceptionDummySession(DummySession):
    """A dummy session whose commit always fails."""
    def commit(self):
        raise Exception("Commit failed")


class DummyDialect:
    def __init__(self, name):
        self.name = name


class DummyEngine:
    def __init__(self, dialect_name):
        self.dialect = DummyDialect(dialect_name)


# --- Pytest Fixtures ---

@pytest.fixture
def dummy_df():
    """Returns a dummy DataFrame resembling the output of pull_stock_daily."""
    return pd.DataFrame({
        '代码': ['ABC', "DEF"],
        '今开': [10.12345, 1,11],
        '最高': [10.54321, 1.21],
        '最低': [9.87654, pd.NA],
        '最新价': [10.23456, pd.NA],
        '成交量': [1000, 2000],
        '成交额': [5000, 14124],
        '总市值': [100000, 23434],
        '流通市值': [80000, 2342341],
        '量比': [1.23, 2.34],
        '换手率': [0.56, 5.35],
    })


@pytest.fixture
def dummy_session(monkeypatch):
    """
    Patches the Session in the ingest module to return a DummySession
    and returns the DummySession instance so that tests can inspect it.
    """
    session = DummySession()
    monkeypatch.setattr("app.utils.ingest.Session", lambda engine: session)
    return session


# --- Test Functions ---

def test_refresh_stock_daily_sqlite(dummy_session, dummy_df, monkeypatch):
    """
    Test the non-postgresql branch (e.g. for sqlite) of refresh_stock_daily.
    This branch should call session.merge() for each row of data.
    """
    # Patch pull_stock_daily so that it returns our dummy DataFrame.
    monkeypatch.setattr("app.utils.ingest.pull_stock_daily", lambda: dummy_df)
    
    # Create a dummy engine with a dialect name that is not 'postgresql'
    engine = DummyEngine("sqlite")
    
    # Call the function under test.
    refresh_stock_daily(engine)
    
    # Verify that commit() was called.
    assert dummy_session.commit_called is True
    
    # In the non-postgresql branch, merge() is called for each row.
    # Since our dummy DataFrame has 1 row, we expect one merge.
    assert len(dummy_session.merges) == 2
    
    # Inspect the merged object.
    merged_stock = dummy_session.merges[0]
    today = date.today()
    # The refresh_stock_daily function renames and transforms the DataFrame so that:
    # - '代码' becomes 'code'
    # - numeric fields are converted (e.g. to Decimal with 3 decimals)
    # - trade_day is set to today's date.
    assert merged_stock.code == 'ABC'
    assert merged_stock.trade_day == today
    assert merged_stock.open == Decimal(format(10.12345, '.3f'))   # Decimal('10.123')
    assert merged_stock.high == Decimal(format(10.54321, '.3f'))   # Decimal('10.543')
    assert merged_stock.low == Decimal(format(9.87654, '.3f'))     # Decimal('9.877')
    assert merged_stock.close == Decimal(format(10.23456, '.3f'))  # Decimal('10.235')
    assert merged_stock.volume == 1000
    assert merged_stock.turnover == 5000
    assert merged_stock.capital == 100000
    assert merged_stock.circulation_capital == 80000
    assert merged_stock.quantity_relative_ratio == 1.23
    assert merged_stock.turnover_rate == 0.56


def test_refresh_stock_daily_postgresql(dummy_df, monkeypatch):
    """
    Test the PostgreSQL branch of refresh_stock_daily.
    In this branch, an upsert (on_conflict) statement is built
    and session.merge() is not used.
    """
    # Create a dummy session instance.
    session = DummySession()
    monkeypatch.setattr("app.utils.ingest.Session", lambda engine: session)
    
    # Patch pull_stock_daily to return our dummy DataFrame.
    monkeypatch.setattr("app.utils.ingest.pull_stock_daily", lambda: dummy_df)
    
    # Create a dummy engine with dialect 'postgresql'
    engine = DummyEngine("postgresql")
    
    # Call the function.
    refresh_stock_daily(engine)
    
    # In the postgresql branch, no merge() should be called.
    assert session.commit_called is True
    assert len(session.merges) == 0


def test_refresh_stock_daily_exception(dummy_df, monkeypatch):
    """
    Test that if an exception occurs (e.g. during commit),
    refresh_stock_daily calls session.rollback() and logs an error.
    """
    # Use a session that raises an exception on commit.
    session = ExceptionDummySession()
    monkeypatch.setattr("app.utils.ingest.Session", lambda engine: session)
    
    # Patch pull_stock_daily to return our dummy DataFrame.
    monkeypatch.setattr("app.utils.ingest.pull_stock_daily", lambda: dummy_df)
    
    # Use any engine (dialect doesn't matter here).
    engine = DummyEngine("sqlite")
    
    # Call refresh_stock_daily. The exception from commit() should be caught.
    refresh_stock_daily(engine)
    
    # Verify that rollback() was called.
    assert session.rollback_called is True