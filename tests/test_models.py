"""
Tests for database models in app/db/models.py
"""
import pytest
from datetime import date, datetime
from decimal import Decimal
import pandas as pd

from app.db.models import (
    Market, Stock, Collection, RelationCollectionStock,
    StockDaily, CollectionDaily, FeedDaily
)
from app.constant.collection import CollectionType


class TestMarket:
    """Tests for Market model."""

    def test_market_creation(self, sample_market):
        """Test creating a Market instance."""
        assert sample_market.id == 1
        assert sample_market.name == "Shanghai Stock Exchange"
        assert sample_market.name_short == "SSE"
        assert sample_market.country == "China"
        assert sample_market.currency == "CNY"

    def test_market_persistence(self, db_session, sample_market):
        """Test saving and retrieving a Market from database."""
        db_session.add(sample_market)
        db_session.commit()

        retrieved = db_session.query(Market).filter_by(id=1).first()
        assert retrieved is not None
        assert retrieved.name == "Shanghai Stock Exchange"


class TestStock:
    """Tests for Stock model."""

    def test_stock_creation(self, sample_stock):
        """Test creating a Stock instance."""
        assert sample_stock.code == "600000"
        assert sample_stock.name == "浦发银行"
        assert sample_stock.market_id == 1

    def test_stock_persistence(self, db_session, sample_market, sample_stock):
        """Test saving and retrieving a Stock from database."""
        db_session.add(sample_market)
        db_session.add(sample_stock)
        db_session.commit()

        retrieved = db_session.query(Stock).filter_by(code="600000").first()
        assert retrieved is not None
        assert retrieved.name == "浦发银行"

    def test_stock_unique_name_constraint(self, db_session, sample_market):
        """Test that stock names must be unique."""
        db_session.add(sample_market)

        stock1 = Stock(code="600000", name="Test", market_id=1)
        stock2 = Stock(code="600001", name="Test", market_id=1)

        db_session.add(stock1)
        db_session.commit()

        db_session.add(stock2)
        with pytest.raises(Exception):  # SQLAlchemy IntegrityError
            db_session.commit()


class TestCollection:
    """Tests for Collection model."""

    def test_collection_creation(self, sample_collection):
        """Test creating a Collection instance."""
        assert sample_collection.code == "BK0001"
        assert sample_collection.name == "Banking"
        assert sample_collection.type == CollectionType.INDUSTRY_BOARD

    def test_collection_type_enum(self):
        """Test CollectionType enum values."""
        assert CollectionType.ANALYST.value == 'a'
        assert CollectionType.BOARD.value == 'b'
        assert CollectionType.CONCEPT_BOARD.value == 'c'
        assert CollectionType.INDUSTRY_BOARD.value == 'i'
        assert CollectionType.INDEX.value == 'x'

    def test_collection_stock_relationship(self, db_session, sample_market, sample_stock, sample_collection):
        """Test many-to-many relationship between Collection and Stock."""
        db_session.add(sample_market)
        db_session.add(sample_stock)
        db_session.add(sample_collection)
        db_session.commit()

        # Add stock to collection
        sample_collection.stocks.append(sample_stock)
        db_session.commit()

        # Verify relationship
        retrieved_collection = db_session.query(Collection).filter_by(code="BK0001").first()
        assert len(retrieved_collection.stocks) == 1
        assert retrieved_collection.stocks[0].code == "600000"


class TestStockDaily:
    """Tests for StockDaily model."""

    def test_stock_daily_creation(self, sample_stock_daily):
        """Test creating a StockDaily instance."""
        assert sample_stock_daily.code == "600000"
        assert sample_stock_daily.trade_day == date(2025, 11, 17)
        assert sample_stock_daily.close == Decimal("10.80")
        assert sample_stock_daily.volume == 1000000
        assert sample_stock_daily.ma_250 == Decimal("10.00")

    def test_stock_daily_to_dict(self, sample_stock_daily):
        """Test converting StockDaily to dictionary."""
        data = sample_stock_daily.to_dict()

        assert isinstance(data, dict)
        assert data['code'] == "600000"
        assert data['close'] == Decimal("10.80")
        assert 'trade_day' in data

    def test_stock_daily_composite_pk(self, db_session, sample_market, sample_stock):
        """Test composite primary key (code, trade_day)."""
        db_session.add(sample_market)
        db_session.add(sample_stock)
        db_session.commit()

        # Add first record
        sd1 = StockDaily(
            code="600000",
            trade_day=date(2025, 11, 17),
            close=Decimal("10.80")
        )
        db_session.add(sd1)
        db_session.commit()

        # Try to add duplicate - should fail
        sd2 = StockDaily(
            code="600000",
            trade_day=date(2025, 11, 17),
            close=Decimal("11.00")
        )
        db_session.add(sd2)
        with pytest.raises(Exception):  # SQLAlchemy IntegrityError
            db_session.commit()


class TestFeedDaily:
    """Tests for FeedDaily model."""

    def test_feed_daily_creation(self, sample_feed_daily):
        """Test creating a FeedDaily instance."""
        assert sample_feed_daily.code == "600000"
        assert sample_feed_daily.name == "浦发银行"
        assert sample_feed_daily.gain == 8.0
        assert sample_feed_daily.volume_gain == 25.0

    def test_feed_daily_to_dict(self, sample_feed_daily):
        """Test converting FeedDaily to dictionary."""
        data = sample_feed_daily.to_dict()

        assert isinstance(data, dict)
        assert data['code'] == "600000"
        assert data['name'] == "浦发银行"
        assert data['gain'] == 8.0

    def test_feed_daily_to_dataframe(self, sample_feed_daily):
        """Test converting FeedDaily list to DataFrame."""
        feeds = [sample_feed_daily]
        df = FeedDaily.to_dataframe(feeds)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]['code'] == "600000"
        assert df.iloc[0]['gain'] == 8.0

    def test_feed_daily_to_dataframe_empty(self):
        """Test converting empty FeedDaily list to DataFrame."""
        df = FeedDaily.to_dataframe([])

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_feed_column_mapping(self):
        """Test column mapping for feed display."""
        mapping = FeedDaily.feed_column_mapping()

        assert isinstance(mapping, dict)
        assert mapping['code'] == '股票代码'
        assert mapping['name'] == '股票名称'
        assert mapping['gain'] == '涨幅'
        assert mapping['volume_gain'] == '量涨幅'

    def test_convert_to_feed(self, sample_dataframe):
        """Test converting DataFrame to feed format."""
        # Create minimal DataFrame with required columns
        df = pd.DataFrame({
            'trade_day': [date(2025, 11, 17)],
            'code': ['600000'],
            'name': ['浦发银行'],
            'collection_name': ['Banking'],
            'collection_performance': [2.5],
            'previous_close': [Decimal("10.00")],
            'close': [Decimal("10.80")],
            'gain': [8.0],
            'previous_volume': [800000],
            'volume': [1000000],
            'volume_gain': [25.0],
        })

        result = FeedDaily.convert_to_feed(df)

        assert isinstance(result, pd.DataFrame)
        assert '股票代码' in result.columns
        assert '股票名称' in result.columns
        assert '涨幅' in result.columns
        # Check formatting
        assert result.iloc[0]['涨幅'] == '8.00%'

    def test_right_align_columns(self):
        """Test getting columns that should be right-aligned."""
        columns = FeedDaily.right_align_columns()

        assert isinstance(columns, list)
        assert 'gain' in columns
        assert 'volume_gain' in columns
        assert 'close' in columns

    def test_colorize_columns(self):
        """Test getting columns that should be colorized."""
        columns = FeedDaily.colorize_columns()

        assert isinstance(columns, list)
        assert 'gain' in columns
        assert 'volume_gain' in columns
        assert 'collection_performance' in columns


class TestRelationCollectionStock:
    """Tests for RelationCollectionStock model."""

    def test_relation_creation(self, db_session, sample_market, sample_stock, sample_collection):
        """Test creating a collection-stock relation."""
        db_session.add(sample_market)
        db_session.add(sample_stock)
        db_session.add(sample_collection)
        db_session.commit()

        relation = RelationCollectionStock(
            collection_code="BK0001",
            stock_code="600000"
        )
        db_session.add(relation)
        db_session.commit()

        retrieved = db_session.query(RelationCollectionStock).first()
        assert retrieved.collection_code == "BK0001"
        assert retrieved.stock_code == "600000"
