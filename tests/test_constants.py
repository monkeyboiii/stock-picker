"""
Tests for constant modules in app/constant/
"""
import pytest
from datetime import date, timedelta

from app.constant.collection import CollectionType
from app.constant.exchange import MARKET_SUPPORTED
from app.constant.schedule import (
    is_stock_market_open,
    previous_trade_day,
    next_trade_day,
    CHINA_MAINLAND_HOLIDAYS
)


class TestCollectionType:
    """Tests for CollectionType enum."""

    def test_collection_type_values(self):
        """Test CollectionType enum values."""
        assert CollectionType.ANALYST.value == 'a'
        assert CollectionType.BOARD.value == 'b'
        assert CollectionType.CONCEPT_BOARD.value == 'c'
        assert CollectionType.INDUSTRY_BOARD.value == 'i'
        assert CollectionType.INDEX.value == 'x'

    def test_collection_type_members(self):
        """Test that all expected members exist."""
        expected_types = {'ANALYST', 'BOARD', 'CONCEPT_BOARD', 'INDUSTRY_BOARD', 'INDEX'}
        actual_types = {member.name for member in CollectionType}
        assert actual_types == expected_types


class TestExchange:
    """Tests for exchange constants."""

    def test_market_supported_exists(self):
        """Test that MARKET_SUPPORTED is defined."""
        assert MARKET_SUPPORTED is not None
        assert isinstance(MARKET_SUPPORTED, (list, tuple, set))


class TestSchedule:
    """Tests for trading day schedule functions."""

    def test_weekends_not_trading_days(self):
        """Test that weekends are not trading days."""
        # Saturday
        saturday = date(2025, 11, 15)
        assert saturday.weekday() == 5
        assert not is_stock_market_open(saturday)

        # Sunday
        sunday = date(2025, 11, 16)
        assert sunday.weekday() == 6
        assert not is_stock_market_open(sunday)

    def test_weekdays_are_trading_days(self):
        """Test that regular weekdays are trading days."""
        # Monday - normal trading day
        monday = date(2025, 11, 17)
        assert monday.weekday() == 0
        assert is_stock_market_open(monday)

        # Wednesday - normal trading day
        wednesday = date(2025, 11, 19)
        assert wednesday.weekday() == 2
        assert is_stock_market_open(wednesday)

    def test_holidays_not_trading_days(self):
        """Test that Chinese holidays are not trading days."""
        # New Year 2025
        assert not is_stock_market_open(date(2025, 1, 1))

        # Chinese New Year 2025
        assert not is_stock_market_open(date(2025, 1, 29))
        assert not is_stock_market_open(date(2025, 2, 4))

        # National Day 2025
        assert not is_stock_market_open(date(2025, 10, 1))

    def test_previous_trade_day_inclusive(self):
        """Test previous_trade_day with inclusive=True."""
        # Monday - should return Monday itself
        monday = date(2025, 11, 17)
        assert previous_trade_day(monday, inclusive=True) == monday

        # Sunday - should return previous Friday
        sunday = date(2025, 11, 16)
        expected_friday = date(2025, 11, 14)
        assert previous_trade_day(sunday, inclusive=True) == expected_friday

    def test_previous_trade_day_exclusive(self):
        """Test previous_trade_day with inclusive=False."""
        # Monday - should return previous Friday
        monday = date(2025, 11, 17)
        expected_friday = date(2025, 11, 14)
        assert previous_trade_day(monday, inclusive=False) == expected_friday

        # Wednesday - should return Tuesday
        wednesday = date(2025, 11, 19)
        expected_tuesday = date(2025, 11, 18)
        assert previous_trade_day(wednesday, inclusive=False) == expected_tuesday

    def test_previous_trade_day_skips_holidays(self):
        """Test that previous_trade_day skips holidays."""
        # Day after Chinese New Year holiday
        after_cny = date(2025, 2, 5)  # First trading day after CNY
        # Should skip back through the holiday period
        result = previous_trade_day(after_cny, inclusive=False)
        # Should be before the holiday started
        assert result < date(2025, 1, 28)

    def test_next_trade_day_inclusive(self):
        """Test next_trade_day with inclusive=True."""
        # Monday - should return Monday itself
        monday = date(2025, 11, 17)
        assert next_trade_day(monday, inclusive=True) == monday

        # Saturday - should return next Monday
        saturday = date(2025, 11, 15)
        expected_monday = date(2025, 11, 17)
        assert next_trade_day(saturday, inclusive=True) == expected_monday

    def test_next_trade_day_exclusive(self):
        """Test next_trade_day with inclusive=False."""
        # Monday - should return Tuesday
        monday = date(2025, 11, 17)
        expected_tuesday = date(2025, 11, 18)
        assert next_trade_day(monday, inclusive=False) == expected_tuesday

        # Friday - should skip to next Monday
        friday = date(2025, 11, 14)
        expected_monday = date(2025, 11, 17)
        assert next_trade_day(friday, inclusive=False) == expected_monday

    def test_next_trade_day_skips_holidays(self):
        """Test that next_trade_day skips holidays."""
        # Day before Chinese New Year
        before_cny = date(2025, 1, 27)  # Day before holiday starts
        result = next_trade_day(before_cny, inclusive=False)
        # Should skip through the entire holiday
        assert result > date(2025, 2, 4)

    def test_holiday_set_not_empty(self):
        """Test that CHINA_MAINLAND_HOLIDAYS is populated."""
        assert len(CHINA_MAINLAND_HOLIDAYS) > 0
        # Verify it contains date objects
        for holiday in list(CHINA_MAINLAND_HOLIDAYS)[:5]:
            assert isinstance(holiday, date)

    def test_consecutive_previous_trade_days(self):
        """Test finding consecutive previous trading days."""
        start_day = date(2025, 11, 20)  # Thursday
        day1 = previous_trade_day(start_day, inclusive=True)
        day2 = previous_trade_day(day1, inclusive=False)
        day3 = previous_trade_day(day2, inclusive=False)

        assert day1 == date(2025, 11, 20)  # Thursday
        assert day2 == date(2025, 11, 19)  # Wednesday
        assert day3 == date(2025, 11, 18)  # Tuesday

    def test_consecutive_next_trade_days(self):
        """Test finding consecutive next trading days."""
        start_day = date(2025, 11, 18)  # Tuesday
        day1 = next_trade_day(start_day, inclusive=True)
        day2 = next_trade_day(day1, inclusive=False)
        day3 = next_trade_day(day2, inclusive=False)

        assert day1 == date(2025, 11, 18)  # Tuesday
        assert day2 == date(2025, 11, 19)  # Wednesday
        assert day3 == date(2025, 11, 20)  # Thursday

    @pytest.mark.parametrize("test_date,expected", [
        (date(2024, 8, 4), False),   # Sunday
        (date(2024, 9, 17), False),  # Mid-Autumn Festival
        (date(2025, 2, 4), False),   # Chinese New Year
        (date(2025, 2, 8), False),   # Chinese New Year (weekend recovery)
    ])
    def test_specific_non_trading_days(self, test_date, expected):
        """Test specific dates that should not be trading days."""
        assert is_stock_market_open(test_date) == expected
