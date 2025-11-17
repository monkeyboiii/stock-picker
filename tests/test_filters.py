"""
Tests for filter utilities in app/filter/
"""

from app.filter.misc import (
    StockFilter,
    filter_to_id,
    get_filter_canonical_name,
    get_filter_id,
    get_filter_name,
    id_to_filter,
)


class TestStockFilter:
    """Tests for StockFilter enum."""

    def test_stock_filter_enum_exists(self):
        """Test that StockFilter enum has expected members."""
        assert hasattr(StockFilter, 'TAIL_SCRAPER')
        assert StockFilter.TAIL_SCRAPER.value == 1

    def test_stock_filter_is_enum(self):
        """Test that StockFilter is an Enum."""
        from enum import Enum
        assert issubclass(StockFilter, Enum)


class TestFilterMapping:
    """Tests for filter mapping dictionaries."""

    def test_filter_to_id_mapping(self):
        """Test filter_to_id dictionary."""
        assert isinstance(filter_to_id, dict)
        assert 'TAIL_SCRAPER' in filter_to_id
        assert filter_to_id['TAIL_SCRAPER'] == 1

    def test_id_to_filter_mapping(self):
        """Test id_to_filter dictionary."""
        assert isinstance(id_to_filter, dict)
        assert 1 in id_to_filter
        assert id_to_filter[1] == 'TAIL_SCRAPER'

    def test_bidirectional_mapping(self):
        """Test that filter_to_id and id_to_filter are inverses."""
        # For each entry in filter_to_id
        for name, id_val in filter_to_id.items():
            # The corresponding id_to_filter entry should map back
            assert id_to_filter[id_val] == name

        # And vice versa
        for id_val, name in id_to_filter.items():
            assert filter_to_id[name] == id_val


class TestGetFilterId:
    """Tests for get_filter_id function."""

    def test_get_filter_id_tail_scraper(self):
        """Test getting ID for TAIL_SCRAPER filter."""
        assert get_filter_id(StockFilter.TAIL_SCRAPER) == 1

    def test_get_filter_id_returns_int(self):
        """Test that get_filter_id returns an integer."""
        result = get_filter_id(StockFilter.TAIL_SCRAPER)
        assert isinstance(result, int)


class TestGetFilterName:
    """Tests for get_filter_name function."""

    def test_get_filter_name_tail_scraper(self):
        """Test getting name for TAIL_SCRAPER filter."""
        assert get_filter_name(StockFilter.TAIL_SCRAPER) == "tail-scraper"

    def test_get_filter_name_format(self):
        """Test that filter name is properly formatted (kebab-case)."""
        result = get_filter_name(StockFilter.TAIL_SCRAPER)
        # Should be lowercase with hyphens
        assert result.islower()
        assert '-' in result
        assert '_' not in result


class TestGetFilterCanonicalName:
    """Tests for get_filter_canonical_name function."""

    def test_get_filter_canonical_name_tail_scraper(self):
        """Test getting canonical name for TAIL_SCRAPER filter."""
        assert get_filter_canonical_name(StockFilter.TAIL_SCRAPER) == "TAIL_SCRAPER"

    def test_get_filter_canonical_name_format(self):
        """Test that canonical name is UPPER_SNAKE_CASE."""
        result = get_filter_canonical_name(StockFilter.TAIL_SCRAPER)
        # Should be uppercase
        assert result.isupper()
        # Should contain underscore
        assert '_' in result


class TestFilterUtilsIntegration:
    """Integration tests for filter utility functions."""

    def test_round_trip_conversion(self):
        """Test converting filter to ID and back."""
        original_filter = StockFilter.TAIL_SCRAPER

        # Get ID
        filter_id = get_filter_id(original_filter)

        # Get canonical name from ID
        canonical_name = id_to_filter[filter_id]

        # Verify we get back to original
        assert canonical_name == original_filter.name

    def test_all_filter_functions_consistent(self):
        """Test that all filter utility functions return consistent results."""
        filter_obj = StockFilter.TAIL_SCRAPER

        # Get all representations
        id_val = get_filter_id(filter_obj)
        name = get_filter_name(filter_obj)
        canonical = get_filter_canonical_name(filter_obj)

        # Verify consistency
        assert id_val == 1
        assert name == "tail-scraper"
        assert canonical == "TAIL_SCRAPER"

        # Verify they all refer to the same filter
        assert id_to_filter[id_val] == canonical
        assert filter_to_id[canonical] == id_val
