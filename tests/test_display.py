"""
Tests for display utilities in app/display/utils.py
"""
import pytest
import pandas as pd
from gspread_formatting import Color

from app.display.utils import ten_thousand_format, get_color_for_column


class TestTenThousandFormat:
    """Tests for ten_thousand_format function."""

    def test_format_basic_numbers(self):
        """Test formatting basic numbers."""
        assert ten_thousand_format(123456789) == '1,2345,6789'
        assert ten_thousand_format(12345) == '1,2345'
        assert ten_thousand_format(1234) == '1234'
        assert ten_thousand_format(123) == '123'

    def test_format_negative_numbers(self):
        """Test formatting negative numbers."""
        assert ten_thousand_format(-123456789) == '-1,2345,6789'
        assert ten_thousand_format(-12345) == '-1,2345'
        assert ten_thousand_format(-1234) == '-1234'

    def test_format_zero(self):
        """Test formatting zero."""
        assert ten_thousand_format(0) == '0'

    def test_format_float_numbers(self):
        """Test formatting floating point numbers."""
        assert ten_thousand_format(12345.67) == '1,2345.67'
        assert ten_thousand_format(123456789.123) == '1,2345,6789.123'
        assert ten_thousand_format(-12345.67) == '-1,2345.67'

    def test_format_small_numbers(self):
        """Test formatting numbers less than 10000."""
        assert ten_thousand_format(9999) == '9999'
        assert ten_thousand_format(1000) == '1000'
        assert ten_thousand_format(100) == '100'
        assert ten_thousand_format(10) == '10'
        assert ten_thousand_format(1) == '1'

    def test_format_large_numbers(self):
        """Test formatting very large numbers."""
        assert ten_thousand_format(1234567890123) == '1,2345,6789,0123'
        assert ten_thousand_format(99999999) == '9999,9999'

    def test_format_edge_cases(self):
        """Test edge cases."""
        # Single digit
        assert ten_thousand_format(5) == '5'
        # Exactly 4 digits
        assert ten_thousand_format(1000) == '1000'
        # Exactly 5 digits
        assert ten_thousand_format(10000) == '1,0000'
        # Exactly 8 digits
        assert ten_thousand_format(10000000) == '1000,0000'


class TestGetColorForColumn:
    """Tests for get_color_for_column function."""

    def test_color_for_positive_values(self):
        """Test color generation for positive values."""
        series = pd.Series([10, 20, 30])
        colors = get_color_for_column(series)

        assert len(colors) == 3
        # All should be Color objects
        for color in colors:
            assert isinstance(color, Color)

        # For positive values, red should be 1.0
        for color in colors:
            assert color.red == 1.0

    def test_color_for_negative_values(self):
        """Test color generation for negative values."""
        series = pd.Series([-10, -20, -30])
        colors = get_color_for_column(series)

        assert len(colors) == 3
        # For negative values, green should be 1.0
        for color in colors:
            assert color.green == 1.0

    def test_color_for_zero(self):
        """Test color generation for zero values."""
        series = pd.Series([0])
        colors = get_color_for_column(series)

        # Zero should return white (1.0, 1.0, 1.0)
        assert colors[0].red == 1.0
        assert colors[0].green == 1.0
        assert colors[0].blue == 1.0

    def test_color_for_mixed_values(self):
        """Test color generation for mixed positive and negative values."""
        series = pd.Series([-10, 0, 10])
        colors = get_color_for_column(series)

        assert len(colors) == 3

        # Negative value: green = 1.0
        assert colors[0].green == 1.0

        # Zero: white
        assert colors[1].red == 1.0
        assert colors[1].green == 1.0
        assert colors[1].blue == 1.0

        # Positive value: red = 1.0
        assert colors[2].red == 1.0

    def test_color_intensity_increases_with_value(self):
        """Test that color intensity changes with value magnitude."""
        series = pd.Series([10, 50, 100])
        colors = get_color_for_column(series)

        # For positive values, as value increases, green and blue decrease
        # So the smallest value should have highest green/blue
        assert colors[0].green > colors[1].green
        assert colors[1].green > colors[2].green

    def test_color_for_all_same_values(self):
        """Test color generation when all values are the same."""
        series = pd.Series([10, 10, 10])
        colors = get_color_for_column(series)

        # When all values are same (max == min), fraction is 0
        # Should return white for all
        for color in colors:
            assert color.red == 1.0
            assert color.green == 1.0
            assert color.blue == 1.0

    def test_color_range_values(self):
        """Test that color values are in valid range [0, 1]."""
        series = pd.Series([i for i in range(-100, 101, 10)])
        colors = get_color_for_column(series)

        for color in colors:
            assert 0 <= color.red <= 1.0
            assert 0 <= color.green <= 1.0
            assert 0 <= color.blue <= 1.0

    def test_color_for_float_series(self):
        """Test color generation with float values."""
        series = pd.Series([1.5, 2.7, 3.9, 5.1])
        colors = get_color_for_column(series)

        assert len(colors) == 4
        for color in colors:
            assert isinstance(color, Color)

    def test_color_symmetry(self):
        """Test that positive and negative values produce symmetric colors."""
        # Positive series
        pos_series = pd.Series([10, 20, 30])
        pos_colors = get_color_for_column(pos_series)

        # Negative series (same magnitudes)
        neg_series = pd.Series([-10, -20, -30])
        neg_colors = get_color_for_column(neg_series)

        # The patterns should be symmetric (though channels differ)
        # Positive: red=1.0, varying green/blue
        # Negative: green=1.0, varying red/blue
        assert pos_colors[0].red == 1.0
        assert neg_colors[0].green == 1.0
