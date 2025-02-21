from decimal import Decimal
import re
from math import log10
from typing import Optional

from pandas import DataFrame, Series
from gspread_formatting import Color # type: ignore


def ten_thousand_format(num):
    """
    Format a number so that its integer part is grouped in blocks of 4 digits.
    For example:
      123456789  -> '1,2345,6789'
      12345      -> '1,2345'
    This function also handles negative numbers and floats.
    """
    s = str(num)
    
    negative = s.startswith('-')
    if negative:
        s = s[1:]
    
    if '.' in s:
        integer_part, decimal_part = s.split('.')
    else:
        integer_part, decimal_part = s, None

    formatted_integer = re.sub(r'(?<=\d)(?=(\d{4})+$)', ',', integer_part)
    
    formatted = formatted_integer if decimal_part is None else f"{formatted_integer}.{decimal_part}"
    if negative:
        formatted = '-' + formatted
    return formatted


def get_color_for_column(series_: Series) -> Series:
    """
    Return a gspread-formatting Color based on the value.
    
    For positive values:
      - The red channel remains at 1.0.
      - The green and blue channels decrease as the value approaches max_val.
    
    For negative values:
      - The green channel remains at 1.0.
      - The red and blue channels decrease as the value approaches min_val.
    
    Zero (or near zero) will return white (1,1,1).
    """

    series = series_.map(float)

    min_val = series.min()
    max_val = series.max()

    def adjust(num: float):
        assert -1 <= num and num <= 1
        return log10(abs(num) * 2 + 1)

    def colorize(
        value: float,
        cap: Optional[int] = 1,
        threshold: Optional[float] = 0,
    ) -> Color:
        base = 1.0
        if value > 0:
            fraction = (value - min_val) / (max_val - min_val) if max_val != min_val else 0
            if fraction <= threshold:
                return Color(red=1.0, green=1.0, blue=1.0)

            red = 1.0
            green = round(base - adjust(fraction * base) * cap, 3)
            blue = round(base - adjust(fraction * base) * cap, 3)

            return Color(red=red, green=green, blue=blue)
        elif value < 0:
            fraction = (0 - value) / (0 - min_val) if min_val != 0 else 0

            if fraction <= threshold:
                return Color(red=1.0, green=1.0, blue=1.0)

            green = 1.0
            red = round(base - adjust(fraction * base) * cap, 3)
            blue = round(base - adjust(fraction * base) * cap, 3)
            
            return Color(red=red, green=green, blue=blue)
        else:
            return Color(red=1.0, green=1.0, blue=1.0)
    
    return Series([colorize(value) for value in series])


if __name__ == '__main__':
    series = Series([
        35.28, 30.28, 63.04, 79.88, 114.82, 78.21, 334.85, 31.85, 99.47, 128.54, 
        45.04, 48.79, 63.45, 20.04, 63.56, 174.84, 95.98, 28.15, 490.30, 86.22,
    ])
    color_series = get_color_for_column(series_=series)
    for idx, color in enumerate(color_series):
        if color.blue != 1.0:
            print(f"{idx:2}. blue = [{color.blue}]")