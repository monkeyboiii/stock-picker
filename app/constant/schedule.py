from datetime import date, timedelta


CHINA_MAINLAND_HOLIDAYS = set([
    # New Year's Day
    date(2022, 12, 31),
    date(2023,  1,  1),
    date(2023,  1,  2),

    # Chinese New Year
    date(2023,  1, 21),
    date(2023,  1, 22),
    date(2023,  1, 23),
    date(2023,  1, 24),
    date(2023,  1, 25),
    date(2023,  1, 26),
    date(2023,  1, 27),
    date(2023,  1, 28),
    date(2023,  1, 29),
    
    # Qingming Festival
    date(2023,  4,  5),

    # Chinese Laybor Day
    date(2023,  4, 23),
    date(2023,  4, 29),
    date(2023,  4, 30),
    date(2023,  5,  1),
    date(2023,  5,  2),
    date(2023,  5,  3),
    date(2023,  5,  6),
    
    # Dragon Boat Festival
    date(2023,  6, 22),
    date(2023,  6, 23),
    
    # China's National Day and Mid-Autumn Festival
    date(2023,  9, 29),
    date(2023,  9, 30),
    date(2023, 10,  1),
    date(2023, 10,  2),
    date(2023, 10,  3),
    date(2023, 10,  4),
    date(2023, 10,  5),
    date(2023, 10,  6),
    date(2023, 10,  7),
    date(2023, 10,  8),
    
    ####################################################################################
    
    # New Year's Day
    date(2023, 12, 31),
    date(2024,  1,  1),

    # Chinese New Year    
    date(2024,  2,  4),
    date(2024,  2,  9),
    date(2024,  2, 10),
    date(2024,  2, 11),
    date(2024,  2, 12),
    date(2024,  2, 13),
    date(2024,  2, 14),
    date(2024,  2, 15),
    date(2024,  2, 16),
    date(2024,  2, 17),
    date(2024,  2, 18),

    # Qingming Festival
    date(2024,  4,  4),
    date(2024,  4,  5),
    date(2024,  4,  6),
    date(2024,  4,  7),

    # Chinese Laybor Day
    date(2024,  4, 28),
    date(2024,  5,  1),
    date(2024,  5,  2),
    date(2024,  5,  3),
    date(2024,  5,  4),
    date(2024,  5,  5),
    date(2024,  5,  11),
    
    # Dragon Boat Festival
    date(2024,  6, 10),
    
    # Mid-Autumn Festival
    date(2024,  9, 14),
    date(2024,  9, 15),
    date(2024,  9, 17),
    
    # China's National Day
    date(2024,  9, 29),
    date(2024, 10,  1),
    date(2024, 10,  2),
    date(2024, 10,  3),
    date(2024, 10,  4),
    date(2024, 10,  5),
    date(2024, 10,  6),
    date(2024, 10,  7),
    date(2024, 10, 12),
    
    ####################################################################################
    
    # New Year's Day
    date(2025,  1,  1),

    # Chinese New Year    
    date(2025,  1, 26),
    date(2025,  1, 28),
    date(2025,  1, 29),
    date(2025,  1, 30),
    date(2025,  1, 31),
    date(2025,  2,  1),
    date(2025,  2,  2),
    date(2025,  2,  3),
    date(2025,  2,  4),
    date(2025,  2,  8),

    # Qingming Festival
    date(2025,  4,  4),
    date(2025,  4,  5),
    date(2025,  4,  6),

    # Chinese Laybor Day
    date(2025,  4, 27),
    date(2025,  5,  1),
    date(2025,  5,  2),
    date(2025,  5,  3),
    date(2025,  5,  4),
    date(2025,  5,  5),
    
    # Dragon Boat Festival
    date(2025,  5, 31),
    date(2025,  6,  1),
    date(2025,  6,  2),
    
    # China's National Day and Mid-Autumn Festival
    date(2025,  9, 28),
    date(2025, 10,  1),
    date(2025, 10,  2),
    date(2025, 10,  3),
    date(2025, 10,  4),
    date(2025, 10,  5),
    date(2025, 10,  6),
    date(2025, 10,  7),
    date(2025, 10,  8),
    date(2025, 10, 11),
])


def is_stock_market_open(day: date):
    '''
    Checks from 2023-01-01 to 2025-12-31.
    '''

    return day.weekday() < 5 and day not in CHINA_MAINLAND_HOLIDAYS


def previous_trade_day(day: date, inclusive = True) -> date:
    if not inclusive:
        day = day - timedelta(days=1)

    while not is_stock_market_open(day):
        day = day - timedelta(days=1)
    
    return day


def next_trade_day(day: date, inclusive = True) -> date:
    if not inclusive:
        day = day + timedelta(days=1)

    while not is_stock_market_open(day):
        day = day + timedelta(days=1)
    
    return day


if __name__ == '__main__':
    assert not is_stock_market_open(date(2024, 8, 4))
    assert not is_stock_market_open(date(2024, 9, 17))
    assert not is_stock_market_open(date(2025, 2, 4))
    assert not is_stock_market_open(date(2025, 2, 8))