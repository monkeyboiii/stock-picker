"""
Trading Constants - Eliminates magic numbers throughout the codebase

This module centralizes all trading-related constants to improve code maintainability
and make configuration changes easier.
"""

from decimal import Decimal

# Commission Rates
DEFAULT_COMMISSION_RATE = Decimal("0.0003")  # 0.03% - Standard broker commission in Chinese markets
MIN_COMMISSION_RATE = Decimal("0.0")         # 0% - For testing or zero-commission scenarios
MAX_COMMISSION_RATE = Decimal("0.01")        # 1% - Maximum reasonable commission rate

# Slippage Rates
DEFAULT_SLIPPAGE_RATE = Decimal("0.001")     # 0.1% - Standard market slippage
MIN_SLIPPAGE_RATE = Decimal("0.0")           # 0% - For testing or perfect execution
MAX_SLIPPAGE_RATE = Decimal("0.05")          # 5% - Maximum reasonable slippage

# Capital Constraints
DEFAULT_INITIAL_CAPITAL = Decimal("1000000.00")  # 1,000,000 CNY - Default backtest capital
MIN_INITIAL_CAPITAL = Decimal("1000.00")         # 1,000 CNY - Minimum viable capital
MAX_INITIAL_CAPITAL = Decimal("1000000000.00")   # 1 billion CNY - Maximum for validation
MIN_TRADE_AMOUNT = Decimal("1000.00")            # 1,000 CNY - Minimum single trade amount

# Position Management
DEFAULT_MAX_POSITIONS = 10                    # Maximum concurrent positions
MIN_MAX_POSITIONS = 1                         # At least one position allowed
MAX_MAX_POSITIONS = 100                       # Upper limit for diversification

# Risk Management
DEFAULT_RISK_FREE_RATE = Decimal("0.03")     # 3% - Chinese 10-year treasury bond yield
DEFAULT_MAX_POSITION_SIZE = Decimal("0.20")  # 20% - Maximum % of capital per position
DEFAULT_STOP_LOSS_PCT = Decimal("0.10")      # 10% - Default stop loss threshold
DEFAULT_TAKE_PROFIT_PCT = Decimal("0.20")    # 20% - Default take profit threshold

# Performance Metrics
ANNUAL_TRADING_DAYS = 250                     # Approximate trading days per year in Chinese markets
SHARPE_RATIO_SCALING = Decimal("252") ** Decimal("0.5")  # Annualization factor for Sharpe ratio

# Date Range Constraints
MAX_BACKTEST_YEARS = 10                       # Maximum backtest period in years
MIN_BACKTEST_DAYS = 1                         # Minimum backtest period in days

# Stock Filter Thresholds (from tail_scraper.py)
QUANTITY_RELATIVE_RATIO_MIN = Decimal("1.0")  # T2: Minimum quantity relative ratio
TURNOVER_RATE_MIN = Decimal("5.0")            # T3: Minimum turnover rate (%)
CIRCULATION_CAPITAL_MIN = Decimal("200000")   # T4: Minimum circulation capital (万元 = 10k CNY)
CIRCULATION_CAPITAL_MAX = Decimal("20000000") # T4: Maximum circulation capital (万元 = 10k CNY)

# Pagination
DEFAULT_PAGE_SIZE = 20                        # Default items per page
MAX_PAGE_SIZE = 100                           # Maximum items per page
MIN_PAGE_SIZE = 1                             # Minimum items per page

# Cache TTL (seconds)
CACHE_TTL_SHORT = 300                         # 5 minutes - For volatile data
CACHE_TTL_MEDIUM = 3600                       # 1 hour - For moderately stable data
CACHE_TTL_LONG = 86400                        # 24 hours - For stable data

# API Rate Limiting
RATE_LIMIT_PER_MINUTE = 100                   # Requests per minute per IP
RATE_LIMIT_PER_HOUR = 1000                    # Requests per hour per IP

# Moving Averages
MA_250_DAYS = 250                             # 250-day moving average period
