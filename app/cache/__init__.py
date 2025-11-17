"""
Caching Layer - Redis-based caching for frequently accessed data

This module provides caching functionality for:
- Backtest results
- Strategy configurations
- Stock daily data
- Analytics calculations
"""

from app.cache.redis_cache import RedisCache, cache_manager, get_cache


__all__ = [
    "RedisCache",
    "cache_manager",
    "get_cache",
]
