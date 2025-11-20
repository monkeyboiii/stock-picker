"""
Caching Layer - Redis-based caching for frequently accessed data

This module provides caching functionality for:
- Backtest results
- Strategy configurations
- Stock daily data
- Analytics calculations
- Secure secrets management
"""

from app.cache.redis_cache import RedisCache, cache_manager, get_cache
from app.cache.secrets import (
    SecretsManager,
    get_secret,
    get_secrets_manager,
    get_database_credentials,
    get_logto_config,
    get_redis_config,
)


__all__ = [
    "RedisCache",
    "cache_manager",
    "get_cache",
    "SecretsManager",
    "get_secret",
    "get_secrets_manager",
    "get_database_credentials",
    "get_logto_config",
    "get_redis_config",
]
