"""
Redis Cache Manager

Production-ready Redis caching with TTL, invalidation, and connection pooling.
"""

import json
import os
from typing import Any, Optional

from loguru import logger


# Redis optional dependency
try:
    import redis
    from redis import ConnectionPool
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("Redis not installed. Install with: pip install redis")


class RedisCache:
    """Redis cache manager with connection pooling"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        prefix: str = "stock_picker:",
        default_ttl: int = 3600,  # 1 hour
    ):
        """
        Initialize Redis cache

        Args:
            host: Redis host
            port: Redis port
            db: Redis database number
            password: Redis password (optional)
            prefix: Key prefix for namespacing
            default_ttl: Default TTL in seconds
        """
        self.prefix = prefix
        self.default_ttl = default_ttl
        self.enabled = REDIS_AVAILABLE

        if not REDIS_AVAILABLE:
            logger.warning("Redis caching disabled - redis package not installed")
            self.client = None
            return

        try:
            # Create connection pool
            pool = ConnectionPool(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=True,
                max_connections=50,
            )

            # Create Redis client
            self.client = redis.Redis(connection_pool=pool)

            # Test connection
            self.client.ping()
            logger.info(f"Redis cache connected: {host}:{port}/{db}")

        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            logger.warning("Cache operations will be disabled")
            self.enabled = False
            self.client = None

    def _make_key(self, key: str) -> str:
        """Create prefixed cache key"""
        return f"{self.prefix}{key}"

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found
        """
        if not self.enabled or not self.client:
            return None

        try:
            value = self.client.get(self._make_key(key))
            if value is None:
                return None

            # Deserialize JSON
            return json.loads(value)

        except Exception as e:
            logger.error(f"Cache get error for key {key}: {e}")
            return None

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Set value in cache

        Args:
            key: Cache key
            value: Value to cache (must be JSON serializable)
            ttl: Time to live in seconds (default: use default_ttl)

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            # Serialize to JSON
            serialized = json.dumps(value, default=str)

            # Set with TTL
            ttl = ttl or self.default_ttl
            self.client.setex(
                self._make_key(key),
                ttl,
                serialized
            )

            return True

        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")
            return False

    def delete(self, key: str) -> bool:
        """
        Delete key from cache

        Args:
            key: Cache key

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            self.client.delete(self._make_key(key))
            return True

        except Exception as e:
            logger.error(f"Cache delete error for key {key}: {e}")
            return False

    def invalidate_pattern(self, pattern: str) -> int:
        """
        Invalidate all keys matching pattern

        Args:
            pattern: Key pattern (e.g., "backtest:*")

        Returns:
            Number of keys deleted
        """
        if not self.enabled or not self.client:
            return 0

        try:
            keys = self.client.keys(self._make_key(pattern))
            if keys:
                return self.client.delete(*keys)
            return 0

        except Exception as e:
            logger.error(f"Cache invalidate error for pattern {pattern}: {e}")
            return 0

    def clear(self) -> bool:
        """
        Clear all cache keys with prefix

        Returns:
            True if successful, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            keys = self.client.keys(self._make_key("*"))
            if keys:
                self.client.delete(*keys)
            return True

        except Exception as e:
            logger.error(f"Cache clear error: {e}")
            return False

    def exists(self, key: str) -> bool:
        """
        Check if key exists in cache

        Args:
            key: Cache key

        Returns:
            True if key exists, False otherwise
        """
        if not self.enabled or not self.client:
            return False

        try:
            return bool(self.client.exists(self._make_key(key)))

        except Exception as e:
            logger.error(f"Cache exists error for key {key}: {e}")
            return False

    def get_stats(self) -> dict:
        """
        Get cache statistics

        Returns:
            Dictionary with cache stats
        """
        if not self.enabled or not self.client:
            return {"enabled": False}

        try:
            info = self.client.info("stats")
            return {
                "enabled": True,
                "total_connections_received": info.get("total_connections_received", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "used_memory_human": self.client.info("memory").get("used_memory_human", "N/A"),
            }

        except Exception as e:
            logger.error(f"Cache stats error: {e}")
            return {"enabled": True, "error": str(e)}


# Global cache manager instance
cache_manager: Optional[RedisCache] = None


def get_cache() -> RedisCache:
    """
    Get global cache manager instance

    Returns:
        RedisCache instance
    """
    global cache_manager

    if cache_manager is None:
        # Initialize from environment variables
        cache_manager = RedisCache(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            password=os.getenv("REDIS_PASSWORD"),
            prefix=os.getenv("REDIS_PREFIX", "stock_picker:"),
            default_ttl=int(os.getenv("REDIS_DEFAULT_TTL", "3600")),
        )

    return cache_manager


# Cache key patterns for different data types
CACHE_KEYS = {
    "backtest_run": "backtest:run:{run_id}",
    "backtest_results": "backtest:results:{run_id}",
    "strategy": "strategy:{strategy_id}",
    "stock_daily": "stock:daily:{code}:{date}",
    "stock_daily_range": "stock:daily_range:{code}:{start}:{end}",
    "analytics": "analytics:{run_id}",
    "comparison": "comparison:{comparison_id}",
    "chart_data": "chart:{run_id}:{chart_type}",
}


def cache_key(pattern: str, **kwargs) -> str:
    """
    Generate cache key from pattern

    Args:
        pattern: Key pattern name
        **kwargs: Values to format into pattern

    Returns:
        Formatted cache key

    Example:
        cache_key("backtest_run", run_id="123")
        # Returns: "backtest:run:123"
    """
    if pattern not in CACHE_KEYS:
        raise ValueError(f"Unknown cache key pattern: {pattern}")

    return CACHE_KEYS[pattern].format(**kwargs)
