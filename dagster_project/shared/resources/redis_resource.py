"""
Redis Resource for caching frequently accessed data.

This module provides:
1. RedisClient - Standalone client for Redis operations
2. RedisResource - Dagster-aware resource wrapper
3. Cache decorator for query results
4. Support for parquet and JSON data
5. TTL management for different data types
6. Rate limiting capabilities
"""

import io
import json
import pickle
import time
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
import redis
from dagster import ConfigurableResource
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
)
from redis.exceptions import (
    RedisError,
)
from redis.exceptions import (
    TimeoutError as RedisTimeoutError,
)

from src.config.logging import get_logger
from src.config.settings import RedisConfig


class CacheDataType(Enum):
    """Supported cache data types with default TTLs."""

    PARQUET = "parquet"
    JSON = "json"
    QUERY_RESULT = "query_result"
    FILE = "file"
    STRING = "string"


DEFAULT_TTLS = {
    CacheDataType.PARQUET: 3600,  # 1 hour
    CacheDataType.JSON: 1800,  # 30 minutes
    CacheDataType.QUERY_RESULT: 600,  # 10 minutes
    CacheDataType.FILE: 7200,  # 2 hours
    CacheDataType.STRING: 300,  # 5 minutes
}


# =============================================================================
# Redis Client
# =============================================================================


class RedisClient:
    """
    Standalone client for Redis caching operations.

    Supports caching files (parquet, JSON), query results, and arbitrary data
    with configurable TTLs and cache invalidation strategies.
    """

    def __init__(
        self,
        config: RedisConfig,
        socket_timeout: int = 5,
        socket_connect_timeout: int = 5,
        max_connections: int = 50,
        decode_responses: bool = False,
        max_retries: int = 3,
        retry_delay: float = 0.5,
        key_prefix: str = "f1:",
    ):
        """
        Initialize RedisClient.

        Args:
            config: contains Redis host, Redis port,
                    Redis database number (0-15) and Redis password (if required)
            socket_timeout: Socket timeout in seconds
            socket_connect_timeout: Connection timeout in seconds
            max_connections: Maximum connections in pool
            decode_responses: Decode responses to strings (False for binary data)
            max_retries: Maximum retry attempts
            retry_delay: Initial delay between retries
            key_prefix: Prefix for all cache keys (for namespacing)
        """

        self.host = config.host
        self.port = config.port
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.key_prefix = key_prefix
        self.logger = get_logger("resources.redis")

        # Create connection pool
        self.pool = redis.ConnectionPool(
            host=config.host,
            port=config.port,
            db=config.db,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            max_connections=max_connections,
            decode_responses=decode_responses,
        )

        # Create Redis client with connection pool
        self.client = redis.Redis(connection_pool=self.pool)

        self.logger.info(
            "RedisClient initialized (host=%s, port=%s, db=%s)",
            config.host,
            config.port,
            config.db,
        )

    @classmethod
    def from_env(cls):
        """Factory to create RedisClient from environment variables."""

        return cls(config=RedisConfig.from_env())

    def _build_key(self, key: str, data_type: Optional[CacheDataType] = None) -> str:
        """
        Build a namespaced cache key.

        Args:
            key: Base key
            data_type: Data type (adds type prefix)

        Returns:
            Namespaced key with prefix
        """

        if data_type:
            return f"{self.key_prefix}{data_type.value}:{key}"

        return f"{self.key_prefix}{key}"

    def _retry_with_backoff(self, operation, *args, **kwargs):
        """
        Execute operation with exponential backoff retry logic.

        Args:
            operation: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Result of the operation

        Raises:
            Exception: If all retry attempts fail
        """

        last_exception = None

        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            except (RedisConnectionError, RedisTimeoutError, RedisError) as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)
                    self.logger.warning(
                        "Attempt %d/%d failed: %s. Retrying in %d s...",
                        attempt + 1,
                        self.max_retries,
                        str(e),
                        delay,
                    )
                    time.sleep(delay)
                else:
                    self.logger.error(
                        "All %d attempts failed for operation", self.max_retries
                    )

        raise last_exception

    def ping(self) -> bool:
        """
        Test Redis connection.

        Returns:
            True if connection successful, False otherwise
        """

        try:
            return self.client.ping()

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Redis ping failed: %s", str(e))
            return False

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        data_type: CacheDataType = CacheDataType.STRING,
    ) -> bool:
        """
        Set a value in cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (None for no expiration)
            data_type: Type of data being cached

        Returns:
            True if successful, False otherwise
        """

        try:
            cache_key = self._build_key(key, data_type)

            # Serialize value based on type
            if isinstance(value, (dict, list)):
                serialized = json.dumps(value).encode("utf-8")
            elif isinstance(value, pd.DataFrame):
                buffer = io.BytesIO()
                value.to_parquet(buffer)
                serialized = buffer.getvalue()
            elif isinstance(value, str):
                serialized = value.encode("utf-8")
            elif isinstance(value, bytes):
                serialized = value
            else:
                # Fallback to pickle for other types
                serialized = pickle.dumps(value)

            # Use default TTL if not specified
            if ttl is None:
                ttl = DEFAULT_TTLS.get(data_type)

            def _set():
                if ttl:
                    return self.client.setex(cache_key, ttl, serialized)
                else:
                    return self.client.set(cache_key, serialized)

            result = self._retry_with_backoff(_set)

            self.logger.info(
                "Cached '%s' (type=%s, ttl=%d s)",
                key,
                data_type.value,
                ttl,
            )
            return bool(result)

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to cache '%s': %s", key, str(e))
            return False

    def get(
        self,
        key: str,
        data_type: CacheDataType = CacheDataType.STRING,
        return_type: Optional[type] = None,
    ) -> Optional[Any]:
        """
        Get a value from cache.

        Args:
            key: Cache key
            data_type: Type of cached data
            return_type: Expected return type (dict, pd.DataFrame, str, bytes)

        Returns:
            Cached value or None if not found
        """

        try:
            cache_key = self._build_key(key, data_type)

            def _get():
                return self.client.get(cache_key)

            serialized = self._retry_with_backoff(_get)

            if serialized is None:
                self.logger.debug("Cache miss: '%s'", key)
                return None

            # Deserialize based on type
            if return_type is pd.DataFrame or data_type == CacheDataType.PARQUET:
                value = pd.read_parquet(io.BytesIO(serialized))
            elif return_type is dict or data_type == CacheDataType.JSON:
                value = json.loads(serialized.decode("utf-8"))
            elif return_type is str:
                value = serialized.decode("utf-8")
            elif return_type is bytes:
                value = serialized
            else:
                # Try JSON first, fallback to pickle
                try:
                    value = json.loads(serialized.decode("utf-8"))
                except:  # pylint: disable=bare-except  # noqa: E722
                    try:
                        value = pickle.loads(serialized)
                    except:  # pylint: disable=bare-except  # noqa: E722
                        value = serialized

            self.logger.debug("Cache hit: '%s'", key)
            return value

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to retrieve '%s': %s", key, str(e))
            return None

    def delete(self, key: str, data_type: Optional[CacheDataType] = None) -> bool:
        """
        Delete a key from cache.

        Args:
            key: Cache key
            data_type: Type of cached data (for namespacing)

        Returns:
            True if deleted, False otherwise
        """

        try:
            cache_key = self._build_key(key, data_type)

            def _delete():
                return self.client.delete(cache_key)

            result = self._retry_with_backoff(_delete)

            if result:
                self.logger.info("Deleted cache key: '%s'", key)

            return bool(result)

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to delete '%s': %s", key, str(e))
            return False

    def exists(self, key: str, data_type: Optional[CacheDataType] = None) -> bool:
        """
        Check if a key exists in cache.

        Args:
            key: Cache key
            data_type: Type of cached data

        Returns:
            True if exists, False otherwise
        """

        try:
            cache_key = self._build_key(key, data_type)
            return bool(self.client.exists(cache_key))

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to check existence of '%s': %s", key, str(e))
            return False

    def get_ttl(self, key: str, data_type: Optional[CacheDataType] = None) -> int:
        """
        Get remaining TTL for a key.

        Args:
            key: Cache key
            data_type: Type of cached data

        Returns:
            Remaining TTL in seconds, -1 if no expiration, -2 if key doesn't exist
        """

        try:
            cache_key = self._build_key(key, data_type)
            return self.client.ttl(cache_key)

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to get TTL for '%s': %s", key, str(e))
            return -2

    def expire(
        self, key: str, ttl: int, data_type: Optional[CacheDataType] = None
    ) -> bool:
        """
        Set expiration time for a key.

        Args:
            key: Cache key
            ttl: Time-to-live in seconds
            data_type: Type of cached data

        Returns:
            True if successful, False otherwise
        """

        try:
            cache_key = self._build_key(key, data_type)
            return bool(self.client.expire(cache_key, ttl))

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to set expiration for '%s': %s", key, str(e))
            return False

    def cache_parquet(
        self,
        key: str,
        df: pd.DataFrame,
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Cache a pandas DataFrame as parquet.

        Args:
            key: Cache key
            df: DataFrame to cache
            ttl: Time-to-live in seconds

        Returns:
            True if successful, False otherwise
        """

        return self.set(key, df, ttl=ttl, data_type=CacheDataType.PARQUET)

    def get_parquet(self, key: str) -> Optional[pd.DataFrame]:
        """
        Get a cached DataFrame.

        Args:
            key: Cache key

        Returns:
            DataFrame or None if not found
        """

        return self.get(key, data_type=CacheDataType.PARQUET, return_type=pd.DataFrame)

    def cache_json(
        self,
        key: str,
        data: Union[dict, list],
        ttl: Optional[int] = None,
    ) -> bool:
        """
        Cache JSON-serializable data.

        Args:
            key: Cache key
            data: Data to cache (dict or list)
            ttl: Time-to-live in seconds

        Returns:
            True if successful, False otherwise
        """

        return self.set(key, data, ttl=ttl, data_type=CacheDataType.JSON)

    def get_json(self, key: str) -> Optional[Union[dict, list]]:
        """
        Get cached JSON data.

        Args:
            key: Cache key

        Returns:
            Dict/list or None if not found
        """

        return self.get(key, data_type=CacheDataType.JSON, return_type=dict)

    def invalidate_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching a pattern.

        Args:
            pattern: Key pattern (e.g., "race_2024_*")

        Returns:
            Number of keys deleted

        Example:
            >>> client.invalidate_pattern("race_2024_*")
            5  # Deleted 5 keys
        """

        try:
            full_pattern = self._build_key(pattern)
            keys = self.client.keys(full_pattern)

            if not keys:
                return 0

            count = self.client.delete(*keys)
            self.logger.info("Invalidated %d keys matching '%s'", count, pattern)
            return count

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to invalidate pattern '%s': %s", pattern, str(e))
            return 0

    def clear_all(self) -> bool:
        """
        Clear all keys with the configured prefix.

        Returns:
            True if successful, False otherwise

        Warning:
            This deletes ALL cached data! Use with caution.
        """

        try:
            pattern = f"{self.key_prefix}*"
            keys = self.client.keys(pattern)

            if keys:
                count = self.client.delete(*keys)
                self.logger.warning("Cleared %d keys from cache", count)
            else:
                self.logger.info("No keys to clear")

            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to clear cache: %s", str(e))
            return False

    def get_info(self) -> Dict[str, Any]:
        """
        Get Redis server information.

        Returns:
            Dictionary with server info
        """

        try:
            info = self.client.info()
            return {
                "version": info.get("redis_version"),
                "used_memory_human": info.get("used_memory_human"),
                "connected_clients": info.get("connected_clients"),
                "total_keys": self.client.dbsize(),
                "uptime_days": info.get("uptime_in_days"),
            }
        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to get Redis info: %s", str(e))
            return {}

    # ==========================================================================
    # Rate Limiting
    # ==========================================================================

    def check_rate_limit(
        self,
        identifier: str,
        max_requests: int,
        window_seconds: int,
    ) -> bool:
        """
        Check if request is within rate limit.

        Args:
            identifier: Unique identifier (e.g., user_id, ip_address)
            max_requests: Maximum requests allowed
            window_seconds: Time window in seconds

        Returns:
            True if within limit, False if exceeded

        Example:
            >>> if client.check_rate_limit("user_123", max_requests=100,
                    window_seconds=3600):
            ...     # Process request
            ...     pass
            ... else:
            ...     # Rate limit exceeded
            ...     raise Exception("Too many requests")
        """

        try:
            key = self._build_key(f"rate_limit:{identifier}")

            # Increment counter
            current = self.client.incr(key)

            # Set expiration on first request
            if current == 1:
                self.client.expire(key, window_seconds)

            # Check if limit exceeded
            if current > max_requests:
                self.logger.warning(
                    "Rate limit exceeded for '%s': %d/%d",
                    identifier,
                    current,
                    max_requests,
                )
                return False

            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Rate limit check failed: %s", str(e))
            # Fail open - allow request on error
            return True

    def get_request_count(self, identifier: str) -> int:
        """
        Get current request count for identifier.

        Args:
            identifier: Unique identifier

        Returns:
            Current request count
        """

        try:
            key = self._build_key(f"rate_limit:{identifier}")
            count = self.client.get(key)
            return int(count) if count else 0

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to get request count: %s", str(e))
            return 0

    # ==========================================================================
    # Batch Operations
    # ==========================================================================

    def batch_set(
        self,
        items: Dict[str, Any],
        ttl: Optional[int] = None,
        data_type: CacheDataType = CacheDataType.STRING,
    ) -> int:
        """
        Set multiple key-value pairs at once.

        Args:
            items: Dictionary of key-value pairs
            ttl: Time-to-live in seconds
            data_type: Type of data being cached

        Returns:
            Number of keys successfully set
        """

        success_count = 0

        for key, value in items.items():
            if self.set(key, value, ttl=ttl, data_type=data_type):
                success_count += 1

        self.logger.info("Batch set: %d/%d successful", success_count, len(items))
        return success_count

    def batch_get(
        self,
        keys: List[str],
        data_type: CacheDataType = CacheDataType.STRING,
        return_type: Optional[type] = None,
    ) -> Dict[str, Any]:
        """
        Get multiple values at once.

        Args:
            keys: List of cache keys
            data_type: Type of cached data
            return_type: Expected return type

        Returns:
            Dictionary of key-value pairs (only existing keys)
        """

        results = {}

        for key in keys:
            value = self.get(key, data_type=data_type, return_type=return_type)
            if value is not None:
                results[key] = value

        self.logger.info("Batch get: %d/%d found", len(results), len(keys))
        return results

    def batch_delete(
        self, keys: List[str], data_type: Optional[CacheDataType] = None
    ) -> int:
        """
        Delete multiple keys at once.

        Args:
            keys: List of cache keys
            data_type: Type of cached data

        Returns:
            Number of keys deleted
        """

        count = 0

        for key in keys:
            if self.delete(key, data_type=data_type):
                count += 1

        self.logger.info("Batch delete: %d/%d deleted", count, len(keys))
        return count

    def close(self):
        """Close Redis connection pool."""

        self.pool.disconnect()
        self.logger.info("Redis connection pool closed")


# =============================================================================
# Cache Decorator
# =============================================================================


def cache_query(
    ttl: int = 600,
    key_prefix: str = "query",
    data_type: CacheDataType = CacheDataType.QUERY_RESULT,
):
    """
    Decorator to cache query results.

    Args:
        ttl: Time-to-live in seconds
        key_prefix: Prefix for cache key
        data_type: Type of cached data

    Example:
        >>> @cache_query(ttl=3600, key_prefix="race_results")
        ... def get_race_results(year: int, round: int):
        ...     # Expensive database query
        ...     return db.query("races", filters={"year": year, "round": round})

        >>> # First call: queries database and caches result
        >>> results = get_race_results(2024, 1)

        >>> # Second call: returns from cache (fast!)
        >>> results = get_race_results(2024, 1)
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get Redis client from context (if available)
            # Otherwise, you'll need to pass it explicitly
            redis_client = kwargs.pop("redis_client", None)

            if redis_client is None:
                # No caching, execute function normally
                return func(*args, **kwargs)

            # Build cache key from function name and arguments
            arg_str = "_".join(str(arg) for arg in args)
            kwarg_str = "_".join(f"{k}_{v}" for k, v in sorted(kwargs.items()))
            cache_key = f"{key_prefix}:{func.__name__}:{arg_str}:{kwarg_str}"

            # Try to get from cache
            cached_result = redis_client.get(cache_key, data_type=data_type)

            if cached_result is not None:
                redis_client.logger.info(f"Cache hit for {func.__name__}")
                return cached_result

            # Cache miss - execute function
            redis_client.logger.info(f"Cache miss for {func.__name__}")
            result = func(*args, **kwargs)

            # Cache the result
            redis_client.set(cache_key, result, ttl=ttl, data_type=data_type)

            return result

        return wrapper

    return decorator


# =============================================================================
# Dagster Resource
# =============================================================================


class RedisResource(ConfigurableResource):
    """
    Dagster resource wrapper for RedisClient.

    This resource can be configured in Dagster and used across assets, ops, and jobs.
    """

    def get_client(self) -> RedisClient:
        """
        Get a RedisClient instance with the configured settings.

        Returns:
            Configured RedisClient instance
        """

        return RedisClient.from_env()

    @classmethod
    def from_env(cls):
        """Factory to create RedisResource from environment variables."""

        return cls(config=RedisConfig.from_env())
