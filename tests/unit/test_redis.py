"""
Unit tests for RedisClient
"""

import io
import json
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from dagster_project.shared.resources import CacheDataType, RedisClient, cache_query
from src.config.settings import RedisConfig

# pylint: disable=unused-argument, protected-access


@pytest.mark.unit
class TestRedisClientInit:
    """Test RedisClient initialization"""

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_init_with_custom_config(
        self,
        mock_redis,
        mock_connection_pool,
        redis_custom_config_dict: dict,
    ):
        """Test initializing RedisClient with config"""

        config = RedisConfig(**redis_custom_config_dict)
        client = RedisClient(config=config)

        assert client.host == "localhost"
        assert client.port == 6378
        assert client.key_prefix == "f1:"
        assert client.max_retries == 3
        mock_redis.assert_called_once()

    def test_init_from_env(self, mock_redis_client):
        """Test initializing RedisClient from environment"""

        assert mock_redis_client.host == "redis"
        assert mock_redis_client.port == 6379
        assert mock_redis_client.key_prefix == "f1:"
        assert isinstance(mock_redis_client.client, Mock)


@pytest.mark.unit
class TestRedisClientKeyBuilding:
    """Test Redis key building"""

    def test_build_key_simple(self, mock_redis_client):
        """Test building simple key"""

        key = mock_redis_client._build_key("test_key")
        assert key == "f1:test_key"

    def test_build_key_with_type(self, mock_redis_client):
        """Test building key with data type"""

        key = mock_redis_client._build_key("test_key", CacheDataType.PARQUET)
        assert key == "f1:parquet:test_key"


@pytest.mark.unit
class TestRedisClientRetry:
    """Test Redis retry operations"""

    @patch("time.sleep")
    def test_retry_success_on_second_attempt(self, mock_sleep, mock_redis_client):
        """Test successful retry after initial failure"""

        mock_operation = MagicMock()
        mock_operation.side_effect = [
            RedisConnectionError("Error connecting to Redis"),
            "Success",
        ]

        result = mock_redis_client._retry_with_backoff(mock_operation)

        assert result == "Success"
        assert mock_operation.call_count == 2
        mock_sleep.assert_called_once()

    @patch("time.sleep")
    def test_retry_all_attempts_fail(self, mock_sleep, mock_redis_client):
        """Test successful retry after initial failure"""

        mock_operation = MagicMock()
        mock_operation.side_effect = RedisConnectionError("Error connecting to Redis")

        with pytest.raises(RedisConnectionError):
            mock_redis_client._retry_with_backoff(mock_operation)

        assert mock_operation.call_count == 3
        assert mock_sleep.call_count == 2


@pytest.mark.unit
class TestRedisClientSetGet:
    """Test Redis set/get operations"""

    def test_ping_success(self, mock_redis_client):
        """Test successful ping"""

        result = mock_redis_client.ping()
        assert result is True

    def test_set_string(self, mock_redis_client):
        """Test setting string value"""

        result = mock_redis_client.set("test_key", "test_value", ttl=3600)
        assert result is True
        mock_redis_client.client.setex.assert_called_once()
        mock_redis_client.client.set.assert_not_called()

    def test_set_dict(
        self,
        mock_redis_client,
        sample_json_data: dict,
    ):
        """Test setting dictionary value"""

        result = mock_redis_client.set("test_key", sample_json_data, ttl=3600)
        assert result is True
        mock_redis_client.client.setex.assert_called_once()
        mock_redis_client.client.set.assert_not_called()

    def test_set_dataframe(
        self,
        mock_redis_client,
        sample_schedule_df: pd.DataFrame,
    ):
        """Test setting DataFrame value"""

        result = mock_redis_client.set("test_key", sample_schedule_df, ttl=3600)
        assert result is True
        mock_redis_client.client.setex.assert_called_once()
        mock_redis_client.client.set.assert_not_called()

    def test_get_string(self, mock_redis_client):
        """Test getting string value"""

        result = mock_redis_client.get("test_key", return_type=str)
        assert result == "test_value"
        mock_redis_client.client.get.assert_called_once()

    def test_get_not_found(self, mock_redis_client):
        """Test getting non-existent key"""

        mock_redis_client.client.get.return_value = None

        result = mock_redis_client.get("nonexistent_key")
        assert result is None

    def test_get_bytes(self, mock_redis_client):
        """Test getting string value"""

        # Return raw bytes (not decoded)
        raw_bytes = b"\x89PNG\r\n\x1a\n\x00\x00\x00"  # Example binary data
        mock_redis_client.client.get.return_value = raw_bytes

        result = mock_redis_client.get("test_key", return_type=bytes)

        assert result == raw_bytes
        assert isinstance(result, bytes)

    def test_get_without_return_type(self, mock_redis_client):
        """Test getting string value"""

        # Return raw bytes (not decoded)
        expected_dict = {"key": "value", "number": 42}
        mock_redis_client.client.get.return_value = expected_dict

        result = mock_redis_client.get("test_key")

        assert result == expected_dict
        assert isinstance(result, dict)


@pytest.mark.unit
class TestRedisClientParquet:
    """Test Redis parquet operations"""

    def test_cache_parquet(self, mock_redis_client, sample_schedule_df: pd.DataFrame):
        """Test caching DataFrame as parquet"""

        result = mock_redis_client.cache_parquet(
            "test_key", sample_schedule_df, ttl=3600
        )
        assert result is True
        mock_redis_client.client.setex.assert_called_once()
        mock_redis_client.client.set.assert_not_called()

    def test_get_parquet(self, mock_redis_client, sample_schedule_df: dict):
        """Test getting cached DataFrame"""

        # Serialize DataFrame to bytes
        buffer = io.BytesIO()
        sample_schedule_df.to_parquet(buffer, index=False)
        parquet_bytes = buffer.getvalue()
        mock_redis_client.client.get.return_value = parquet_bytes

        result = mock_redis_client.get_parquet("test_key")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(sample_schedule_df)


@pytest.mark.unit
class TestRedisClientJSON:
    """Test Redis JSON operations"""

    def test_cache_json(self, mock_redis_client, sample_json_data: dict):
        """Test caching JSON data"""

        result = mock_redis_client.cache_json("test_key", sample_json_data, ttl=1800)
        assert result is True

    def test_get_json(self, mock_redis_client, sample_json_data: dict):
        """Test getting cached JSON data"""

        json_bytes = json.dumps(sample_json_data).encode("utf-8")
        mock_redis_client.client.get.return_value = json_bytes

        result = mock_redis_client.get_json("test_key")
        assert result == sample_json_data


@pytest.mark.unit
class TestRedisClientOperations:
    """Test Redis other operations"""

    def test_exists_true(self, mock_redis_client):
        """Test checking if key exists (true)"""

        mock_redis_client.client.exists.return_value = 1

        result = mock_redis_client.exists("test_key")
        assert result is True

    def test_exists_false(self, mock_redis_client):
        """Test checking if key exists (false)"""

        mock_redis_client.client.exists.return_value = 0

        result = mock_redis_client.exists("test_key")
        assert result is False

    def test_delete(self, mock_redis_client):
        """Test deleting key"""

        mock_redis_client.client.delete.return_value = 1

        result = mock_redis_client.delete("test_key")
        assert result is True

    def test_get_ttl(self, mock_redis_client):
        """Test getting TTL"""

        result = mock_redis_client.get_ttl("test_key")
        assert result == 3600

    def test_expire(self, mock_redis_client):
        """Test setting expiration"""

        result = mock_redis_client.expire("test_key", 7200)
        assert result is True


@pytest.mark.unit
class TestRedisClientRateLimit:
    """Test Redis rate limiting"""

    def test_check_rate_limit_within(self, mock_redis_client):
        """Test rate limit check when within limit"""

        mock_redis_client.client.incr.return_value = 50  # Within limit

        result = mock_redis_client.check_rate_limit(
            "user_123", max_requests=100, window_seconds=3600
        )
        assert result is True

    def test_check_rate_limit_exceeded(self, mock_redis_client):
        """Test rate limit check when exceeded"""

        mock_redis_client.client.incr.return_value = 101  # Exceeded

        result = mock_redis_client.check_rate_limit(
            "user_123", max_requests=100, window_seconds=3600
        )
        assert result is False

    def test_get_request_count(self, mock_redis_client):
        """Test getting request count"""

        mock_redis_client.client.get.return_value = b"75"

        result = mock_redis_client.get_request_count("user_123")
        assert result == 75


@pytest.mark.unit
class TestRedisClientBatch:
    """Test Redis batch operations"""

    def test_batch_set(self, mock_redis_client):
        """Test batch set operation"""

        items = {"key1": "value1", "key2": "value2", "key3": "value3"}

        result = mock_redis_client.batch_set(items, ttl=3600)
        assert result == 3
        assert mock_redis_client.client.setex.call_count == 3
        mock_redis_client.client.set.assert_not_called()

    def test_batch_get(self, mock_redis_client):
        """Test batch get operation"""

        mock_redis_client.client.get.side_effect = [b"value1", b"value2", None]

        keys = ["key1", "key2", "key3"]
        result = mock_redis_client.batch_get(keys, return_type=str)

        assert len(result) == 2  # Only 2 keys found
        assert "key1" in result
        assert "key2" in result

    def test_batch_delete(self, mock_redis_client):
        """Test batch delete operation"""

        keys = ["key1", "key2", "key3"]
        result = mock_redis_client.batch_delete(keys)

        assert result == 3
        assert mock_redis_client.client.delete.call_count == 3


@pytest.mark.unit
class TestRedisClientPattern:
    """Test Redis pattern operations"""

    def test_invalidate_pattern(self, mock_redis_client):
        """Test invalidating keys by pattern"""

        mock_redis_client.client.keys.return_value = [
            b"f1:race_2024_1",
            b"f1:race_2024_2",
        ]
        mock_redis_client.client.delete.return_value = 2

        result = mock_redis_client.invalidate_pattern("race_2024_*")
        assert result == 2

    def test_clear_all(self, mock_redis_client):
        """Test clearing all keys with prefix"""

        mock_redis_client.client.keys.return_value = [
            b"f1:key1",
            b"f1:key2",
            b"f1:key3",
        ]
        mock_redis_client.client.delete.return_value = 3

        result = mock_redis_client.clear_all()
        assert result is True


@pytest.mark.unit
class TestCacheQuery:
    """Test Redis query caching"""

    def test_executes_function_when_no_redis_client(self, sample_function):
        """Test that function executes normally when no redis_client is provided."""

        decorated = cache_query()(sample_function)

        result = decorated("arg1", "arg2", key="value")

        sample_function.assert_called_once_with("arg1", "arg2", key="value")
        assert result == {"data": "result"}

    def test_returns_cached_result_on_cache_hit(
        self, mock_redis_client, sample_function
    ):
        """Test that cached result is returned on cache hit."""
        cached_data = {"cached": "data"}
        mock_redis_client.client.get.return_value = cached_data

        decorated = cache_query()(sample_function)
        result = decorated("arg1", redis_client=mock_redis_client)

        assert result == cached_data
        sample_function.assert_not_called()  # Function should NOT be called

    def test_executes_function_on_cache_miss(self, mock_redis_client, sample_function):
        """Test that function is executed on cache miss."""
        mock_redis_client.client.get.return_value = None  # Cache miss

        decorated = cache_query()(sample_function)
        result = decorated("arg1", redis_client=mock_redis_client)

        sample_function.assert_called_once_with("arg1")
        assert result == {"data": "result"}

    def test_caches_result_after_function_execution(
        self, mock_redis_client, sample_function
    ):
        """Test that result is cached after function execution."""
        mock_redis_client.client.get.return_value = None  # Cache miss

        decorated = cache_query(ttl=300)(sample_function)
        decorated("arg1", redis_client=mock_redis_client)

        # Verify set was called with result
        mock_redis_client.client.setex.assert_called_once()
        call_kwargs = mock_redis_client.client.setex.call_args
        assert call_kwargs[0][1] == 300
        assert call_kwargs[0][2] == b'{"data": "result"}'  # The cached value


@pytest.mark.unit
class TestRedisClientGetInfo:
    """Test redis client get info"""

    def test_clear_all(self, mock_redis_client):
        """Test get info"""

        expected_dict = {
            "version": "7.4.7",
            "used_memory_human": "1.01M",
            "connected_clients": 1,
            "total_keys": 5,
            "uptime_days": 0,
        }

        mock_redis_client.client.info.return_value = {
            "redis_version": "7.4.7",
            "used_memory_human": "1.01M",
            "connected_clients": 1,
            "uptime_in_days": 0,
        }
        mock_redis_client.client.dbsize.return_value = 5
        result = mock_redis_client.get_info()

        assert result == expected_dict
        assert isinstance(result, dict)
