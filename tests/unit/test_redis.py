"""
Unit tests for RedisClient
"""

import io
import json
from unittest.mock import MagicMock, patch

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
    def test_init_with_config(
        self,
        mock_redis,
        mock_connection_pool,
        redis_config_dict: dict,
    ):
        """Test initializing RedisClient with config"""
        config = RedisConfig(**redis_config_dict)
        client = RedisClient(config=config)

        assert client.key_prefix == "f1:"
        assert client.max_retries == 3
        mock_redis.assert_called_once()

    def test_init_from_env(self, mock_env_vars):
        """Test initializing RedisClient from environment"""
        with patch(
            "dagster_project.shared.resources.redis_resource.redis.ConnectionPool"
        ):
            with patch("dagster_project.shared.resources.redis_resource.redis.Redis"):
                client = RedisClient.from_env()

                assert client.key_prefix == "f1:"


@pytest.mark.unit
class TestRedisClientKeyBuilding:
    """Test Redis key building"""

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_build_key_simple(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test building simple key"""
        config = RedisConfig(**redis_config_dict)
        client = RedisClient(config=config)

        key = client._build_key("test_key")
        assert key == "f1:test_key"

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_build_key_with_type(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test building key with data type"""
        config = RedisConfig(**redis_config_dict)
        client = RedisClient(config=config)

        key = client._build_key("test_key", CacheDataType.PARQUET)
        assert key == "f1:parquet:test_key"


@pytest.mark.unit
class TestRedisClientRetry:
    """Test Redis retry operations"""

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    @patch("time.sleep")
    def test_retry_success_on_second_attempt(
        self, mock_sleep, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test successful retry after initial failure"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_redis.return_value = mock_client

        mock_operation = MagicMock()
        mock_operation.side_effect = [
            RedisConnectionError("Error connecting to Redis"),
            "Success",
        ]

        client = RedisClient(config=config)
        result = client._retry_with_backoff(mock_operation)

        assert result == "Success"
        assert mock_operation.call_count == 2
        mock_sleep.assert_called_once()

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    @patch("time.sleep")
    def test_retry_all_attempts_fail(
        self, mock_sleep, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test successful retry after initial failure"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_redis.return_value = mock_client

        mock_operation = MagicMock()
        mock_operation.side_effect = RedisConnectionError("Error connecting to Redis")

        client = RedisClient(config=config)

        with pytest.raises(RedisConnectionError):
            client._retry_with_backoff(mock_operation)

        assert mock_operation.call_count == 3
        assert mock_sleep.call_count == 2


@pytest.mark.unit
class TestRedisClientSetGet:
    """Test Redis set/get operations"""

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_ping_success(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test successful ping"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.ping()
        assert result is True

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_set_string(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test setting string value"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.setex.return_value = True
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.set("test_key", "test_value", ttl=3600)
        assert result is True
        mock_client.setex.assert_called_once()

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_set_dict(
        self,
        mock_redis,
        mock_connection_pool,
        redis_config_dict: dict,
        sample_json_data: dict,
    ):
        """Test setting dictionary value"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.setex.return_value = True
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.set("test_key", sample_json_data, ttl=3600)
        assert result is True

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_set_dataframe(
        self,
        mock_redis,
        mock_connection_pool,
        redis_config_dict: dict,
        sample_schedule_df: pd.DataFrame,
    ):
        """Test setting DataFrame value"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.setex.return_value = True
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.set("test_key", sample_schedule_df, ttl=3600)
        assert result is True

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_get_string(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test getting string value"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.get.return_value = b"test_value"
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.get("test_key", return_type=str)
        assert result == "test_value"

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_get_not_found(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test getting non-existent key"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.get.return_value = None
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.get("nonexistent_key")
        assert result is None

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_get_bytes(self, mock_redis, mock_connection_pool, redis_config_dict: dict):
        """Test getting string value"""
        config = RedisConfig(**redis_config_dict)

        # Return raw bytes (not decoded)
        raw_bytes = b"\x89PNG\r\n\x1a\n\x00\x00\x00"  # Example binary data
        mock_client = MagicMock()
        mock_client.get.return_value = raw_bytes
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)
        result = client.get("test_key", return_type=bytes)

        assert result == raw_bytes
        assert isinstance(result, bytes)

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_get_without_return_type(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test getting string value"""
        config = RedisConfig(**redis_config_dict)

        # Return raw bytes (not decoded)
        expected_dict = {"key": "value", "number": 42}
        mock_client = MagicMock()
        mock_client.get.return_value = expected_dict
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)
        result = client.get("test_key")

        assert result == expected_dict
        assert isinstance(result, dict)


@pytest.mark.unit
class TestRedisClientParquet:
    """Test Redis parquet operations"""

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_cache_parquet(
        self,
        mock_redis,
        mock_connection_pool,
        redis_config_dict: dict,
        sample_schedule_df: pd.DataFrame,
    ):
        """Test caching DataFrame as parquet"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.setex.return_value = True
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.cache_parquet("test_key", sample_schedule_df, ttl=3600)
        assert result is True

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_get_parquet(
        self,
        mock_redis,
        mock_connection_pool,
        redis_config_dict: dict,
        sample_schedule_df: dict,
    ):
        """Test getting cached DataFrame"""
        config = RedisConfig(**redis_config_dict)

        # Serialize DataFrame to bytes
        buffer = io.BytesIO()
        sample_schedule_df.to_parquet(buffer, index=False)
        parquet_bytes = buffer.getvalue()

        mock_client = MagicMock()
        mock_client.get.return_value = parquet_bytes
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.get_parquet("test_key")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(sample_schedule_df)


@pytest.mark.unit
class TestRedisClientJSON:
    """Test Redis JSON operations"""

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_cache_json(
        self,
        mock_redis,
        mock_connection_pool,
        redis_config_dict: dict,
        sample_json_data: dict,
    ):
        """Test caching JSON data"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.setex.return_value = True
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.cache_json("test_key", sample_json_data, ttl=1800)
        assert result is True

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_get_json(
        self,
        mock_redis,
        mock_connection_pool,
        redis_config_dict: dict,
        sample_json_data: dict,
    ):
        """Test getting cached JSON data"""
        config = RedisConfig(**redis_config_dict)

        json_bytes = json.dumps(sample_json_data).encode("utf-8")

        mock_client = MagicMock()
        mock_client.get.return_value = json_bytes
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.get_json("test_key")
        assert result == sample_json_data


@pytest.mark.unit
class TestRedisClientOperations:
    """Test Redis other operations"""

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_exists_true(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test checking if key exists (true)"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.exists.return_value = 1
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.exists("test_key")
        assert result is True

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_exists_false(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test checking if key exists (false)"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.exists.return_value = 0
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.exists("test_key")
        assert result is False

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_delete(
        self,
        mock_redis,
        mock_connection_pool,
        redis_config_dict: dict,
    ):
        """Test deleting key"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.delete.return_value = 1
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.delete("test_key")
        assert result is True

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_get_ttl(self, mock_redis, mock_connection_pool, redis_config_dict: dict):
        """Test getting TTL"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.ttl.return_value = 3600
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.get_ttl("test_key")
        assert result == 3600

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_expire(self, mock_redis, mock_connection_pool, redis_config_dict: dict):
        """Test setting expiration"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.expire.return_value = True
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.expire("test_key", 7200)
        assert result is True


@pytest.mark.unit
class TestRedisClientRateLimit:
    """Test Redis rate limiting"""

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_check_rate_limit_within(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test rate limit check when within limit"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.incr.return_value = 50  # Within limit
        mock_client.expire.return_value = True
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.check_rate_limit(
            "user_123", max_requests=100, window_seconds=3600
        )
        assert result is True

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_check_rate_limit_exceeded(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test rate limit check when exceeded"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.incr.return_value = 101  # Exceeded
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.check_rate_limit(
            "user_123", max_requests=100, window_seconds=3600
        )
        assert result is False

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_get_request_count(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test getting request count"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.get.return_value = b"75"
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.get_request_count("user_123")
        assert result == 75


@pytest.mark.unit
class TestRedisClientBatch:
    """Test Redis batch operations"""

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_batch_set(self, mock_redis, mock_connection_pool, redis_config_dict: dict):
        """Test batch set operation"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.setex.return_value = True
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        items = {"key1": "value1", "key2": "value2", "key3": "value3"}

        result = client.batch_set(items, ttl=3600)
        assert result == 3
        assert mock_client.setex.call_count == 3

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_batch_get(self, mock_redis, mock_connection_pool, redis_config_dict: dict):
        """Test batch get operation"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.get.side_effect = [b"value1", b"value2", None]
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        keys = ["key1", "key2", "key3"]
        result = client.batch_get(keys, return_type=str)

        assert len(result) == 2  # Only 2 keys found
        assert "key1" in result
        assert "key2" in result

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_batch_delete(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test batch delete operation"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.delete.return_value = 1
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        keys = ["key1", "key2", "key3"]
        result = client.batch_delete(keys)

        assert result == 3
        assert mock_client.delete.call_count == 3


@pytest.mark.unit
class TestRedisClientPattern:
    """Test Redis pattern operations"""

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_invalidate_pattern(
        self, mock_redis, mock_connection_pool, redis_config_dict: dict
    ):
        """Test invalidating keys by pattern"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.keys.return_value = [b"f1:race_2024_1", b"f1:race_2024_2"]
        mock_client.delete.return_value = 2
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.invalidate_pattern("race_2024_*")
        assert result == 2

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_clear_all(self, mock_redis, mock_connection_pool, redis_config_dict: dict):
        """Test clearing all keys with prefix"""
        config = RedisConfig(**redis_config_dict)

        mock_client = MagicMock()
        mock_client.keys.return_value = [b"f1:key1", b"f1:key2", b"f1:key3"]
        mock_client.delete.return_value = 3
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)

        result = client.clear_all()
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
        mock_redis_client.get.return_value = cached_data

        decorated = cache_query()(sample_function)
        result = decorated("arg1", redis_client=mock_redis_client)

        assert result == cached_data
        sample_function.assert_not_called()  # Function should NOT be called

    def test_executes_function_on_cache_miss(self, mock_redis_client, sample_function):
        """Test that function is executed on cache miss."""
        mock_redis_client.get.return_value = None  # Cache miss

        decorated = cache_query()(sample_function)
        result = decorated("arg1", redis_client=mock_redis_client)

        sample_function.assert_called_once_with("arg1")
        assert result == {"data": "result"}

    def test_caches_result_after_function_execution(
        self, mock_redis_client, sample_function
    ):
        """Test that result is cached after function execution."""
        mock_redis_client.get.return_value = None  # Cache miss

        decorated = cache_query(ttl=300)(sample_function)
        decorated("arg1", redis_client=mock_redis_client)

        # Verify set was called with result
        mock_redis_client.set.assert_called_once()
        call_kwargs = mock_redis_client.set.call_args
        assert call_kwargs[0][1] == {"data": "result"}  # The cached value
        assert call_kwargs[1]["ttl"] == 300


@pytest.mark.unit
class TestRedisClientGetInfo:
    """Test redis client get info"""

    @patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
    @patch("dagster_project.shared.resources.redis_resource.redis.Redis")
    def test_clear_all(self, mock_redis, mock_connection_pool, redis_config_dict: dict):
        """Test get info"""

        config = RedisConfig(**redis_config_dict)

        expected_dict = {
            "version": "7.4.7",
            "used_memory_human": "1.01M",
            "connected_clients": 1,
            "total_keys": 5,
            "uptime_days": 0,
        }

        mock_client = MagicMock()
        mock_client.info.return_value = {
            "redis_version": "7.4.7",
            "used_memory_human": "1.01M",
            "connected_clients": 1,
            "uptime_in_days": 0,
        }
        mock_client.dbsize.return_value = 5
        mock_redis.return_value = mock_client

        client = RedisClient(config=config)
        result = client.get_info()

        assert result == expected_dict
        assert isinstance(result, dict)
