"""
Integration tests for redis cache
"""

from unittest.mock import patch

import pandas as pd
import pandas.testing as pdt
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from dagster_project.shared.resources import CacheDataType, cache_query


@pytest.mark.integration
class TestRedisClient:
    """Test redis client for integration"""

    def test_redis_client_connection(self, redis_client):
        """Test if the redis server is live"""

        status = redis_client.ping()

        assert status is True


@pytest.mark.integration
class TestRedisClientSetGet:
    """Test redis client set and get operations"""

    def test_redis_cache_roundtrip_string(self, redis_client):
        """Test the similarity of df after set and get"""

        set_status = redis_client.set(
            key="integration:test_string",
            value="test_string",
            data_type=CacheDataType.STRING,
        )

        download_data = redis_client.get(
            key="integration:test_string",
            data_type=CacheDataType.STRING,
            return_type=str,
        )

        assert set_status is True
        assert download_data == "test_string"

        # Cleanup
        redis_client.delete(
            key="integration:test_string", data_type=CacheDataType.STRING
        )

    def test_redis_cache_get_parquet(self, redis_client):
        """Test roundtrip parquet data"""

        df = pd.DataFrame(
            {
                "driver": ["Charles", "Lando"],
                "number": [16, 4],
                "team": ["Scuderia Ferrari", "McLaren F1"],
            }
        )

        set_status = redis_client.cache_parquet(
            key="integration:test_df",
            df=df,
        )

        get_df = redis_client.get_parquet(key="integration:test_df")

        assert set_status is True
        pdt.assert_frame_equal(
            df.reset_index(drop=True),
            get_df.reset_index(drop=True),
        )

        # Cleanup
        redis_client.delete(
            key="integration:test_string", data_type=CacheDataType.STRING
        )

    def test_redis_cache_get_json(self, redis_client):
        """Test roundtrip json data"""

        sample_json = {
            "year": 2024,
            "grand_prix": "Bahrain Grand Prix",
            "session": "Race",
            "winner": "Max Verstappen",
            "fastest_lap": "1:31.447",
        }

        set_status = redis_client.cache_json(
            key="integration:sample_json", data=sample_json
        )

        get_json = redis_client.get_json(key="integration:sample_json")

        assert set_status is True
        assert get_json == sample_json

        # Cleanup
        redis_client.delete(key="integration:sample_json", data_type=CacheDataType.JSON)


@pytest.mark.integration
class TestRedisClientBatchSetGet:
    """Test batch set and get operations"""

    def test_redis_batch_roundtrip(self, redis_client):
        """Test roundtrip batch set and get"""

        test_string_1 = "test-string-1"
        test_string_2 = "test-string-2"
        test_string_3 = "test-string-3"

        key_1 = "integration:testing:string-1"
        key_2 = "integration:testing:string-2"
        key_3 = "integration:testing:string-3"

        batch_set_count = redis_client.batch_set(
            items={
                key_1: test_string_1,
                key_2: test_string_2,
                key_3: test_string_3,
            },
            data_type=CacheDataType.STRING,
        )

        batch_get_data = redis_client.batch_get(
            keys=[key_1, key_2, key_3],
            data_type=CacheDataType.STRING,
        )

        assert batch_set_count == 3
        assert (
            all(
                (
                    key_1 in batch_get_data,
                    key_2 in batch_get_data,
                    key_3 in batch_get_data,
                )
            )
            is True
        )
        assert batch_get_data[key_1] == test_string_1.encode("utf-8")
        assert batch_get_data[key_2] == test_string_2.encode("utf-8")
        assert batch_get_data[key_3] == test_string_3.encode("utf-8")

        # Cleanup
        redis_client.clear_all()

    def test_batch_delete(self, redis_client):
        """Test batch delate"""

        test_string_1 = "test-string-1"
        test_string_2 = "test-string-2"
        test_string_3 = "test-string-3"

        key_1 = "integration:testing:string-1"
        key_2 = "integration:testing:string-2"
        key_3 = "integration:testing:string-3"

        batch_set_count = redis_client.batch_set(
            items={
                key_1: test_string_1,
                key_2: test_string_2,
                key_3: test_string_3,
            },
            data_type=CacheDataType.STRING,
        )

        batch_delete_count = redis_client.batch_delete(
            keys=[key_1, key_2, key_3],
            data_type=CacheDataType.STRING,
        )

        assert batch_set_count == 3
        assert batch_delete_count == 3


@pytest.mark.integration
class TestRedisClientOperations:
    """Test redis client operations"""

    def test_exists(self, redis_client):
        """Test key value exists"""

        _ = redis_client.set(
            key="integration:test_string",
            value="test_string",
            data_type=CacheDataType.STRING,
        )

        exists_status = redis_client.exists(
            key="integration:test_string", data_type=CacheDataType.STRING
        )

        assert exists_status is True

        # Cleanup
        redis_client.delete(
            key="integration:test_string", data_type=CacheDataType.STRING
        )

    def test_exists_nonexistent(self, redis_client):
        """Test nonexistent key value exists"""

        exists_status = redis_client.exists(key="integration:nonexistent")

        assert exists_status is False

    def test_get_ttl(self, redis_client):
        """Test get ttl for different data types"""

        sample_json = {
            "year": 2024,
            "grand_prix": "Bahrain Grand Prix",
            "session": "Race",
            "winner": "Max Verstappen",
            "fastest_lap": "1:31.447",
        }

        sample_df = pd.DataFrame(
            {
                "driver": ["Charles", "Lando"],
                "number": [16, 4],
                "team": ["Scuderia Ferrari", "McLaren F1"],
            }
        )

        string_key = "integration:sample-string"
        json_key = "integration:sample-json"
        parquet_key = "integration:sample-parquet"

        string_set_status = redis_client.set(
            key=string_key,
            value="test-string-value",
            data_type=CacheDataType.STRING,
        )
        json_set_status = redis_client.set(
            key=json_key,
            value=sample_json,
            data_type=CacheDataType.JSON,
        )
        parquet_set_status = redis_client.set(
            key=parquet_key,
            value=sample_df,
            data_type=CacheDataType.PARQUET,
        )

        string_ttl = redis_client.get_ttl(
            key=string_key, data_type=CacheDataType.STRING
        )
        json_ttl = redis_client.get_ttl(key=json_key, data_type=CacheDataType.JSON)
        parquet_ttl = redis_client.get_ttl(
            key=parquet_key, data_type=CacheDataType.PARQUET
        )

        assert string_set_status is True
        assert json_set_status is True
        assert parquet_set_status is True

        assert string_ttl == 300
        assert json_ttl == 1800
        assert parquet_ttl == 3600

        # Cleanup
        redis_client.delete(key=string_key, data_type=CacheDataType.STRING)
        redis_client.delete(key=json_key, data_type=CacheDataType.JSON)
        redis_client.delete(key=parquet_key, data_type=CacheDataType.PARQUET)

    def test_invalidate_pattern(self, redis_client):
        """Test that all keys of a pattern are deleted"""

        _ = redis_client.set(
            key="sample_test1", value="test-string-1", data_type=CacheDataType.STRING
        )

        _ = redis_client.set(
            key="sample_test2", value="test-string-2", data_type=CacheDataType.STRING
        )

        delete_count = redis_client.invalidate_pattern(
            pattern="sample_*", data_type=CacheDataType.STRING
        )

        assert delete_count == 2

    def test_clear_all(self, redis_client):
        """Test that all keys with a prefix are deleted"""

        _ = redis_client.set(
            key="sample_test1", value="test-string-1", data_type=CacheDataType.STRING
        )

        _ = redis_client.set(
            key="sample_test2", value="test-string-2", data_type=CacheDataType.STRING
        )

        clear_all_status = redis_client.clear_all()

        assert clear_all_status is True

    def test_get_info(self, redis_client):
        """Test get info"""

        info = redis_client.get_info()

        assert info["version"] == "7.4.7"
        assert info["connected_clients"] == 1
        assert info["total_keys"] == 0
        assert info["uptime_days"] == 0


@pytest.mark.integration
class TestRedisCacheQuery:
    """Test cache query"""

    def test_executes_function_on_cache_miss(self, redis_client):
        """Test that function is executed on cache miss."""

        def sample_function():
            """Sample function for testing"""
            return {"data": "result"}

        decorated = cache_query()(sample_function)
        result = decorated(redis_client=redis_client)

        assert result == {"data": "result"}
        assert (
            redis_client.exists(
                key="query:sample_function::", data_type=CacheDataType.QUERY_RESULT
            )
            is True
        )

        # Cleanup
        redis_client.clear_all()

    def test_returns_cache_on_cache_hit(self, redis_client):
        """Test that cached value is return on cache hit"""

        function_call_count = 0

        def sample_function():
            """Sample function for testing"""

            nonlocal function_call_count
            function_call_count += 1

            return {"data": "result"}

        decorated = cache_query()(sample_function)
        _ = decorated(redis_client=redis_client)

        result = decorated(redis_client=redis_client)

        assert result == {"data": "result"}
        assert function_call_count == 1

        # Cleanup
        redis_client.clear_all()


@pytest.mark.integration
class TestRedisClientRateLimit:
    """Test rate limt"""

    def test_redis_client_rate_limit(self, redis_client):
        """Test that valid rate limits are tested"""

        result1 = redis_client.check_rate_limit(
            identifier="redisuser",
            max_requests=1,
            window_seconds=3600,
        )

        result2 = redis_client.check_rate_limit(
            identifier="redisuser",
            max_requests=1,
            window_seconds=3600,
        )

        assert result1 is True
        assert result2 is False

        # Cleanup
        redis_client.clear_all()

    def test_redis_get_request_count(self, redis_client):
        """Test get requet count"""

        _ = redis_client.check_rate_limit(
            identifier="testredisuser",
            max_requests=1,
            window_seconds=3600,
        )

        _ = redis_client.check_rate_limit(
            identifier="testredisuser",
            max_requests=1,
            window_seconds=3600,
        )

        request_count = redis_client.get_request_count(identifier="testredisuser")

        assert request_count == 2


# pylint: disable=protected-access


@pytest.mark.integration
class TestRedisRetry:
    """Test the redis client retry functionality"""

    def test_retry_with_real_redis_operation(self, redis_client):
        """Test retry logic with actual Redis get/set"""

        redis_client.set(
            key="test_key",
            value="test_value",
        )

        def get_operation():
            cache_key = redis_client._build_key(
                "test_key", data_type=CacheDataType.STRING
            )
            return redis_client.client.get(cache_key)

        result = redis_client._retry_with_backoff(get_operation)

        assert result == b"test_value"

    def test_retry_recovers_from_simulated_transient_failure(self, redis_client):
        """Test recovery from a transient connection issue"""
        call_count = 0

        def flaky_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RedisConnectionError("simulated transient failure")
            return redis_client.ping()

        with patch("time.sleep"):
            result = redis_client._retry_with_backoff(flaky_operation)

        assert result is True
        assert call_count == 2

    def test_retry_raises_after_all_failed_attempts(self, redis_client):
        """Test exception raise after all failed attempts"""

        def flaky_operation():
            raise RedisConnectionError("simulated connection failure")

        with pytest.raises(RedisConnectionError, match="simulated connection failure"):
            _ = redis_client._retry_with_backoff(flaky_operation)
