"""
Unit test fixtures
"""

# pylint: disable=redefined-outer-name, protected-access, unused-argument

import io
from unittest.mock import MagicMock, Mock, patch

import pytest
from fastf1.core import Session as FastF1Session
from sqlalchemy.orm import Session

from dagster_project.ingestion.resources import FastF1Resource
from dagster_project.shared.resources import (
    BucketClient,
    BucketResource,
    DatabaseClient,
    RedisClient,
    RedisResource,
)

# =============================================================================
# Config Fixtures
# =============================================================================


@pytest.fixture
def bucket_custom_config_dict() -> dict:
    """Custom bucket configuration"""

    return {
        "endpoint_url": "http://localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "region_name": "us-east-1",
        "raw_data_bucket": "custom-config-bucket-raw",
        "processed_data_bucket": "custom-config-bucket-processed",
        "model_bucket": "custom-config-bucket-models",
    }


@pytest.fixture
def database_custom_config_dict() -> dict:
    """Custom database configuration"""

    return {
        "host": "localhost",
        "port": 5434,
        "user": "customuser",
        "password": "custompass",
        "database": "custom_db",
        "backend": "supabase",
    }


@pytest.fixture
def redis_custom_config_dict() -> dict:
    """Custom redis configuration"""

    return {
        "host": "localhost",
        "port": 6378,
        "db": 1,
        "password": "custompass",
    }


# =============================================================================
# Mock Client Fixtures
# =============================================================================


@pytest.fixture
@patch("dagster_project.shared.resources.bucket_resource.boto3.client")
def mock_bucket_client(mock_boto_client):
    """Mock bucket client for testing"""

    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    client = BucketClient.from_env()

    return client


@pytest.fixture
def mock_session():
    """Create a mock session object."""
    session = Mock(spec=Session)
    return session


@pytest.fixture
def mock_session_factory(mock_session):
    """Create a mock session factory that returns the mock session."""
    factory = Mock(return_value=mock_session)
    return factory


@pytest.fixture
def mock_database_client(mock_session_factory):
    """Mock database client for testing"""

    with patch.object(DatabaseClient, "__init__", lambda self, *args, **kwargs: None):
        client = DatabaseClient.__new__(DatabaseClient)
        client.session_local = mock_session_factory
        client.logger = Mock()

        return client


@pytest.fixture
@patch("dagster_project.shared.resources.db_resource.create_engine")
@patch("dagster_project.shared.resources.db_resource.sessionmaker")
def mock_database_client_with_init(
    mock_sessionmaker, mock_create_engine, mock_session_factory
):
    """Mock database client with initialization"""

    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine
    mock_sessionmaker.return_value = mock_session_factory

    client = DatabaseClient.from_env()

    return client


@pytest.fixture
@patch("dagster_project.shared.resources.redis_resource.redis.ConnectionPool")
@patch("dagster_project.shared.resources.redis_resource.redis.Redis")
def mock_redis_client(mock_redis, mock_connection_pool):
    """Mock Redis client for testing"""

    mock_pool = Mock()
    mock_connection_pool.return_value = mock_pool

    mock_redis_cache_client = Mock()
    mock_redis_cache_client.ping.return_value = True
    mock_redis_cache_client.setex.return_value = True
    mock_redis_cache_client.get.return_value = b"test_value"
    mock_redis_cache_client.ttl.return_value = 3600
    mock_redis_cache_client.expire.return_value = True
    mock_redis_cache_client.delete.return_value = 1
    mock_redis.return_value = mock_redis_cache_client

    client = RedisClient.from_env()
    return client


@pytest.fixture
def mock_fastf1_session():
    """Mock fastf1 session object"""

    fastf1_session = Mock(spec=FastF1Session)
    fastf1_session.load = MagicMock()
    return fastf1_session


@pytest.fixture
def mock_fastf1_resource(mock_fastf1_session, sample_schedule_df):
    """Mock FastF1 resource for testing"""

    with patch("dagster_project.ingestion.resources.fastf1") as mock_fastf1:
        mock_fastf1.get_event_schedule.return_value = sample_schedule_df
        mock_fastf1.get_session.return_value = mock_fastf1_session
        resource = FastF1Resource.from_env()
        resource._logger = Mock()

        yield resource


# =============================================================================
# Mock Dagster Resources
# =============================================================================


@pytest.fixture
def mock_bucket_resource(mock_bucket_client):
    """Mock bucket resource for testing"""

    resource = BucketResource.from_env()
    with patch.object(resource, "get_client", return_value=mock_bucket_client):
        return resource


@pytest.fixture
def mock_redis_resource(mock_redis_client):
    """Mock redis resource for testing"""

    mock_resource = MagicMock(spec=RedisResource)
    mock_resource.get_client.return_value = mock_redis_client

    return mock_resource


# =============================================================================
# Data Fixtures
# =============================================================================


@pytest.fixture
def sample_parquet_bytes(sample_schedule_df) -> bytes:
    """Convert sample DataFrame to parquet bytes"""
    buffer = io.BytesIO()
    sample_schedule_df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer.getvalue()


@pytest.fixture
def sample_json_data() -> dict:
    """Sample JSON data for testing"""
    return {
        "year": 2024,
        "grand_prix": "Bahrain Grand Prix",
        "session": "Race",
        "winner": "Max Verstappen",
        "fastest_lap": "1:31.447",
    }


@pytest.fixture
def sample_function():
    """Create a sample function to decorate."""
    mock_func = Mock(return_value={"data": "result"})
    mock_func.__name__ = "sample_function"
    mock_func.__doc__ = "Sample docstring"
    return mock_func
