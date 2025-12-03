"""
Shared fixtures for integration tests
"""

# pylint: disable=protected-access, redefined-outer-name

from unittest.mock import MagicMock, Mock, patch

import pytest
from fastf1.core import Session as FastF1Session
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from dagster_project.ingestion.resources import FastF1Resource
from dagster_project.shared.resources import (
    BucketClient,
    RedisClient,
)
from src.config.settings import BucketConfig, RedisConfig

# =============================================================================
# Mock Minio Client and Container
# =============================================================================


@pytest.fixture(scope="session")
def bucket_container():
    """Minio container for integartion tests"""

    container = MinioContainer(
        image="minio/minio:latest",
        access_key="minioadmin",
        secret_key="minioadmin",
    )
    container.with_env("MINIO_REGION", "ap-south-1")

    with container as minio:
        yield minio


@pytest.fixture
def bucket_client(bucket_container):
    """Minio client for integration testing"""

    config = bucket_container.get_config()

    bucket_config = BucketConfig(
        endpoint_url=f"http://{config['endpoint']}",
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        raw_data_bucket="test-bucket-raw",
        processed_data_bucket="test-bucket-processed",
        model_bucket="test-bucket-model",
    )

    client = BucketClient(config=bucket_config)

    client.create_bucket("test-bucket-raw")
    client.create_bucket("test-bucket-processed")
    client.create_bucket("test-bucket-models")

    yield client

    # Cleanup: remove all objects and buckets after each test
    for bucket_name in [
        "test-bucket-raw",
        "test-bucket-processed",
        "test-bucket-models",
    ]:
        # Delete bucket, force=True ensures that files in non-empty
        # buckets are deleted before deleting the bucket
        client.delete_bucket(bucket_name, force=True)


@pytest.fixture(scope="session")
def redis_container():
    """Redis container for integartion tests"""

    with RedisContainer("redis:7-alpine") as redis:
        yield redis


@pytest.fixture
def redis_client(redis_container):
    """Redis client for integration testing"""

    redis_config = RedisConfig(
        host=redis_container.get_container_host_ip(),
        port=redis_container.get_exposed_port(6379),
    )
    client = RedisClient(config=redis_config)

    yield client


@pytest.fixture(scope="session")
def postgres_container():
    """Postgres container for integartion tests"""

    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


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
