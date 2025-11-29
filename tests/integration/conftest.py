"""
Shared fixtures for integration tests
"""

# pylint: disable=protected-access

import pytest
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from dagster_project.shared.resources import BucketClient

# =============================================================================
# Mock Minio Client and Container
# =============================================================================


@pytest.fixture(scope="session")
def minio_container():
    """Minio container for integartion tests"""

    with MinioContainer(
        image="minio/minio:latest",
        port=9000,
        access_key="minioadmin",
        secret_key="minioadmin",
    ) as minio:
        yield minio


@pytest.fixture
def bucket_client():
    """Minio client for integration testing"""

    client = BucketClient.from_env()

    client.create_bucket("test-bucket-raw")
    client.create_bucket("test-bucket-processed")
    client.create_bucket("test-bucket-models")


@pytest.fixture(scope="session")
def postgres_container():
    """Postgres container for integartion tests"""

    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def redis_container():
    """Redis container for integartion tests"""

    with RedisContainer("redis:7-alpine") as redis:
        yield redis
