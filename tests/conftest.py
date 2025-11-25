"""
Shared pytest fixtures and configuration for the entire test suite.

This file provides:
1. Docker service fixtures (PostgreSQL, MinIO)
2. Dagster-related fixtures (resources, contexts)
3. Test data fixtures
4. File and path fixtures
5. Mock fixtures
"""

# pylint: disable=redefined-outer-name

import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Dict, Generator, Iterator
from unittest.mock import MagicMock

import boto3
import numpy as np
import pandas as pd
import psycopg2
import pytest
from dagster import (
    DagsterInstance,
    build_asset_context,
    build_op_context,
)
from dagster._core.execution.context.invocation import DirectAssetExecutionContext
from pytest_mock.plugin import _mocker
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker

from dagster_project.shared.resources import (
    BucketClient,
    BucketPath,
    BucketResource,
)

# =============================================================================
# Pytest Configuration
# =============================================================================


def pytest_configure(config):
    """Configure custom markers for pytest."""

    config.addinivalue_line(
        "markers", "unit: Unit tests that don't require external services"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests that require Docker services"
    )
    config.addinivalue_line(
        "markers", "dagster: Tests for Dagster assets, jobs, and resources"
    )
    config.addinivalue_line("markers", "slow: Tests that take a long time to run")


def pytest_collection_modifyitems(config, items):  # pylint: disable=unused-argument
    """Automatically mark tests based on their location."""

    for item in items:
        # Auto-mark integration tests
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)

        # Auto-mark unit tests
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)

        # Auto-mark data validation tests
        if "data_validation" in str(item.fspath):
            item.add_marker(pytest.mark.slow)


# =============================================================================
# Environment and Configuration Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def test_env_vars() -> Iterator[Dict[str, str]]:
    """Set up test environment variables."""

    env_vars = {
        # PostgreSQL
        "POSTGRES_USER": "testuser",
        "POSTGRES_PASSWORD": "testpass",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "testdb",
        # MinIO
        "MINIO_ENDPOINT": "http://localhost:9000",
        "MINIO_ACCESS_KEY": "minioadmin",
        "MINIO_SECRET_KEY": "minioadmin",
        "MINIO_ROOT_USER": "minioadmin",
        "MINIO_ROOT_PASSWORD": "minioadmin",
        # Dagster
        "DAGSTER_HOME": ".dagster_home_test",
    }

    # Set environment variables
    for key, value in env_vars.items():
        os.environ[key] = value

    yield env_vars

    # Cleanup
    for key in env_vars:
        os.environ.pop(key, None)


# @pytest.fixture(scope="session")
# def test_settings(test_env_vars):
#     """Provide test settings/configuration."""

#     from src.config import get_settings

#     return get_settings()


# =============================================================================
# Docker Service Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def docker_services_available() -> bool:
    """Check if Docker services are available."""

    try:
        # Try to connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            user="testuser",
            password="testpass",
            dbname="testdb",
            connect_timeout=3,
        )
        conn.close()

        # Try to connect to MinIO
        s3 = boto3.client(
            "s3",
            endpoint_url="http://localhost:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
        s3.list_buckets()

        return True

    except Exception:  # pylint: disable=broad-exception-caught
        return False


@pytest.fixture(scope="session")
def postgres_engine(
    test_env_vars: Iterator[Dict[str, str]], docker_services_available: bool
):
    """Create SQLAlchemy engine for PostgreSQL."""

    if not docker_services_available:
        pytest.skip("Docker services not available")

    connection_string = (
        f"postgresql://{test_env_vars['POSTGRES_USER']}:"
        f"{test_env_vars['POSTGRES_PASSWORD']}@"
        f"{test_env_vars['POSTGRES_HOST']}:"
        f"{test_env_vars['POSTGRES_PORT']}/"
        f"{test_env_vars['POSTGRES_DB']}"
    )

    engine = create_engine(connection_string)

    yield engine

    engine.dispose()


@pytest.fixture(scope="function")
def db_session(postgres_engine: Engine):
    """Create a database session for a test."""

    session_object = sessionmaker(bind=postgres_engine)
    session = session_object()

    yield session

    session.rollback()
    session.close()


# =============================================================================
# MinIO / S3 Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def minio_client_session(
    test_env_vars: Iterator[Dict[str, str]], docker_services_available: bool
) -> BucketClient:
    """
    Create a session-scoped MinIO client for integration tests.

    This client persists across all tests in the session.
    """

    if not docker_services_available:
        pytest.skip("Docker services not available")

    client = create_bucket_client(
        endpoint_url=test_env_vars["MINIO_ENDPOINT"],
        access_key=test_env_vars["MINIO_ACCESS_KEY"],
        secret_key=test_env_vars["MINIO_SECRET_KEY"],
        use_ssl=False,
    )

    return client


@pytest.fixture(scope="function")
def minio_client(minio_client_session: BucketClient) -> Iterator[BucketClient]:
    """
    Function-scoped MinIO client.

    Creates test buckets before each test and cleans up after.
    """

    # Create test buckets
    test_buckets = ["test-raw", "test-processed", "test-integration"]
    for bucket in test_buckets:
        try:
            minio_client_session.create_bucket(bucket)
        except Exception:  # pylint: disable=broad-exception-caught
            pass  # Bucket might already exist

    yield minio_client_session

    # Cleanup: Delete test buckets
    for bucket in test_buckets:
        try:
            minio_client_session.delete_bucket(bucket, force=True)
        except Exception:  # pylint: disable=broad-exception-caught
            pass


@pytest.fixture(scope="function")
def mock_s3_client():
    """Create a mocked S3 client for unit tests."""

    mock_client = MagicMock()

    # Configure common mock responses
    mock_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "2024/Bahrain Grand Prix/Practice 1/data.parquet"},
            {"Key": "2024/Bahrain Grand Prix/Practice 2/data.parquet"},
        ]
    }

    mock_client.head_object.return_value = {}
    mock_client.list_buckets.return_value = {
        "Buckets": [
            {"Name": "test-bucket-1"},
            {"Name": "test-bucket-2"},
        ]
    }

    return mock_client


# =============================================================================
# Dagster Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def dagster_instance() -> Iterator[DagsterInstance]:
    """Create a Dagster instance for testing."""

    with tempfile.TemporaryDirectory() as temp_dir:
        instance = DagsterInstance.ephemeral(tempdir=temp_dir)
        yield instance


@pytest.fixture
def dagster_op_context(dagster_instance: Iterator[DagsterInstance]):
    """Create a Dagster op context for testing ops."""

    return build_op_context(instance=dagster_instance)


@pytest.fixture
def dagster_asset_context(dagster_instance: Iterator[DagsterInstance]):
    """Create a Dagster asset context for testing assets."""

    return build_asset_context(instance=dagster_instance)


@pytest.fixture
def bucket_resource_dagster(test_env_vars: Iterator[Dict[str, str]]) -> BucketResource:
    """Create a BucketResource for Dagster tests."""

    return BucketResource(
        endpoint_url=test_env_vars["MINIO_ENDPOINT"],
        access_key=test_env_vars["MINIO_ACCESS_KEY"],
        secret_key=test_env_vars["MINIO_SECRET_KEY"],
        use_ssl=False,
        max_retries=3,
        retry_delay=0.5,  # Shorter delay for tests
    )


@pytest.fixture
def dagster_context_with_resources(
    dagster_asset_context: DirectAssetExecutionContext,
    bucket_resource_dagster: BucketResource,
):
    """Create a Dagster context with resources attached."""

    # Add resources to context
    context = dagster_asset_context
    context.resources = MagicMock()
    context.resources.bucket_resource = bucket_resource_dagster

    return context


# =============================================================================
# File and Path Fixtures
# =============================================================================


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test files."""

    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def sample_parquet_file(temp_dir: Path) -> Path:
    """Create a sample parquet file for testing."""

    df = pd.DataFrame(
        {
            "lap": [1, 2, 3, 4, 5],
            "time": [90.123, 89.456, 88.789, 89.012, 88.345],
            "driver": ["VER", "HAM", "LEC", "NOR", "SAI"],
            "team": ["Red Bull", "Mercedes", "Ferrari", "McLaren", "Ferrari"],
        }
    )

    file_path = temp_dir / "test_data.parquet"
    df.to_parquet(file_path)

    return file_path


@pytest.fixture
def sample_csv_file(temp_dir: Path) -> Path:
    """Create a sample CSV file for testing."""

    df = pd.DataFrame(
        {
            "position": [1, 2, 3],
            "driver": ["VER", "HAM", "LEC"],
            "points": [25, 18, 15],
        }
    )

    file_path = temp_dir / "test_data.csv"
    df.to_csv(file_path, index=False)

    return file_path


@pytest.fixture
def sample_json_file(temp_dir: Path) -> Path:
    """Create a sample JSON file for testing."""

    data = {"race": "Bahrain Grand Prix", "year": 2024, "winner": "VER", "laps": 57}

    file_path = temp_dir / "test_data.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    return file_path


@pytest.fixture
def bucket_path_factory():
    """Factory fixture for creating BucketPath objects."""

    def _create_bucket_path(
        bucket: str = "test-bucket",
        year: str = "2024",
        grand_prix: str = "bahrain",
        session: str = "practice1",
        filename: str = "data.parquet",
    ) -> BucketPath:
        return BucketPath(
            bucket=bucket,
            year=year,
            grand_prix=grand_prix,
            session=session,
            filename=filename,
        )

    return _create_bucket_path


# =============================================================================
# Test Data Fixtures
# =============================================================================


@pytest.fixture
def sample_lap_data() -> pd.DataFrame:
    """Create sample lap time data."""
    return pd.DataFrame(
        {
            "lap": list(range(1, 11)),
            "time": [
                90.123,
                89.456,
                88.789,
                89.012,
                88.345,
                87.890,
                88.123,
                87.567,
                87.890,
                88.234,
            ],
            "driver": ["VER"] * 10,
            "compound": ["SOFT"] * 5 + ["MEDIUM"] * 5,
            "is_personal_best": [
                False,
                False,
                True,
                False,
                True,
                False,
                False,
                True,
                False,
                False,
            ],
        }
    )


@pytest.fixture
def sample_telemetry_data() -> pd.DataFrame:
    """Create sample telemetry data."""

    distance = np.linspace(0, 5000, 100)
    speed = 200 + 50 * np.sin(distance / 1000)
    throttle = np.clip(80 + 20 * np.sin(distance / 800), 0, 100)

    return pd.DataFrame(
        {
            "distance": distance,
            "speed": speed,
            "throttle": throttle,
            "brake": 100 - throttle,
            "gear": np.random.randint(1, 8, 100),
        }
    )


@pytest.fixture
def sample_race_results() -> pd.DataFrame:
    """Create sample race results data."""

    return pd.DataFrame(
        {
            "position": [1, 2, 3, 4, 5],
            "driver": ["VER", "HAM", "LEC", "NOR", "SAI"],
            "team": ["Red Bull", "Mercedes", "Ferrari", "McLaren", "Ferrari"],
            "points": [25, 18, 15, 12, 10],
            "status": ["Finished"] * 5,
            "time": ["1:32:15.123", "+5.234", "+12.456", "+18.789", "+25.123"],
        }
    )


@pytest.fixture
def fastf1_session_mock():
    """Create a mock FastF1 session object."""

    mock_session = MagicMock()
    mock_session.event = MagicMock()
    mock_session.event.EventName = "Bahrain Grand Prix"
    mock_session.event.EventDate = "2024-03-02"
    mock_session.session_name = "Race"

    # Mock laps data
    mock_session.laps = pd.DataFrame(
        {
            "Time": pd.timedelta_range(start="0 days 00:00:00", periods=10, freq="90s"),
            "Driver": ["VER"] * 10,
            "LapTime": pd.timedelta_range(
                start="0 days 00:01:30", periods=10, freq="1s"
            ),
            "Compound": ["SOFT"] * 5 + ["MEDIUM"] * 5,
        }
    )

    return mock_session


# =============================================================================
# Mock Fixtures for External APIs
# =============================================================================


@pytest.fixture
def mock_fastf1_api(mocker: Callable[..., Generator[MockerFixture, None, None]]):
    """Mock the FastF1 API for testing."""

    mock_api = mocker.patch("fastf1.get_session")

    # Configure mock session
    mock_session = MagicMock()
    mock_session.load.return_value = None
    mock_session.laps = pd.DataFrame(
        {
            "LapNumber": [1, 2, 3],
            "LapTime": [90.123, 89.456, 88.789],
            "Driver": ["VER", "VER", "VER"],
        }
    )

    mock_api.return_value = mock_session
    return mock_api


@pytest.fixture
def mock_supabase_client(mocker: Callable[..., Generator[MockerFixture, None, None]]):  # pylint: disable=unused-argument
    """Mock Supabase client for testing."""

    mock_client = MagicMock()

    # Configure common operations
    mock_client.table.return_value.select.return_value.execute.return_value = MagicMock(
        data=[{"id": 1, "name": "test"}]
    )

    mock_client.table.return_value.insert.return_value.execute.return_value = MagicMock(
        data=[{"id": 1}]
    )

    return mock_client


# =============================================================================
# Utility Fixtures
# =============================================================================


@pytest.fixture
def assert_dataframes_equal():
    """Fixture that provides a function to compare DataFrames."""

    def _assert_equal(df1: pd.DataFrame, df2: pd.DataFrame, **kwargs):
        """Assert two DataFrames are equal."""
        pd.testing.assert_frame_equal(df1, df2, **kwargs)

    return _assert_equal


@pytest.fixture
def file_comparison_helper():
    """Helper for comparing files."""

    def _compare_files(file1: Path, file2: Path) -> bool:
        """Compare two files byte by byte."""
        with open(file1, "rb") as f1, open(file2, "rb") as f2:
            return f1.read() == f2.read()

    return _compare_files


@pytest.fixture(autouse=True)
def cleanup_test_files(temp_dir: Path):  # pylint: disable=unused-argument
    """Automatically cleanup test files after each test."""

    yield
    # Cleanup happens automatically with temp_dir context manager


@pytest.fixture
def capture_logs(caplog: pytest.LogCaptureFixture):
    """Fixture to capture and analyze logs."""

    caplog.set_level(logging.INFO)
    return caplog


# =============================================================================
# Performance Testing Fixtures
# =============================================================================


@pytest.fixture
def benchmark_timer():
    """Fixture for timing test operations."""

    class Timer:
        """Mock timer class"""

        def __init__(self):
            self.times = {}

        def start(self, name: str):
            """Start timing an operation."""

            self.times[name] = {"start": time.time()}

        def stop(self, name: str):
            """Stop timing an operation."""

            if name in self.times and "start" in self.times[name]:
                self.times[name]["end"] = time.time()
                self.times[name]["duration"] = (
                    self.times[name]["end"] - self.times[name]["start"]
                )

        def get_duration(self, name: str) -> float:
            """Get the duration of a timed operation."""

            return self.times.get(name, {}).get("duration", 0.0)

    return Timer()


# =============================================================================
# Parametrized Test Data
# =============================================================================


@pytest.fixture(params=["parquet", "csv", "json"])
def file_format(request: pytest.FixtureRequest):
    """Parametrize tests across different file formats."""

    return request.param


@pytest.fixture(params=["2023", "2024"])
def test_year(request: pytest.FixtureRequest):
    """Parametrize tests across different years."""

    return request.param


@pytest.fixture(
    params=[
        "Bahrain Grand Prix",
        "Saudi Arabian Grand Prix",
        "Australian Grand Prix",
        "Japanese Grand Prix",
        "Chinese Grand Prix",
        "Miami Grand Prix",
        "Monaco Grand Prix",
        "Spanish Grand Prix",
    ]
)
def grand_prix(request: pytest.FixtureRequest):
    """Parametrize tests across different Grand Prix races."""

    return request.param


@pytest.fixture(params=["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"])
def session_type(request: pytest.FixtureRequest):
    """Parametrize tests across different session types."""

    return request.param


# =============================================================================
# Markers and Skip Conditions
# =============================================================================

skip_if_no_docker = pytest.mark.skipif(
    not os.path.exists("/.dockerenv") and os.system("docker ps > /dev/null 2>&1") != 0,
    reason="Docker services not available",
)


skip_if_no_network = pytest.mark.skipif(
    os.system("ping -c 1 google.com > /dev/null 2>&1") != 0,
    reason="Network not available",
)


# =============================================================================
# Custom Assertions
# =============================================================================


@pytest.fixture
def assert_valid_bucket_path():
    """Custom assertion for valid BucketPath."""

    def _assert(path: BucketPath):
        assert path.bucket, "Bucket name cannot be empty"
        assert path.year, "Year cannot be empty"
        assert path.grand_prix, "Grand Prix cannot be empty"
        assert path.session, "Session cannot be empty"
        assert path.filename, "Filename cannot be empty"
        assert "/" not in path.filename, "Filename should not contain '/'"

    return _assert


@pytest.fixture
def assert_valid_parquet_file():
    """Custom assertion for valid parquet files."""

    def _assert(file_path: Path):
        assert file_path.exists(), f"File does not exist: {file_path}"
        assert file_path.suffix == ".parquet", f"Not a parquet file: {file_path}"

        # Try to read it
        df = pd.read_parquet(file_path)
        assert len(df) > 0, "Parquet file is empty"
        assert len(df.columns) > 0, "Parquet file has no columns"

    return _assert
