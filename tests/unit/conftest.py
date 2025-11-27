"""
Unit test fixtures
"""

# pylint: disable=redefined-outer-name

import io
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy.orm import Session

from dagster_project.shared.resources import DatabaseClient

# =============================================================================
# Mock Client Fixtures
# =============================================================================


@pytest.fixture
def mock_redis_client():
    """Mock Redis client for testing"""
    mock_client = MagicMock()
    mock_client.ping.return_value = True
    mock_client.set.return_value = True
    mock_client.get.return_value = None
    mock_client.delete.return_value = 1
    mock_client.exists.return_value = False
    mock_client.keys.return_value = []
    mock_client.incr.return_value = 1
    mock_client.expire.return_value = True
    mock_client.ttl.return_value = 3600
    return mock_client


@pytest.fixture
def mock_sqlalchemy_engine():
    """Mock SQLAlchemy engine for testing"""
    mock_engine = MagicMock()
    mock_engine.connect.return_value = MagicMock()
    mock_engine.dispose = MagicMock()
    return mock_engine


@pytest.fixture
def mock_fastf1_session():
    """Mock FastF1 session object"""
    mock_session = MagicMock()
    mock_session.load = MagicMock()
    mock_session.results = None
    mock_session.laps = None
    mock_session.weather_data = None
    mock_session.race_control_messages = None
    mock_session.session_status = None
    mock_session.track_status = None
    return mock_session


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


# =============================================================================
# Config Fixtures
# =============================================================================


@pytest.fixture
def bucket_config_dict() -> dict:
    """Sample bucket configuration"""

    return {
        "endpoint_url": "http://localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
        "region_name": "us-east-1",
        "raw_data_bucket": "test-bucket",
        "processed_data_bucket": "test-bucket-processed",
        "model_bucket": "test-bucket-models",
    }


@pytest.fixture
def redis_config_dict() -> dict:
    """Sample Redis configuration"""

    return {
        "host": "localhost",
        "port": 6379,
        "db": 0,
        "password": None,
    }


@pytest.fixture
def database_config_dict() -> dict:
    """Sample database configuration"""

    return {
        "host": "localhost",
        "port": 5432,
        "user": "testuser",
        "password": "testpass",
        "database": "testdb",
        "backend": "postgresql",
    }


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
def db_client(mock_session_factory):
    """Create a DatabaseClient with mocked internals."""
    with patch.object(DatabaseClient, "__init__", lambda self, *args, **kwargs: None):
        client = DatabaseClient.__new__(DatabaseClient)
        client.session_local = mock_session_factory
        client.logger = Mock()
        return client


@pytest.fixture
def sample_function():
    """Create a sample function to decorate."""
    mock_func = Mock(return_value={"data": "result"})
    mock_func.__name__ = "sample_function"
    mock_func.__doc__ = "Sample docstring"
    return mock_func
