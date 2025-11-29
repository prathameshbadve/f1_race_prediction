"""
Root conftest.py - Shared fixtures for all tests
"""

import io
import tempfile
from pathlib import Path
from typing import Generator

import numpy as np
import pandas as pd
import pytest
from dagster import build_asset_context, build_op_context

# ==========================================================
# ENVIRONMENT SETUP
# ==========================================================


@pytest.fixture(autouse=True)
def setup_environment_variables(monkeypatch: pytest.MonkeyPatch):
    """Mock environment variables for testing"""

    env_vars = {
        # Environment
        "ENVIRONMENT": "testing",
        # MinIO/S3
        "DOCKER_ENDPOINT": "http://minio:9000",
        "LOCAL_ENDPOINT": "http://localhost:9000",
        "ACCESS_KEY": "minioadmin",
        "SECRET_KEY": "minioadmin",
        "REGION": "ap-south-1",
        # Buckets
        "RAW_DATA_BUCKET": "test-bucket-raw",
        "PROCESSED_DATA_BUCKET": "test-bucket-processed",
        "MODEL_BUCKET": "test-bucket-models",
        # Redis
        "REDIS_LOCAL_HOST": "localhost",
        "REDIS_DOCKER_HOST": "redis",
        "REDIS_PORT": "6379",
        "REDIS_DB": "0",
        "REDIS_PASSWORD": "testredisadmin",
        # Redis Comander
        "REDIS_COMMANDER_USER": "testredisuser",
        "REDIS_COMMANDER_PASSWORD": "testredispass",
        # Database
        "DB_BACKEND": "postgresql",
        "DB_HOST": "postgres",
        "DB_LOCAL_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_DEFAULT_NAME": "postgres",
        "DB_NAME": "test_data_warehouse",
        "DB_USER": "testuser",
        "DB_PASSWORD": "testpass",
        # Add MLFlow variables here
        # Project Logging
        "LOG_DIR": "test_monitoring/logs/",
        "LOG_LEVEL": "DEBUG",
        # FastF1
        "FASTF1_CACHE_DIR": "data/test_cache",
        "FASTF1_CACHE_ENABLED": "False",
        "FASTF1_FORCE_RENEW_CACHE": "False",
        "FASTF1_LOG_LEVEL": "WARNING",
        "FASTF1_REQUEST_TIMEOUT": "30",
        "FASTF1_MAX_RETRIES": "3",
        "FASTF1_RETRY_DELAY": "5",
        "FASTF1_INCLUDE_TESTING": "False",
    }

    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)

    yield env_vars


@pytest.fixture(scope="session")
def test_data_dir() -> Generator[Path, None, None]:
    """Create temporary directory for test data"""

    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# ==========================================================
# SAMPLE DATA FIXTURES
# ==========================================================

schedule_df = pd.DataFrame(
    {
        "RoundNumber": [1, 2, 3],
        "Country": ["Bahrain", "Saudi Arabia", "Australia"],
        "Location": ["Sakhir", "Jeddah", "Melbourne"],
        "OfficialEventName": [
            "FORMULA 1 GULF AIR BAHRAIN GRAND PRIX 2024",
            "FORMULA 1 STC SAUDI ARABIAN GRAND PRIX 2024",
            "FORMULA 1 ROLEX AUSTRALIAN GRAND PRIX 2024",
        ],
        "EventDate": [
            pd.Timestamp("2024-03-02 00:00:00"),
            pd.Timestamp("2024-03-09 00:00:00"),
            pd.Timestamp("2024-03-24 00:00:00"),
        ],
        "EventName": [
            "Bahrain Grand Prix",
            "Saudi Arabian Grand Prix",
            "Australian Grand Prix",
        ],
        "EventFormat": ["conventional", "conventional", "conventional"],
        "Session1": ["Practice 1", "Practice 1", "Practice 1"],
        "Session1Date": [
            pd.Timestamp("2024-02-29 14:30:00+0300"),
            pd.Timestamp("2024-03-07 16:30:00+0300"),
            pd.Timestamp("2024-03-22 04:30:00+0300"),
        ],
        "Session1DateUtc": [
            pd.Timestamp("2024-02-29 11:30:00"),
            pd.Timestamp("2024-03-07 13:30:00"),
            pd.Timestamp("2024-03-22 01:30:00"),
        ],
        "Session2": ["Practice 2", "Practice 2", "Practice 2"],
        "Session2Date": [
            pd.Timestamp("2024-02-29 14:30:00+0300"),
            pd.Timestamp("2024-03-07 16:30:00+0300"),
            pd.Timestamp("2024-03-22 04:30:00+0300"),
        ],
        "Session2DateUtc": [
            pd.Timestamp("2024-02-29 11:30:00"),
            pd.Timestamp("2024-03-07 13:30:00"),
            pd.Timestamp("2024-03-22 01:30:00"),
        ],
        "Session3": ["Practice 3", "Practice 3", "Practice 3"],
        "Session3Date": [
            pd.Timestamp("2024-02-29 14:30:00+0300"),
            pd.Timestamp("2024-03-07 16:30:00+0300"),
            pd.Timestamp("2024-03-22 04:30:00+0300"),
        ],
        "Session3DateUtc": [
            pd.Timestamp("2024-02-29 11:30:00"),
            pd.Timestamp("2024-03-07 13:30:00"),
            pd.Timestamp("2024-03-22 01:30:00"),
        ],
        "Session4": ["Qualifying", "Qualifying", "Qualifying"],
        "Session4Date": [
            pd.Timestamp("2024-02-29 14:30:00+0300"),
            pd.Timestamp("2024-03-07 16:30:00+0300"),
            pd.Timestamp("2024-03-22 04:30:00+0300"),
        ],
        "Session4DateUtc": [
            pd.Timestamp("2024-02-29 11:30:00"),
            pd.Timestamp("2024-03-07 13:30:00"),
            pd.Timestamp("2024-03-22 01:30:00"),
        ],
        "Session5": ["Race", "Race", "Race"],
        "Session5Date": [
            pd.Timestamp("2024-02-29 14:30:00+0300"),
            pd.Timestamp("2024-03-07 16:30:00+0300"),
            pd.Timestamp("2024-03-22 04:30:00+0300"),
        ],
        "Session6DateUtc": [
            pd.Timestamp("2024-02-29 11:30:00"),
            pd.Timestamp("2024-03-07 13:30:00"),
            pd.Timestamp("2024-03-22 01:30:00"),
        ],
        "F1ApiSupport": [True, True, True],
    }
)


@pytest.fixture
def sample_schedule_df() -> pd.DataFrame:
    """Sample F1 season schedule DataFrame"""

    return schedule_df


@pytest.fixture
def sample_schedule_parquet_bytes():
    """Sample parquet bytes"""

    buffer = io.BytesIO()
    schedule_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    return buffer.getvalue()


race_laps_df = pd.DataFrame(
    {
        "Time": {
            0: pd.Timedelta("0 days 00:57:18.931000"),
            1: pd.Timedelta("0 days 00:58:44.327000"),
            2: pd.Timedelta("0 days 01:00:09.506000"),
        },
        "Driver": {0: "LEC", 1: "LEC", 2: "LEC"},
        "DriverNumber": {0: "16", 1: "16", 2: "16"},
        "LapTime": {
            0: pd.Timedelta("0 days 00:01:28.179000"),
            1: pd.Timedelta("0 days 00:01:25.396000"),
            2: pd.Timedelta("0 days 00:01:25.179000"),
        },
        "LapNumber": {0: 1.0, 1: 2.0, 2: 3.0},
        "Stint": {0: 1.0, 1: 1.0, 2: 1.0},
        "PitOutTime": {0: None, 1: None, 2: None},
        "PitInTime": {0: None, 1: None, 2: None},
        "Sector1Time": {
            0: None,
            1: pd.Timedelta("0 days 00:00:27.707000"),
            2: pd.Timedelta("0 days 00:00:27.679000"),
        },
        "Sector2Time": {
            0: pd.Timedelta("0 days 00:00:29.989000"),
            1: pd.Timedelta("0 days 00:00:29.265000"),
            2: pd.Timedelta("0 days 00:00:29.001000"),
        },
        "Sector3Time": {
            0: pd.Timedelta("0 days 00:00:28.398000"),
            1: pd.Timedelta("0 days 00:00:28.424000"),
            2: pd.Timedelta("0 days 00:00:28.499000"),
        },
        "Sector1SessionTime": {
            0: None,
            1: pd.Timedelta("0 days 00:57:46.661000"),
            2: pd.Timedelta("0 days 00:59:12.029000"),
        },
        "Sector2SessionTime": {
            0: pd.Timedelta("0 days 00:56:50.708000"),
            1: pd.Timedelta("0 days 00:58:15.926000"),
            2: pd.Timedelta("0 days 00:59:41.030000"),
        },
        "Sector3SessionTime": {
            0: pd.Timedelta("0 days 00:57:19.087000"),
            1: pd.Timedelta("0 days 00:58:44.350000"),
            2: pd.Timedelta("0 days 01:00:09.529000"),
        },
        "SpeedI1": {0: 316.0, 1: 315.0, 2: 313.0},
        "SpeedI2": {0: 313.0, 1: 325.0, 2: 324.0},
        "SpeedFL": {0: 311.0, 1: 312.0, 2: 313.0},
        "SpeedST": {0: 300.0, 1: 325.0, 2: 328.0},
        "IsPersonalBest": {0: False, 1: True, 2: True},
        "Compound": {0: "MEDIUM", 1: "MEDIUM", 2: "MEDIUM"},
        "TyreLife": {0: 1.0, 1: 2.0, 2: 3.0},
        "FreshTyre": {0: True, 1: True, 2: True},
        "Team": {0: "Ferrari", 1: "Ferrari", 2: "Ferrari"},
        "LapStartTime": {
            0: pd.Timedelta("0 days 00:55:50.494000"),
            1: pd.Timedelta("0 days 00:57:18.931000"),
            2: pd.Timedelta("0 days 00:58:44.327000"),
        },
        "LapStartDate": {0: None, 1: None, 2: None},
        "TrackStatus": {0: "1", 1: "1", 2: "1"},
        "Position": {0: 2.0, 1: 2.0, 2: 2.0},
        "Deleted": {0: None, 1: None, 2: None},
        "DeletedReason": {0: "", 1: "", 2: ""},
        "FastF1Generated": {0: False, 1: False, 2: False},
        "IsAccurate": {0: False, 1: True, 2: True},
    }
)


@pytest.fixture
def sample_race_laps_df() -> pd.DataFrame:
    """Sample F1 session laps DataFrame"""

    return race_laps_df


@pytest.fixture
def sample_race_laps_parquet_bytes():
    """Sample race laps parquet bytes"""

    buffer = io.BytesIO()
    race_laps_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    return buffer.getvalue()


race_results_df = pd.DataFrame(
    {
        "DriverNumber": {0: "16", 1: "81", 2: "4"},
        "BroadcastName": {0: "C LECLERC", 1: "O PIASTRI", 2: "L NORRIS"},
        "Abbreviation": {0: "LEC", 1: "PIA", 2: "NOR"},
        "DriverId": {0: "leclerc", 1: "piastri", 2: "norris"},
        "TeamName": {0: "Ferrari", 1: "McLaren", 2: "McLaren"},
        "TeamColor": {0: "E80020", 1: "FF8000", 2: "FF8000"},
        "TeamId": {0: "ferrari", 1: "mclaren", 2: "mclaren"},
        "FirstName": {0: "Charles", 1: "Oscar", 2: "Lando"},
        "LastName": {0: "Leclerc", 1: "Piastri", 2: "Norris"},
        "FullName": {0: "Charles Leclerc", 1: "Oscar Piastri", 2: "Lando Norris"},
        "HeadshotUrl": {
            0: "www.google.com",
            1: "www.google.com",
            2: "www.google.com",
        },
        "CountryCode": {0: "MON", 1: "AUS", 2: "GBR"},
        "Position": {0: 1.0, 1: 2.0, 2: 3.0},
        "ClassifiedPosition": {0: "1", 1: "2", 2: "3"},
        "GridPosition": {0: 4.0, 1: 2.0, 2: 1.0},
        "Q1": {0: None, 1: None, 2: None},
        "Q2": {0: None, 1: None, 2: None},
        "Q3": {0: None, 1: None, 2: None},
        "Time": {
            0: pd.Timedelta("0 days 01:14:40.727000"),
            1: pd.Timedelta("0 days 00:00:02.664000"),
            2: pd.Timedelta("0 days 00:00:06.153000"),
        },
        "Status": {0: "Finished", 1: "Finished", 2: "Finished"},
        "Points": {0: 25.0, 1: 18.0, 2: 16.0},
        "Laps": {0: 53.0, 1: 53.0, 2: 53.0},
    }
)


@pytest.fixture
def sample_race_results_df() -> pd.DataFrame:
    """Sample F1 race results DatFrame"""

    return race_results_df


@pytest.fixture
def sample_race_results_parquet_bytes():
    """Sample race results parquet bytes"""

    buffer = io.BytesIO()
    race_results_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    return buffer.getvalue()


session_status_df = pd.DataFrame(
    {
        "Time": {
            0: pd.Timedelta("0 days 00:00:05.174000"),
            1: pd.Timedelta("0 days 00:55:50.494000"),
            2: pd.Timedelta("0 days 02:10:31.456000"),
        },
        "Status": {0: "Inactive", 1: "Started", 2: "Finished"},
    }
)


@pytest.fixture
def sample_session_status_df() -> pd.DataFrame:
    """Sample session status DataFrame"""

    return session_status_df


@pytest.fixture
def sample_session_status_parquet_bytes():
    """Sample session status parquet bytes"""

    buffer = io.BytesIO()
    session_status_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    return buffer.getvalue()


track_status_df = pd.DataFrame(
    {
        "Time": {
            0: pd.Timedelta("0 days 00:00:00"),
            1: pd.Timedelta("0 days 00:46:32.143000"),
            2: pd.Timedelta("0 days 00:46:35.717000"),
        },
        "Status": {0: "1", 1: "2", 2: "1"},
        "Message": {0: "AllClear", 1: "Yellow", 2: "AllClear"},
    }
)


@pytest.fixture
def sample_track_status_df() -> pd.DataFrame:
    """Sample track status DataFrame"""

    return track_status_df


@pytest.fixture
def sample_track_status_parquet_bytes():
    """Sample track status parquet bytes"""

    buffer = io.BytesIO()
    track_status_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    return buffer.getvalue()


weather_df = pd.DataFrame(
    {
        "Time": {
            0: pd.Timedelta("0 days 00:01:01.035000"),
            1: pd.Timedelta("0 days 00:02:01.033000"),
            2: pd.Timedelta("0 days 00:03:01.033000"),
        },
        "AirTemp": {0: 17.9, 1: 17.9, 2: 17.9},
        "Humidity": {0: 70.0, 1: 70.0, 2: 70.0},
        "Pressure": {0: 929.9, 1: 929.9, 2: 930.0},
        "Rainfall": {0: False, 1: False, 2: False},
        "TrackTemp": {0: 32.7, 1: 32.7, 2: 32.8},
        "WindDirection": {0: 124, 1: 160, 2: 178},
        "WindSpeed": {0: 1.0, 1: 1.3, 2: 1.5},
    }
)


@pytest.fixture
def sample_weather_df() -> pd.DataFrame:
    """Sample weather DataFrame"""

    return weather_df


@pytest.fixture
def sample_weather_parquet_bytes():
    """Sample weather parquet bytes"""

    buffer = io.BytesIO()
    weather_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    return buffer.getvalue()


messages_df = pd.DataFrame(
    {
        "Time": {
            0: pd.Timestamp("2025-11-09 16:16:20"),
            1: pd.Timestamp("2025-11-09 16:20:01"),
            2: pd.Timestamp("2025-11-09 16:30:01"),
        },
        "Category": {0: "Other", 1: "Flag", 2: "Other"},
        "Message": {
            0: "AWNINGS MAY BE USED",
            1: "GREEN LIGHT - PIT EXIT OPEN",
            2: "PIT EXIT CLOSED",
        },
        "Status": {0: None, 1: None, 2: None},
        "Flag": {0: None, 1: "GREEN", 2: None},
        "Scope": {0: None, 1: "Track", 2: None},
        "Sector": {
            0: np.float64("nan"),
            1: np.float64("nan"),
            2: np.float64("nan"),
        },
        "RacingNumber": {0: None, 1: None, 2: None},
        "Lap": {0: 1, 1: 1, 2: 1},
    }
)


@pytest.fixture
def sample_messages_df() -> pd.DataFrame:
    """Sample race control messges DataFrame"""

    return messages_df


@pytest.fixture
def sample_messages_parquet_bytes():
    """Sample messages parquet bytes"""

    buffer = io.BytesIO()
    messages_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    return buffer.getvalue()


@pytest.fixture
def sample_session_info_dict() -> dict:
    """Sample session info dict"""

    return {
        "Meeting": {
            "Key": 1273,
            "Name": "São Paulo Grand Prix",
            "OfficialName": "FORMULA 1 MSC CRUISES GRANDE PRÊMIO DE SÃO PAULO 2025",
            "Location": "São Paulo",
            "Number": 21,
            "Country": {"Key": 10, "Code": "BRA", "Name": "Brazil"},
            "Circuit": {"Key": 14, "ShortName": "Interlagos"},
        },
        "SessionStatus": "Inactive",
        "ArchiveStatus": {"Status": "Generating"},
        "Key": 9869,
        "Type": "Race",
        "Name": "Race",
        "StartDate": pd.Timestamp(2025, 11, 9, 14, 0),
        "EndDate": pd.Timestamp(2025, 11, 9, 16, 0),
        "GmtOffset": pd.Timedelta(days=-1, seconds=75600),
        "Path": "2025/2025-11-09_São_Paulo_Grand_Prix/2025-11-09_Race/",
    }


# ==========================================================
# DAGSTER CONTEXT FIXTURES
# ==========================================================


@pytest.fixture
def dagster_asset_context():
    """Build a Dagster asset execution context for testing"""

    return build_asset_context()


@pytest.fixture
def dagster_op_context():
    """Build a Dagster op context for testing"""

    return build_op_context()
