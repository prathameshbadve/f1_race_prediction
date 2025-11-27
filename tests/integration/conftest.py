"""
Shared fixtures for integration tests
"""

# pylint: disable=protected-access

import io
import json
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest
from dagster import AssetExecutionContext

from dagster_project.ingestion.resources import FastF1Resource
from dagster_project.shared.resources import BucketResource


@pytest.fixture
def mock_bucket_client(
    sample_schedule_df: pd.DataFrame,
    sample_race_results_df: pd.DataFrame,
    sample_race_laps_df: pd.DataFrame,
    sample_session_status_df: pd.DataFrame,
    sample_track_status_df: pd.DataFrame,
    sample_weather_df: pd.DataFrame,
    sample_messages_df: pd.DataFrame,
    sample_session_info_dict: dict,
):
    """Create a mock bucket client"""

    client = Mock()
    client.raw_data_bucket = "test-bucket"

    # Create parquet bytes for each DataFrame
    def create_parquet_bytes(df):
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        return buffer.getvalue()

    client._schedule_bytes = create_parquet_bytes(sample_schedule_df)
    client._results_bytes = create_parquet_bytes(sample_race_results_df)
    client._laps_bytes = create_parquet_bytes(sample_race_laps_df)
    client._session_status_bytes = create_parquet_bytes(sample_session_status_df)
    client._track_status_bytes = create_parquet_bytes(sample_track_status_df)
    client._weather_bytes = create_parquet_bytes(sample_weather_df)
    client._messages_bytes = create_parquet_bytes(sample_messages_df)
    client._session_info_bytes = io.BytesIO(
        json.dumps(
            sample_session_info_dict,
            default=str,
            ensure_ascii=False,
            indent=None,
        ).encode("utf-8")
    )

    return client


@pytest.fixture
def mock_bucket_resource(mock_bucket_client):  # pylint: disable=redefined-outer-name
    """Create a mock bucket resource"""

    resource = Mock(spec=BucketResource)
    resource.get_client.return_value = mock_bucket_client
    return resource


@pytest.fixture
def mock_fastf1_resource(
    sample_schedule_df: pd.DataFrame,
    sample_race_results_df: pd.DataFrame,
    sample_race_laps_df: pd.DataFrame,
    sample_session_status_df: pd.DataFrame,
    sample_track_status_df: pd.DataFrame,
    sample_weather_df: pd.DataFrame,
    sample_messages_df: pd.DataFrame,
    sample_session_info_dict: dict,
):
    """Create a mock FastF1 resource"""

    resource = Mock(spec=FastF1Resource)

    mock_session = type(
        "Session",
        (),
        {
            "load": lambda *args, **kwargs: None,
            "results": sample_race_results_df,
            "laps": sample_race_laps_df,
            "session_status": sample_session_status_df,
            "track_status": sample_track_status_df,
            "weather_data": sample_weather_df,
            "race_control_messages": sample_messages_df,
            "session_info": sample_session_info_dict,
        },
    )()

    resource.get_season_schedule.return_value = sample_schedule_df
    resource.get_session_object.return_value = mock_session
    resource.get_session_results.return_value = sample_race_results_df
    resource.get_session_laps.return_value = (
        sample_race_laps_df,
        sample_session_status_df,
        sample_track_status_df,
    )
    resource.get_session_weather.return_value = sample_weather_df
    resource.get_session_messages.return_value = sample_messages_df
    resource.get_session_info.return_value = sample_session_info_dict

    return resource


@pytest.fixture
def mock_context():
    """Create a mock asset execution context"""

    context = MagicMock(spec=AssetExecutionContext)
    context.partition_key = "2024|Italian Grand Prix|Race"
    context.add_output_metadata = Mock()
    context.log = Mock()
    return context
