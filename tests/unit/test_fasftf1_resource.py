"""
Unit tests for FastF1Resource
"""

# pylint: disable=unused-argument

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastf1.core import InvalidSessionError, Session

from dagster_project.ingestion.resources import FastF1Resource


@pytest.mark.unit
class TestFastF1Init:
    """Test FastF1 Resource Initialization"""

    def test_get_config(self):
        """Test config based on environment"""

        fastf1_resource = FastF1Resource.from_env()
        config = fastf1_resource.get_config()

        assert config.cache_enabled is False
        assert config.cache_dir == "data/test_cache"
        assert config.log_level == "WARNING"


@pytest.mark.unit
class TestGetSeasonSchedule:
    """Test get season schedule"""

    def test_get_season_schedule_success(self, mock_fastf1_resource):
        """Test get season schedule"""

        result = mock_fastf1_resource.get_season_schedule(year=2024)

        assert len(result) == 3
        assert isinstance(result, pd.DataFrame)


@pytest.mark.unit
class TestGetSessionData:
    """Test get session object and associated data"""

    def test_get_session_object(self, mock_fastf1_resource, mock_fastf1_session):
        """Test get session object"""

        session_object = mock_fastf1_resource.get_session_object(
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
        )

        assert session_object == mock_fastf1_session

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_get_session_object_failure(self, mock_fastf1):
        """Test get session object"""

        mock_fastf1.get_session.side_effect = InvalidSessionError()

        fastf1_resource = FastF1Resource.from_env()

        with pytest.raises(InvalidSessionError):
            fastf1_resource.get_session_object(
                year=2024,
                grand_prix="Italian Grand Prix",
                session="Race",
            )

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_get_session_results(self, mock_fastf1, sample_race_results_df):
        """Test session results"""

        mock_session = MagicMock(spec=Session)
        mock_session.load = MagicMock()
        mock_session.results = sample_race_results_df
        mock_fastf1.get_session.return_value = mock_session

        fastf1_resource = FastF1Resource.from_env()
        results = fastf1_resource.get_session_results(
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
        )

        mock_fastf1.get_session.assert_called_once()
        mock_session.load.assert_called_once_with(
            laps=False, telemetry=False, weather=False, messages=False
        )
        assert isinstance(results, pd.DataFrame)
        assert len(results) == 3

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_get_session_laps(
        self,
        mock_fastf1,
        sample_race_laps_df,
        sample_session_status_df,
        sample_track_status_df,
    ):
        """Test session results"""

        mock_session = MagicMock(spec=Session)
        mock_session.load = MagicMock()
        mock_session.laps = sample_race_laps_df
        mock_session.session_status = sample_session_status_df
        mock_session.track_status = sample_track_status_df
        mock_fastf1.get_session.return_value = mock_session

        fastf1_resource = FastF1Resource.from_env()
        laps, session_status, track_status = fastf1_resource.get_session_laps(
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
        )

        mock_fastf1.get_session.assert_called_once()
        mock_session.load.assert_called_once_with(
            laps=True, telemetry=False, weather=False, messages=False
        )
        assert isinstance(laps, pd.DataFrame)
        assert isinstance(session_status, pd.DataFrame)
        assert isinstance(track_status, pd.DataFrame)
        assert len(laps) == 3
        assert len(session_status) == 3
        assert len(track_status) == 3

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_get_session_weather(self, mock_fastf1, sample_weather_df):
        """Test session weather"""

        mock_session = MagicMock(spec=Session)
        mock_session.load = MagicMock()
        mock_session.weather_data = sample_weather_df
        mock_fastf1.get_session.return_value = mock_session

        fastf1_resource = FastF1Resource.from_env()
        weather = fastf1_resource.get_session_weather(
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
        )

        mock_fastf1.get_session.assert_called_once()
        mock_session.load.assert_called_once_with(
            laps=False, telemetry=False, weather=True, messages=False
        )
        assert isinstance(weather, pd.DataFrame)
        assert len(weather) == 3

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_get_session_messages(self, mock_fastf1, sample_messages_df):
        """Test session messages"""

        mock_session = MagicMock(spec=Session)
        mock_session.load = MagicMock()
        mock_session.race_control_messages = sample_messages_df
        mock_fastf1.get_session.return_value = mock_session

        fastf1_resource = FastF1Resource.from_env()
        messages = fastf1_resource.get_session_messages(
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
        )

        mock_fastf1.get_session.assert_called_once()
        mock_session.load.assert_called_once_with(
            laps=False, telemetry=False, weather=False, messages=True
        )
        assert isinstance(messages, pd.DataFrame)
        assert len(messages) == 3

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_get_session_info(self, mock_fastf1, sample_session_info_dict):
        """Test session info"""

        mock_session = MagicMock(spec=Session)
        mock_session.load = MagicMock()
        mock_session.session_info = sample_session_info_dict
        mock_fastf1.get_session.return_value = mock_session

        fastf1_resource = FastF1Resource.from_env()
        messages = fastf1_resource.get_session_info(
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
        )

        mock_fastf1.get_session.assert_called_once()
        mock_session.load.assert_not_called()
        assert isinstance(messages, dict)
