"""
Resources to be used only in ingestion.
"""

# pylint: disable=broad-except

import logging
from typing import Optional, Tuple

import fastf1
import pandas as pd
from dagster import ConfigurableResource, InitResourceContext
from fastf1 import Cache
from fastf1.core import InvalidSessionError

from src.config.logging import get_logger
from src.config.settings import FastF1Config
from src.utils.helpers import ensure_directory, get_project_root


class FastF1Resource(ConfigurableResource):
    """
    Resource for interacting with FastF1 API
    """

    _logger: logging.Logger = get_logger("fastf1")

    def get_config(self):
        """Get FastF1 config from environment variables"""

        return FastF1Config.from_env()

    def setup_for_execution(
        self,
        context: InitResourceContext,
    ):  # pylint: disable=unused-argument
        """
        Initialize FastF1 cache
        Called automatically by Dagster before job execution.
        """

        config = self.get_config()
        project_root = get_project_root()
        cache_path = project_root / config.cache_dir
        if config.cache_enabled:
            ensure_directory(cache_path)
            Cache.enable_cache(
                cache_dir=str(cache_path),
                force_renew=config.force_renew_cache,
            )
        else:
            Cache.set_disabled()

        fastf1.set_log_level(config.log_level)

        self._logger.info("FastF1Resource initialized. Cache Dir: %s", cache_path)

    @classmethod
    def from_env(cls):
        """Factory to create FastF1Resource from environment variables."""

        return cls(config=FastF1Config.from_env())

    def get_season_schedule(self, year: int) -> Optional[pd.DataFrame]:
        """
        Load an F1 season schedule from API.

        Args:
            year: Season year (e.g., 2024)

        Returns:
            pd.DataFrame
        """

        # Get FastF1 Config
        config = self.get_config()

        try:
            season_schedule = fastf1.get_event_schedule(
                include_testing=config.include_testing,
                year=year,
            )
            self._logger.info(
                "Season schedule for %d loaded successfully from API", year
            )
            return season_schedule

        except Exception as e:  # pylint: disable=broad-except
            self._logger.error(
                "Error while trying to load schedule for %d from API: %s", year, str(e)
            )
            return None

    def get_session_object(
        self,
        year: int,
        grand_prix: str,
        session: str,
    ):
        """
        Gets the session object from FastF1 API

        Args:
            - year
            - grand_prix
            - session

        Returns:
            - session object
        """

        try:
            session_object = fastf1.get_session(
                year=year, gp=grand_prix, identifier=session
            )
            return session_object

        except InvalidSessionError as e:
            error_msg = f"Error while trying to get session object: {str(e)}"
            self._logger.error(error_msg)
            raise InvalidSessionError(error_msg) from e

    def get_session_results(
        self,
        year: int,
        grand_prix: str,
        session: str,
    ) -> Optional[pd.DataFrame]:
        """
        Gets the session results from FastF1 API

        Args:
            - year
            - grand_prix
            - session

        Returns:
            - pd.DataFrame containing the results
        """

        try:
            session_object = self.get_session_object(year, grand_prix, session)
            session_object.load(
                laps=False, telemetry=False, weather=False, messages=False
            )
            return session_object.results

        except Exception as e:
            self._logger.error("Failed to get session results: %s", str(e))
            return None

    def get_session_laps(
        self,
        year: int,
        grand_prix: str,
        session: str,
    ) -> Optional[Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]]:
        """
        Gets the session laps from FastF1 API

        Args:
            - year
            - grand_prix
            - session

        Returns:
            - pd.DataFrame containing the laps
        """

        try:
            session_object = self.get_session_object(year, grand_prix, session)
            session_object.load(
                laps=True, telemetry=False, weather=False, messages=False
            )
            return (
                session_object.laps,
                session_object.session_status,
                session_object.track_status,
            )

        except Exception as e:
            self._logger.error("Failed to get session laps: %s", str(e))
            return None

    def get_session_weather(
        self,
        year: int,
        grand_prix: str,
        session: str,
    ) -> Optional[pd.DataFrame]:
        """
        Gets the session weather from FastF1 API

        Args:
            - year
            - grand_prix
            - session

        Returns:
            - pd.DataFrame containing the weather
        """

        try:
            session_object = self.get_session_object(year, grand_prix, session)
            session_object.load(
                laps=False, telemetry=False, weather=True, messages=False
            )
            return session_object.weather_data

        except Exception as e:
            self._logger.error("Failed to get session weather: %s", str(e))
            return None

    def get_session_messages(
        self,
        year: int,
        grand_prix: str,
        session: str,
    ) -> Optional[pd.DataFrame]:
        """
        Gets the session messages from FastF1 API

        Args:
            - year
            - grand_prix
            - session

        Returns:
            - pd.DataFrame containing the messages
        """

        try:
            session_object = self.get_session_object(year, grand_prix, session)
            session_object.load(
                laps=False, telemetry=False, weather=False, messages=True
            )
            return session_object.race_control_messages

        except Exception as e:
            self._logger.error("Failed to get session messages: %s", str(e))
            return None

    def get_session_info(
        self,
        year: int,
        grand_prix: str,
        session: str,
    ) -> Optional[dict]:
        """
        Gets the session info from FastF1 API

        Args:
            - year
            - grand_prix
            - session

        Returns:
            - pd.DataFrame containing the weather
        """

        try:
            session_object = self.get_session_object(year, grand_prix, session)
            return session_object.session_info

        except Exception as e:
            self._logger.error("Failed to get session info: %s", str(e))
            return None

    def get_session_telemetry(
        self,
        year: int,
        grand_prix: str,
        session: str,
    ) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
        """
        Gets the session telemetry data from FastF1 API

        Args:
            - year
            - grand_prix
            - session

        Returns:
            - pd.DataFrame containing the telemetry data
        """

        try:
            session_object = self.get_session_object(year, grand_prix, session)
            session_object.load(
                laps=False, telemetry=True, weather=False, messages=False
            )
            return session_object.car_data, session_object.pos_data

        except Exception as e:
            self._logger.error("Failed to get session telemetry: %s", str(e))
            return None
