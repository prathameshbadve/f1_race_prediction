"""
Catalog raw data downloaded from API
"""

import logging
from dataclasses import dataclass, field
from io import BytesIO
from typing import List

import pandas as pd

from dagster_project.shared.resources import BucketClient, BucketPath


@dataclass
class SessionFiles:
    """Files required for a sesssion"""

    results: bool = False
    laps: bool = False
    weather: bool = False
    session_status: bool = False
    track_status: bool = False
    messages: bool = False

    def is_complete(self):
        """
        Check if a session has all critical files

        Critical files:
            - results
            - laps
            - weather
        """

        return all((self.results, self.laps, self.weather))

    def completeness_score(self) -> float:
        """Calculate completeness percentage"""

        total_files = 6
        available = sum(
            [
                self.results,
                self.laps,
                self.weather,
                self.track_status,
                self.session_status,
                self.messages,
            ]
        )
        return (available / total_files) * 100


@dataclass
class RaceCatalogEntry:
    """
    Fields in a single catalog entry. One entry per F1 grand prix,
    eg - 2024|Italian Grand Prix|Race
    """

    round_number: int
    year: int
    grand_prix: str
    session_number: int
    session: str
    file_path: BucketPath

    # Metadata
    total_drivers: int
    total_laps: int = 0  # 0 for sessions other than race or sprint race
    event_format: str = "conventional"

    # Files availability and data quality
    files: SessionFiles = field(default_factory=SessionFiles)
    completeness_score: float = 0.0
    has_valid_results: bool = False
    has_valid_laps: bool = False
    has_valid_session_status: bool = False
    has_valid_track_status: bool = False
    has_valid_weather: bool = False
    has_valid_messages: bool = False

    def to_dict(self):
        """Convert the catalog entry to a dictionary"""

        return {
            "round_number": self.round_number,
            "year": self.year,
            "grand_prix": self.grand_prix,
            "session_number": self.session_number,
            "session": self.session,
            "file_path": self.file_path,
            # Metadata
            "total_drivers": self.total_drivers,
            "total_laps": self.total_laps,
            "event_format": self.event_format,
            # Files flags
            "has_results": self.files.results,
            "has_laps": self.files.laps,
            "has_session_status": self.files.session_status,
            "has_track_status": self.files.track_status,
            "has_weather": self.files.weather,
            "has_messages": self.files.messages,
            "completeness_score": self.completeness_score,
            "is_complete": self.files.is_complete(),
        }


class RaceDataCatalogBuilder:
    """Builds the catalog for raw data from FastF1 API"""

    EXPECTED_FILES = [
        "results.parquet",
        "laps.parquet",
        "session_status.parquet",
        "track_status.parquet",
        "weather.parquet",
        "messages.parquet",
    ]

    def __init__(
        self,
        bucket_client: BucketClient,
        logger: logging.Logger,
    ):
        self.bucket_client = bucket_client
        self.logger = logger

        self.catalog_entries: List[RaceCatalogEntry] = []

    def update_data_catalog_with_year(self, year: int) -> pd.DataFrame:
        """Downloads the existing data catalog and updates with new data"""

        catalog_key = "validation/catalog.parquet"
        try:
            existing_catalog_data = self.bucket_client.download_file(
                bucket_name=self.bucket_client.processed_data_bucket,
                object_key=catalog_key,
            )
            existing_catalog = pd.read_parquet(BytesIO(existing_catalog_data))
        except Exception as e:  # pylint: disable=broad-except
            self.logger.info("Error downloading the existing catalog file %s", str(e))
            existing_catalog = pd.DataFrame()

        year_catalog_df = self._build_year_catalog(year=year)

        year_catalog_df = self._add_derived_features(df=year_catalog_df)

        # Merge logic: keep the latest version of each record
        combined_catalog = pd.concat(
            [existing_catalog, year_catalog_df]
        ).drop_duplicates(subset=["entry_id"], keep="last")

        buffer = BytesIO()
        combined_catalog.to_parquet(buffer, index=False)
        buffer.seek(0)

        upload_status = self.bucket_client.upload_file(
            bucket_name=self.bucket_client.processed_data_bucket,
            object_key=catalog_key,
            file_obj=buffer,
        )

        self.logger.info(
            "Uploaded the updated catalog to bucket %s status: %s",
            self.bucket_client.processed_data_bucket,
            upload_status,
        )

        return combined_catalog

    def _build_year_catalog(self, year: int) -> pd.DataFrame:
        """Checks all the data of a year and creates a data catalog"""

        self.logger.info("Building catalog for year %d", year)

        # Download the schedule dataframe to get the data points from that file
        schedule_key = f"schedules/{year}/schedule.parquet"
        schedule_data = self.bucket_client.download_file(
            bucket_name=self.bucket_client.raw_data_bucket,
            object_key=schedule_key,
        )
        schedule_df = pd.read_parquet(BytesIO(schedule_data))

        # Scan the events list for the year
        events = self._scan_year(year=year)

        # Iterate through the events to get the particular sessions
        for event in events:
            sessions = self._scan_event(event=event)

            # Iterate through the sessions for the event and
            # build catalog entry for each session
            for session in sessions:
                self._scan_session(session=session, schedule_df=schedule_df)

        year_catalog_df = self._to_dataframe()

        return year_catalog_df

    def _scan_year(self, year: int):
        """Scan the particular year for data files"""

        self.logger.info("| Scanning year %d for catalog building", year)

        prefix = f"{year}"
        try:
            objects = self.bucket_client.list_objects(
                bucket=self.bucket_client.raw_data_bucket,
                prefix=prefix,
            )

            events = []
            for object_key in objects:
                event = object_key.split("/")[1]
                event_prefix = f"{year}/{event}"
                if event_prefix not in events:
                    events.append(event_prefix)

            return events

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error(
                "Error trying to get the events for year %d: %s", year, str(e)
            )
            raise

    def _scan_event(self, event: str):
        """Scans a particular grand prix for the available sessions"""

        self.logger.info("| | Scanning event %s for catalog building", event)

        prefix = event
        try:
            objects = self.bucket_client.list_objects(
                bucket=self.bucket_client.raw_data_bucket,
                prefix=prefix,
            )

            sessions = []
            for object_key in objects:
                session = object_key.split("/")[2]
                session_prefix = f"{prefix}/{session}"
                if session_prefix not in sessions:
                    sessions.append(session_prefix)

            return sessions

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error(
                "Error trying to get the sessions for event %s: %s", event, str(e)
            )
            raise

    def _scan_session(self, session: str, schedule_df: pd.DataFrame):
        """Scans a particular grand prix session and validates the available files"""

        self.logger.info("| | | Processing session %s for catalog building", session)

        prefix = session
        year = prefix.split("/")[0]
        grand_prix = prefix.split("/")[1]
        session = prefix.split("/")[2]

        try:
            # Get file keys for the session
            file_keys = self.bucket_client.list_objects(
                bucket=self.bucket_client.raw_data_bucket,
                prefix=prefix,
            )

            # Create the SessionFiles object to populate required data on files
            session_files = SessionFiles()
            for file_key in file_keys:
                filename = file_key.split("/")[-1]

                # Check if laps file exists
                if filename == "laps.parquet":
                    session_files.laps = True

                # Check if results file exists
                if filename == "results.parquet":
                    session_files.results = True

                # Check if session_status file exists
                if filename == "session_status.parquet":
                    session_files.session_status = True

                # Check if track_status file exists
                if filename == "track_status.parquet":
                    session_files.track_status = True

                # Check if weather file exists
                if filename == "weather.parquet":
                    session_files.weather = True

                # Check if messages file exists
                if filename == "messages.parquet":
                    session_files.messages = True

            # Get the results file for the data points from that file
            results_data = self.bucket_client.download_file(
                bucket_path=BucketPath(
                    bucket=self.bucket_client.raw_data_bucket,
                    year=prefix.split("/")[0],
                    grand_prix=prefix.split("/")[1],
                    session=prefix.split("/")[2],
                    filename="results.parquet",
                )
            )
            results_df = pd.read_parquet(BytesIO(results_data))

            # Total drivers for the catalog entry
            total_drivers = len(results_df)
            if prefix.split("/")[-1] not in ["Race", "Sprint"]:
                total_laps = 0
            else:
                total_laps = int(
                    results_df[results_df["ClassifiedPosition"] == "1"]["Laps"].iloc[0]
                )

            grand_prix_schedule_entry = schedule_df[
                schedule_df["EventName"] == grand_prix
            ]
            # Round number for the catalog entry
            round_number = int(grand_prix_schedule_entry["RoundNumber"].iloc[0])
            # Event format for the catalog entry
            event_format = grand_prix_schedule_entry["EventFormat"].iloc[0]

            sessions = {
                grand_prix_schedule_entry["Session1"].iloc[0]: 1,
                grand_prix_schedule_entry["Session2"].iloc[0]: 2,
                grand_prix_schedule_entry["Session3"].iloc[0]: 3,
                grand_prix_schedule_entry["Session4"].iloc[0]: 4,
                grand_prix_schedule_entry["Session5"].iloc[0]: 5,
            }

            race_catalog_entry = RaceCatalogEntry(
                round_number=round_number,
                year=year,
                grand_prix=grand_prix,
                session_number=sessions[session],
                session=session,
                file_path=prefix,
                total_drivers=total_drivers,
                total_laps=total_laps,
                event_format=event_format,
                files=session_files,
                completeness_score=session_files.completeness_score(),
            )

            self.catalog_entries.append(race_catalog_entry)

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error(
                "Error trying to get the files for session %s: %s", session, str(e)
            )
            raise

    def _to_dataframe(self):
        """Build dataframe of the cataloged entries"""

        data = [entry.to_dict() for entry in self.catalog_entries]
        df = pd.DataFrame(data)

        # Sort by year, round, session
        df = df.sort_values(["year", "round_number", "session_number"]).reset_index(
            drop=True
        )

        return df

    def _add_derived_features(self, df: pd.DataFrame):
        """Add derived features to data catalog"""

        if df.empty:
            return df

        # Add race_id
        df["entry_id"] = df.apply(
            lambda row: f"{row['year']}_R{row['round_number']:02d}_S{row['session_number']}",  # pylint: disable=line-too-long  # noqa: E501
            axis=1,
        )

        return df
