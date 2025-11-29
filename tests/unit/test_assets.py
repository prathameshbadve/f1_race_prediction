"""
Unit tests for Dagster assets
"""

from unittest.mock import ANY, MagicMock, patch

import pytest
from dagster import build_asset_context
from fastf1.core import Session

from dagster_project.ingestion.assets import (
    season_schedule,
    session_laps,
    session_messages,
    session_results,
    session_weather,
)
from dagster_project.ingestion.resources import FastF1Resource
from dagster_project.shared.resources import BucketPath


@pytest.mark.unit
class TestSeasonSchedule:
    """Test season schedule asset"""

    def test_season_schedule_cache_miss(
        self,
        mock_bucket_client,
        mock_redis_client,
        mock_fastf1_resource,
        sample_schedule_df,
    ):
        """Test season schedule cache miss scenario"""

        with build_asset_context(partition_key="2024") as context:
            mock_bucket_client.file_exists = MagicMock(return_value=False)
            mock_bucket_client.upload_file = MagicMock(return_value=True)
            mock_bucket_resource = MagicMock()
            mock_bucket_resource.get_client.return_value = mock_bucket_client

            mock_redis_client.exists = MagicMock(return_value=False)
            mock_redis_client.cache_parquet = MagicMock(return_value=True)
            mock_redis_resource = MagicMock()
            mock_redis_resource.get_client.return_value = mock_redis_client

            _ = season_schedule(
                context=context,
                fastf1_resource=mock_fastf1_resource,
                bucket_resource=mock_bucket_resource,
                redis_resource=mock_redis_resource,
            )

            # Check that the redis client exists method is called once
            mock_redis_client.exists.assert_called_once()
            # Check that the redis client get method is not called
            mock_redis_client.client.get.assert_not_called()
            # Check that the dataframe was uploaded to the redis cache
            mock_redis_client.cache_parquet.assert_called_once()
            mock_redis_client.cache_parquet.assert_called_with(
                key="f1:schedule:2024",
                df=sample_schedule_df,
            )

            # Check that the bucket client file exists method is called once
            mock_bucket_client.file_exists.assert_called_once()
            # Check that the bucket client download fileobj method is not called
            mock_bucket_client.s3_client.download_fileobj.assert_not_called()
            # Check that the dataframe was uploaded to the bucket storage
            mock_bucket_client.upload_file.assert_called_once()
            mock_bucket_client.upload_file.assert_called_with(
                bucket_name="test-bucket-raw",
                object_key="schedules/2024/schedule.parquet",
                file_obj=ANY,
            )

            # Check that the corrent DataFrame is returned
            assert context.get_output_metadata("result")["source"] == "fastf1_api"
            assert context.get_output_metadata("result")["num_events"] == 3
            assert (
                context.get_output_metadata("result")["cache_performance"]
                == "CACHE_MISS"
            )
            assert (
                context.get_output_metadata("result")["bucket_backfill_status"] is True
            )
            assert (
                context.get_output_metadata("result")["redis_backfill_status"] is True
            )

    def test_season_schedule_l1_cache_hit(
        self,
        mock_bucket_client,
        mock_redis_client,
        mock_fastf1_resource,
        sample_schedule_df,
    ):
        """Test season schedule l1 cache scenario"""

        with build_asset_context(partition_key="2024") as context:
            mock_bucket_client.file_exists = MagicMock(return_value=False)
            mock_bucket_resource = MagicMock()
            mock_bucket_resource.get_client.return_value = mock_bucket_client

            mock_redis_client.exists = MagicMock(return_value=True)
            mock_redis_client.get_parquet = MagicMock(return_value=sample_schedule_df)
            mock_redis_resource = MagicMock()
            mock_redis_resource.get_client.return_value = mock_redis_client

            _ = season_schedule(
                context=context,
                fastf1_resource=mock_fastf1_resource,
                bucket_resource=mock_bucket_resource,
                redis_resource=mock_redis_resource,
            )

            # Check that the redis client exists method is called once
            mock_redis_client.exists.assert_called_once()
            # Check that the redis client get_parquet method is called
            mock_redis_client.get_parquet.assert_called_with(key="f1:schedule:2024")

            # Check that the bucket client file exists method is called once
            mock_bucket_client.file_exists.assert_not_called()

            assert context.get_output_metadata("result")["source"] == "redis_cache"
            assert context.get_output_metadata("result")["num_events"] == 3
            assert (
                context.get_output_metadata("result")["cache_performance"] == "L1_HIT"
            )

    def test_season_schedule_l2_cache_hit(
        self,
        mock_bucket_client,
        mock_redis_client,
        mock_fastf1_resource,
        sample_schedule_parquet_bytes,
    ):
        """Test season schedule l1 cache scenario"""

        with build_asset_context(partition_key="2024") as context:
            mock_bucket_client.file_exists = MagicMock(return_value=True)
            mock_bucket_client.download_file = MagicMock(
                return_value=sample_schedule_parquet_bytes
            )
            mock_bucket_resource = MagicMock()
            mock_bucket_resource.get_client.return_value = mock_bucket_client

            mock_redis_client.exists = MagicMock(return_value=False)
            mock_redis_client.cache_parquet = MagicMock(return_value=True)
            mock_redis_resource = MagicMock()
            mock_redis_resource.get_client.return_value = mock_redis_client

            _ = season_schedule(
                context=context,
                fastf1_resource=mock_fastf1_resource,
                bucket_resource=mock_bucket_resource,
                redis_resource=mock_redis_resource,
            )

            # Check that the redis client exists method is called once
            mock_redis_client.exists.assert_called_once()
            # Check that the redis client get method is not called
            mock_redis_client.client.get.assert_not_called()
            # Check that the redis client get_parquet method is called
            mock_redis_client.cache_parquet.assert_called_with(
                key="f1:schedule:2024",
                df=ANY,
            )

            # Check that the bucket client file exists method is called once
            mock_bucket_client.file_exists.assert_called_once()
            # Check that the bucket client download fileobj method is not called
            mock_bucket_client.download_file.assert_called_with(
                bucket_name="test-bucket-raw",
                object_key="schedules/2024/schedule.parquet",
            )

            assert context.get_output_metadata("result")["source"] == "bucket_storage"
            assert context.get_output_metadata("result")["num_events"] == 3
            assert (
                context.get_output_metadata("result")["cache_performance"] == "L2_HIT"
            )
            assert (
                context.get_output_metadata("result")["redis_backfill_status"] is True
            )


class TestSessionLaps:
    """Test Session Laps asset"""

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_laps_cache_miss(
        self,
        mock_fastf1,
        mock_bucket_client,
        sample_race_laps_df,
        sample_session_status_df,
        sample_track_status_df,
    ):
        """Test session laps cache miss scenario"""

        with build_asset_context(
            partition_key="2024|Italian Grand Prix|Race"
        ) as context:
            mock_bucket_client.file_exists = MagicMock(return_value=False)
            mock_bucket_client.upload_file = MagicMock(return_value=True)
            mock_bucket_resource = MagicMock()
            mock_bucket_resource.get_client.return_value = mock_bucket_client

            mock_session = MagicMock(spec=Session)
            mock_session.load = MagicMock()
            mock_session.laps = sample_race_laps_df
            mock_session.session_status = sample_session_status_df
            mock_session.track_status = sample_track_status_df
            mock_fastf1.get_session.return_value = mock_session

            fastf1_resource = FastF1Resource.from_env()

            _, _, _ = session_laps(
                context=context,
                fastf1_resource=fastf1_resource,
                bucket_resource=mock_bucket_resource,
            )

            # Check that the bucket client file exists method is called 3 times
            # for laps, session_status and track_status
            assert mock_bucket_client.file_exists.call_count == 3
            # Check that the bucket client download fileobj method is not called
            mock_bucket_client.s3_client.download_fileobj.assert_not_called()
            # Check that the three dataframe was uploaded to the bucket storage
            assert mock_bucket_client.upload_file.call_count == 3

            assert context.get_output_metadata("result")["source"] == "fastf1_api"
            assert context.get_output_metadata("result")["files_downloaded"] == [
                "laps",
                "session_status",
                "track_status",
            ]
            assert (
                context.get_output_metadata("result")["cache_performance"]
                == "CACHE_MISS"
            )
            assert context.get_output_metadata("result")["bucket_backfill_status"] == {
                "laps": True,
                "session_status": True,
                "track_status": True,
            }
            assert (
                context.get_output_metadata("result")["redis_backfill_status"] == "NA"
            )

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_laps_l2_hit(
        self,
        mock_fastf1,
        mock_bucket_client,
        sample_race_laps_parquet_bytes,
        sample_session_status_parquet_bytes,
        sample_track_status_parquet_bytes,
    ):
        """Test session laps L2 cache scenario"""

        with build_asset_context(
            partition_key="2024|Italian Grand Prix|Race"
        ) as context:
            mock_bucket_client.file_exists = MagicMock(return_value=True)
            return_files = [
                sample_race_laps_parquet_bytes,
                sample_session_status_parquet_bytes,
                sample_track_status_parquet_bytes,
            ]
            mock_bucket_client.download_file = MagicMock()
            mock_bucket_client.download_file.side_effect = return_files
            mock_bucket_resource = MagicMock()
            mock_bucket_resource.get_client.return_value = mock_bucket_client

            mock_session = MagicMock(spec=Session)
            mock_session.load = MagicMock()
            mock_fastf1.get_session.return_value = mock_session

            fastf1_resource = FastF1Resource.from_env()

            _, _, _ = session_laps(
                context=context,
                fastf1_resource=fastf1_resource,
                bucket_resource=mock_bucket_resource,
            )

            # Check that the bucket client file exists method is called 3 times
            # for laps, session_status and track_status
            assert mock_bucket_client.file_exists.call_count == 3
            # Check that the bucket client download file method is called 3 times
            assert mock_bucket_client.download_file.call_count == 3

            assert context.get_output_metadata("result")["source"] == "bucket_storage"
            assert context.get_output_metadata("result")["files_downloaded"] == [
                "laps",
                "session_status",
                "track_status",
            ]
            assert (
                context.get_output_metadata("result")["cache_performance"] == "L2_HIT"
            )
            assert (
                context.get_output_metadata("result")["redis_backfill_status"] == "NA"
            )


@pytest.mark.unit
class TestSessionResults:
    """Test Session Results asset"""

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_results_cache_miss(
        self,
        mock_fastf1,
        mock_bucket_client,
        sample_race_results_df,
    ):
        """Test session results cache miss scenario"""

        with build_asset_context(
            partition_key="2024|Italian Grand Prix|Race"
        ) as context:
            mock_bucket_client.file_exists = MagicMock(return_value=False)
            mock_bucket_client.upload_file = MagicMock(return_value=True)
            mock_bucket_resource = MagicMock()
            mock_bucket_resource.get_client.return_value = mock_bucket_client

            mock_session = MagicMock(spec=Session)
            mock_session.load = MagicMock()
            mock_session.results = sample_race_results_df
            mock_fastf1.get_session.return_value = mock_session

            fastf1_resource = FastF1Resource.from_env()

            _ = session_results(
                context=context,
                fastf1_resource=fastf1_resource,
                bucket_resource=mock_bucket_resource,
            )

            # Check that the bucket client file exists method is called 3 times
            # for laps, session_status and track_status
            mock_bucket_client.file_exists.assert_called_once()
            # Check that the bucket client download fileobj method is not called
            mock_bucket_client.s3_client.download_fileobj.assert_not_called()
            # Check that the three dataframe was uploaded to the bucket storage
            mock_bucket_client.upload_file.assert_called_once()

            assert context.get_output_metadata("result")["source"] == "fastf1_api"
            assert context.get_output_metadata("result")["num_results"] == 3
            assert (
                context.get_output_metadata("result")["cache_performance"]
                == "CACHE_MISS"
            )
            assert context.get_output_metadata("result")["first_driver"] == "leclerc"
            assert context.get_output_metadata("result")["last_driver"] == "norris"
            assert (
                context.get_output_metadata("result")["bucket_backfill_status"] is True
            )
            assert (
                context.get_output_metadata("result")["redis_backfill_status"] == "NA"
            )

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_results_l2_hit(
        self,
        mock_fastf1,
        mock_bucket_client,
        sample_race_laps_parquet_bytes,
    ):
        """Test session results L2 cache scenario"""

        with build_asset_context(
            partition_key="2024|Italian Grand Prix|Race"
        ) as context:
            mock_bucket_client.file_exists = MagicMock(return_value=True)
            mock_bucket_client.download_file = MagicMock(
                return_value=sample_race_laps_parquet_bytes
            )
            mock_bucket_resource = MagicMock()
            mock_bucket_resource.get_client.return_value = mock_bucket_client

            mock_session = MagicMock(spec=Session)
            mock_session.load = MagicMock()
            mock_fastf1.get_session.return_value = mock_session

            fastf1_resource = FastF1Resource.from_env()

            _ = session_results(
                context=context,
                fastf1_resource=fastf1_resource,
                bucket_resource=mock_bucket_resource,
            )

            # Check that the bucket client file exists method is called 3 times
            # for laps, session_status and track_status
            mock_bucket_client.file_exists.assert_called_once()
            # Check that the bucket client download file method is called 3 times
            mock_bucket_client.download_file.assert_called_once()
            mock_bucket_client.download_file.assert_called_with(
                bucket_path=BucketPath(
                    bucket="test-bucket-raw",
                    year=2024,
                    grand_prix="Italian Grand Prix",
                    session="Race",
                    filename="results.parquet",
                )
            )

            assert context.get_output_metadata("result")["source"] == "bucket_storage"
            assert context.get_output_metadata("result")["num_results"] == 3
            assert (
                context.get_output_metadata("result")["cache_performance"] == "L2_HIT"
            )
            assert (
                context.get_output_metadata("result")["redis_backfill_status"] == "NA"
            )


@pytest.mark.unit
class TestSessionMessages:
    """Test Session Results asset"""

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_messages_cache_miss(
        self,
        mock_fastf1,
        mock_bucket_client,
        sample_messages_df,
    ):
        """Test session race control messages cache miss scenario"""

        with build_asset_context(
            partition_key="2024|Italian Grand Prix|Race"
        ) as context:
            mock_bucket_client.file_exists = MagicMock(return_value=False)
            mock_bucket_client.upload_file = MagicMock(return_value=True)
            mock_bucket_resource = MagicMock()
            mock_bucket_resource.get_client.return_value = mock_bucket_client

            mock_session = MagicMock(spec=Session)
            mock_session.load = MagicMock()
            mock_session.race_control_messages = sample_messages_df
            mock_fastf1.get_session.return_value = mock_session

            fastf1_resource = FastF1Resource.from_env()

            _ = session_messages(
                context=context,
                fastf1_resource=fastf1_resource,
                bucket_resource=mock_bucket_resource,
            )

            # Check that the bucket client file exists method is called 3 times
            # for laps, session_status and track_status
            mock_bucket_client.file_exists.assert_called_once()
            # Check that the bucket client download fileobj method is not called
            mock_bucket_client.s3_client.download_fileobj.assert_not_called()
            # Check that the three dataframe was uploaded to the bucket storage
            mock_bucket_client.upload_file.assert_called_once()

            assert context.get_output_metadata("result")["source"] == "fastf1_api"
            assert context.get_output_metadata("result")["num_messages"] == 3
            assert (
                context.get_output_metadata("result")["cache_performance"]
                == "CACHE_MISS"
            )
            assert (
                context.get_output_metadata("result")["bucket_backfill_status"] is True
            )
            assert (
                context.get_output_metadata("result")["redis_backfill_status"] == "NA"
            )

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_messages_l2_hit(
        self,
        mock_fastf1,
        mock_bucket_client,
        sample_messages_parquet_bytes,
    ):
        """Test session results L2 cache scenario"""

        with build_asset_context(
            partition_key="2024|Italian Grand Prix|Race"
        ) as context:
            mock_bucket_client.file_exists = MagicMock(return_value=True)
            mock_bucket_client.download_file = MagicMock(
                return_value=sample_messages_parquet_bytes
            )
            mock_bucket_resource = MagicMock()
            mock_bucket_resource.get_client.return_value = mock_bucket_client

            mock_session = MagicMock(spec=Session)
            mock_session.load = MagicMock()
            mock_fastf1.get_session.return_value = mock_session

            fastf1_resource = FastF1Resource.from_env()

            _ = session_messages(
                context=context,
                fastf1_resource=fastf1_resource,
                bucket_resource=mock_bucket_resource,
            )

            # Check that the bucket client file exists method is called 3 times
            # for laps, session_status and track_status
            mock_bucket_client.file_exists.assert_called_once()
            # Check that the bucket client download file method is called 3 times
            mock_bucket_client.download_file.assert_called_once()
            mock_bucket_client.download_file.assert_called_with(
                bucket_path=BucketPath(
                    bucket="test-bucket-raw",
                    year=2024,
                    grand_prix="Italian Grand Prix",
                    session="Race",
                    filename="messages.parquet",
                )
            )

            assert context.get_output_metadata("result")["source"] == "bucket_storage"
            assert context.get_output_metadata("result")["num_messages"] == 3
            assert (
                context.get_output_metadata("result")["cache_performance"] == "L2_HIT"
            )
            assert (
                context.get_output_metadata("result")["redis_backfill_status"] == "NA"
            )


@pytest.mark.unit
class TestSessionWeather:
    """Test Session Weather asset"""

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_weather_cache_miss(
        self,
        mock_fastf1,
        mock_bucket_client,
        sample_weather_df,
    ):
        """Test session weather cache miss scenario"""

        with build_asset_context(
            partition_key="2024|Italian Grand Prix|Race"
        ) as context:
            mock_bucket_client.file_exists = MagicMock(return_value=False)
            mock_bucket_client.upload_file = MagicMock(return_value=True)
            mock_bucket_resource = MagicMock()
            mock_bucket_resource.get_client.return_value = mock_bucket_client

            mock_session = MagicMock(spec=Session)
            mock_session.load = MagicMock()
            mock_session.weather_data = sample_weather_df
            mock_fastf1.get_session.return_value = mock_session

            fastf1_resource = FastF1Resource.from_env()

            _ = session_weather(
                context=context,
                fastf1_resource=fastf1_resource,
                bucket_resource=mock_bucket_resource,
            )

            # Check that the bucket client file exists method is called 3 times
            # for laps, session_status and track_status
            mock_bucket_client.file_exists.assert_called_once()
            # Check that the bucket client download fileobj method is not called
            mock_bucket_client.s3_client.download_fileobj.assert_not_called()
            # Check that the three dataframe was uploaded to the bucket storage
            mock_bucket_client.upload_file.assert_called_once()

            assert context.get_output_metadata("result")["source"] == "fastf1_api"
            assert context.get_output_metadata("result")["num_weather_data"] == 3
            assert (
                context.get_output_metadata("result")["cache_performance"]
                == "CACHE_MISS"
            )
            assert (
                context.get_output_metadata("result")["bucket_backfill_status"] is True
            )
            assert (
                context.get_output_metadata("result")["redis_backfill_status"] == "NA"
            )

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_weather_l2_hit(
        self,
        mock_fastf1,
        mock_bucket_client,
        sample_weather_parquet_bytes,
    ):
        """Test session weather L2 cache scenario"""

        with build_asset_context(
            partition_key="2024|Italian Grand Prix|Race"
        ) as context:
            mock_bucket_client.file_exists = MagicMock(return_value=True)
            mock_bucket_client.download_file = MagicMock(
                return_value=sample_weather_parquet_bytes
            )
            mock_bucket_resource = MagicMock()
            mock_bucket_resource.get_client.return_value = mock_bucket_client

            mock_session = MagicMock(spec=Session)
            mock_session.load = MagicMock()
            mock_fastf1.get_session.return_value = mock_session

            fastf1_resource = FastF1Resource.from_env()

            _ = session_weather(
                context=context,
                fastf1_resource=fastf1_resource,
                bucket_resource=mock_bucket_resource,
            )

            # Check that the bucket client file exists method is called 3 times
            # for laps, session_status and track_status
            mock_bucket_client.file_exists.assert_called_once()
            # Check that the bucket client download file method is called 3 times
            mock_bucket_client.download_file.assert_called_once()
            mock_bucket_client.download_file.assert_called_with(
                bucket_path=BucketPath(
                    bucket="test-bucket-raw",
                    year=2024,
                    grand_prix="Italian Grand Prix",
                    session="Race",
                    filename="weather.parquet",
                )
            )

            assert context.get_output_metadata("result")["source"] == "bucket_storage"
            assert context.get_output_metadata("result")["num_weather_data"] == 3
            assert (
                context.get_output_metadata("result")["cache_performance"] == "L2_HIT"
            )
            assert (
                context.get_output_metadata("result")["redis_backfill_status"] == "NA"
            )
