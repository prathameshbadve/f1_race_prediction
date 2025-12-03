"""
Integration tests for dagster assets
"""

import io
from unittest.mock import MagicMock, patch

import pandas as pd
import pandas.testing as pdt
import pytest
from dagster import DagsterInstance, materialize
from fastf1.core import Session

from dagster_project.ingestion.assets import (
    season_schedule,
    session_laps,
    session_messages,
    session_results,
    session_weather,
)
from dagster_project.ingestion.resources import FastF1Resource
from dagster_project.shared.resources import BucketPath, CacheDataType


@pytest.mark.integration
class TestSeasonSchedule:
    """Test season schedule asset"""

    def test_season_schedule_cache_miss(
        self,
        bucket_client,
        redis_client,
        mock_fastf1_resource,
        sample_schedule_df,
    ):
        """Test that season schedule is downloaded from API and backfilled in cache"""

        mock_bucket_resource = MagicMock()
        mock_bucket_resource.get_client.return_value = bucket_client
        mock_redis_resource = MagicMock()
        mock_redis_resource.get_client.return_value = redis_client

        result = materialize(
            [season_schedule],
            partition_key="2024",
            resources={
                "fastf1_resource": mock_fastf1_resource,
                "bucket_resource": mock_bucket_resource,
                "redis_resource": mock_redis_resource,
            },
        )

        bucket_schedule_data = bucket_client.download_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="schedules/2024/schedule.parquet",
        )
        bucket_schedule_df = pd.read_parquet(io.BytesIO(bucket_schedule_data))

        redis_schedule_df = redis_client.get_parquet(key="f1:schedule:2024")

        # Access materialization events for metadata
        materialization = result.get_asset_materialization_events()[0]
        metadata = materialization.materialization.metadata

        # Check that the corrent DataFrame is returned
        assert metadata["source"].value == "fastf1_api"
        assert metadata["num_events"].value == 3
        assert metadata["cache_performance"].value == "CACHE_MISS"
        assert metadata["bucket_backfill_status"].value is True
        assert metadata["redis_backfill_status"].value is True

        # Check file exists in bucket
        assert (
            bucket_client.file_exists(
                bucket_name=bucket_client.raw_data_bucket,
                object_key="schedules/2024/schedule.parquet",
            )
            is True
        )

        # Check file exists in redis
        assert (
            redis_client.exists(key="f1:schedule:2024", data_type=CacheDataType.PARQUET)
            is True
        )

        pdt.assert_frame_equal(
            sample_schedule_df.reset_index(drop=True),
            bucket_schedule_df.reset_index(drop=True),
            check_dtype=False,
        )

        pdt.assert_frame_equal(
            sample_schedule_df.reset_index(drop=True),
            redis_schedule_df.reset_index(drop=True),
            check_dtype=False,
        )

        pdt.assert_frame_equal(
            bucket_schedule_df.reset_index(drop=True),
            redis_schedule_df.reset_index(drop=True),
        )

        # Cleanup
        redis_client.clear_all()

        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )

    def test_season_schedule_l1_cache_hit(
        self,
        bucket_client,
        redis_client,
        mock_fastf1_resource,
        sample_schedule_df,
    ):
        """Test season schedule l1 cache scenario"""

        _ = redis_client.cache_parquet(
            key="f1:schedule:2024",
            df=sample_schedule_df,
        )

        mock_bucket_resource = MagicMock()
        mock_bucket_resource.get_client.return_value = bucket_client
        mock_redis_resource = MagicMock()
        mock_redis_resource.get_client.return_value = redis_client

        result = materialize(
            [season_schedule],
            partition_key="2024",
            resources={
                "fastf1_resource": mock_fastf1_resource,
                "bucket_resource": mock_bucket_resource,
                "redis_resource": mock_redis_resource,
            },
        )

        # Access materialization events for metadata
        materialization = result.get_asset_materialization_events()[0]
        metadata = materialization.materialization.metadata

        # In this test, the file does not exist in the bucket storage,
        # but in production the file should already be in the bucket storage
        bucket_df_exists = bucket_client.file_exists(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="schedules/2024/schedule.parquet",
        )

        assert metadata["source"].value == "redis_cache"
        assert metadata["num_events"].value == 3
        assert metadata["cache_performance"].value == "L1_HIT"
        assert bucket_df_exists is False

        pdt.assert_frame_equal(
            sample_schedule_df.reset_index(drop=True),
            result.output_for_node("season_schedule").reset_index(drop=True),
            check_dtype=False,
        )

        # Cleanup
        redis_client.clear_all()

    def test_season_schedule_l2_cache_hit(
        self,
        bucket_client,
        redis_client,
        mock_fastf1_resource,
        sample_schedule_df,
    ):
        """Test season schedule l2 cache scenario"""

        buffer = io.BytesIO()
        sample_schedule_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        _ = bucket_client.upload_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="schedules/2024/schedule.parquet",
            file_obj=buffer,
        )

        mock_bucket_resource = MagicMock()
        mock_bucket_resource.get_client.return_value = bucket_client
        mock_redis_resource = MagicMock()
        mock_redis_resource.get_client.return_value = redis_client

        result = materialize(
            [season_schedule],
            partition_key="2024",
            resources={
                "fastf1_resource": mock_fastf1_resource,
                "bucket_resource": mock_bucket_resource,
                "redis_resource": mock_redis_resource,
            },
        )

        # Access materialization events for metadata
        materialization = result.get_asset_materialization_events()[0]
        metadata = materialization.materialization.metadata

        redis_schedule_df = redis_client.get_parquet(key="f1:schedule:2024")

        assert metadata["source"].value == "bucket_storage"
        assert metadata["cache_performance"].value == "L2_HIT"
        assert metadata["redis_backfill_status"].value is True

        pdt.assert_frame_equal(
            sample_schedule_df.reset_index(drop=True),
            result.output_for_node("season_schedule").reset_index(drop=True),
            check_dtype=False,
        )

        pdt.assert_frame_equal(
            sample_schedule_df.reset_index(drop=True),
            redis_schedule_df.reset_index(drop=True),
            check_dtype=False,
        )

        # Cleanup
        redis_client.clear_all()

        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )


@pytest.mark.integration
class TestSessionLaps:
    """Test session laps integration"""

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_laps_cache_miss(
        self,
        mock_fastf1,
        bucket_client,
        sample_race_laps_df,
        sample_session_status_df,
        sample_track_status_df,
    ):
        """Test session laps cache miss scenario"""

        mock_bucket_resource = MagicMock()
        mock_bucket_resource.get_client.return_value = bucket_client

        mock_session = MagicMock(spec=Session)
        mock_session.load = MagicMock()
        mock_session.laps = sample_race_laps_df
        mock_session.session_status = sample_session_status_df
        mock_session.track_status = sample_track_status_df
        mock_fastf1.get_session.return_value = mock_session

        fastf1_resource = FastF1Resource.from_env()

        partition_key = "2024|Italian Grand Prix|Race"

        with DagsterInstance.ephemeral() as instance:
            # Register the dynamic partition
            instance.add_dynamic_partitions(
                partitions_def_name="f1_sessions",
                partition_keys=[partition_key],
            )

            result = materialize(
                [session_laps],
                partition_key=partition_key,
                instance=instance,
                resources={
                    "fastf1_resource": fastf1_resource,
                    "bucket_resource": mock_bucket_resource,
                },
            )

        laps_bucket_path = BucketPath(
            bucket=bucket_client.raw_data_bucket,
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
            filename="laps.parquet",
        )
        session_status_bucket_path = BucketPath(
            bucket=bucket_client.raw_data_bucket,
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
            filename="session_status.parquet",
        )
        track_status_bucket_path = BucketPath(
            bucket=bucket_client.raw_data_bucket,
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
            filename="track_status.parquet",
        )

        bucket_laps_data = bucket_client.download_file(bucket_path=laps_bucket_path)
        bucket_laps_df = pd.read_parquet(io.BytesIO(bucket_laps_data))

        bucket_session_status_data = bucket_client.download_file(
            bucket_path=session_status_bucket_path
        )
        bucket_session_status_df = pd.read_parquet(
            io.BytesIO(bucket_session_status_data)
        )

        bucket_track_status_data = bucket_client.download_file(
            bucket_path=track_status_bucket_path
        )
        bucket_track_status_df = pd.read_parquet(io.BytesIO(bucket_track_status_data))

        laps_df, session_status_df, track_status_df = result.output_for_node(
            "session_laps"
        )

        # Access materialization events for metadata
        materialization = result.get_asset_materialization_events()[0]
        metadata = materialization.materialization.metadata

        assert metadata["source"].value == "fastf1_api"
        assert metadata["files_downloaded"].value == [
            "laps",
            "session_status",
            "track_status",
        ]
        assert metadata["cache_performance"].value == "CACHE_MISS"
        assert metadata["bucket_backfill_status"].value == {
            "laps": True,
            "session_status": True,
            "track_status": True,
        }
        assert metadata["redis_backfill_status"].value == "NA"

        pdt.assert_frame_equal(
            laps_df.reset_index(drop=True),
            bucket_laps_df.reset_index(drop=True),
            check_dtype=False,
        )
        pdt.assert_frame_equal(
            session_status_df.reset_index(drop=True),
            bucket_session_status_df.reset_index(drop=True),
            check_dtype=False,
        )
        pdt.assert_frame_equal(
            track_status_df.reset_index(drop=True),
            bucket_track_status_df.reset_index(drop=True),
            check_dtype=False,
        )

        # Cleanup
        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_laps_l2_cache_hit(
        self,
        mock_fastf1,
        bucket_client,
        sample_race_laps_df,
        sample_session_status_df,
        sample_track_status_df,
    ):
        """Test session laps L2 cache hit scenario"""

        laps_bucket_path = BucketPath(
            bucket=bucket_client.raw_data_bucket,
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
            filename="laps.parquet",
        )
        session_status_bucket_path = BucketPath(
            bucket=bucket_client.raw_data_bucket,
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
            filename="session_status.parquet",
        )
        track_status_bucket_path = BucketPath(
            bucket=bucket_client.raw_data_bucket,
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
            filename="track_status.parquet",
        )

        laps_buffer = io.BytesIO()
        sample_race_laps_df.to_parquet(laps_buffer, index=False)
        laps_buffer.seek(0)

        session_status_buffer = io.BytesIO()
        sample_session_status_df.to_parquet(session_status_buffer, index=False)
        session_status_buffer.seek(0)

        track_status_buffer = io.BytesIO()
        sample_track_status_df.to_parquet(track_status_buffer, index=False)
        track_status_buffer.seek(0)

        _ = bucket_client.batch_upload(
            files=[
                (laps_bucket_path, None, None, None, laps_buffer),
                (session_status_bucket_path, None, None, None, session_status_buffer),
                (track_status_bucket_path, None, None, None, track_status_buffer),
            ]
        )

        mock_bucket_resource = MagicMock()
        mock_bucket_resource.get_client.return_value = bucket_client

        mock_session = MagicMock(spec=Session)
        mock_session.load = MagicMock()
        mock_fastf1.get_session.return_value = mock_session

        fastf1_resource = FastF1Resource.from_env()

        partition_key = "2024|Italian Grand Prix|Race"

        with DagsterInstance.ephemeral() as instance:
            # Register the dynamic partition
            instance.add_dynamic_partitions(
                partitions_def_name="f1_sessions",
                partition_keys=[partition_key],
            )

            result = materialize(
                [session_laps],
                partition_key=partition_key,
                instance=instance,
                resources={
                    "fastf1_resource": fastf1_resource,
                    "bucket_resource": mock_bucket_resource,
                },
            )

        laps_df, session_status_df, track_status_df = result.output_for_node(
            "session_laps"
        )

        # Access materialization events for metadata
        materialization = result.get_asset_materialization_events()[0]
        metadata = materialization.materialization.metadata

        assert metadata["source"].value == "bucket_storage"
        assert metadata["files_downloaded"].value == [
            "laps",
            "session_status",
            "track_status",
        ]
        assert metadata["cache_performance"].value == "L2_HIT"
        assert metadata["redis_backfill_status"].value == "NA"

        pdt.assert_frame_equal(
            laps_df.reset_index(drop=True),
            sample_race_laps_df.reset_index(drop=True),
            check_dtype=False,
        )
        pdt.assert_frame_equal(
            session_status_df.reset_index(drop=True),
            sample_session_status_df.reset_index(drop=True),
            check_dtype=False,
        )
        pdt.assert_frame_equal(
            track_status_df.reset_index(drop=True),
            sample_track_status_df.reset_index(drop=True),
            check_dtype=False,
        )

        # Cleanup
        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )


@pytest.mark.integration
class TestSessionResults:
    """Test session results asset"""

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_results_cache_miss(
        self,
        mock_fastf1,
        bucket_client,
        sample_race_results_df,
    ):
        """Test session results cache miss scenario"""

        mock_bucket_resource = MagicMock()
        mock_bucket_resource.get_client.return_value = bucket_client

        mock_session = MagicMock(spec=Session)
        mock_session.load = MagicMock()
        mock_session.results = sample_race_results_df
        mock_fastf1.get_session.return_value = mock_session

        fastf1_resource = FastF1Resource.from_env()

        partition_key = "2024|Italian Grand Prix|Race"

        with DagsterInstance.ephemeral() as instance:
            # Register the dynamic partition
            instance.add_dynamic_partitions(
                partitions_def_name="f1_sessions",
                partition_keys=[partition_key],
            )

            result = materialize(
                [session_results],
                partition_key=partition_key,
                instance=instance,
                resources={
                    "fastf1_resource": fastf1_resource,
                    "bucket_resource": mock_bucket_resource,
                },
            )

        results_bucket_path = BucketPath(
            bucket=bucket_client.raw_data_bucket,
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
            filename="results.parquet",
        )
        bucket_results_data = bucket_client.download_file(
            bucket_path=results_bucket_path
        )
        bucket_results_df = pd.read_parquet(io.BytesIO(bucket_results_data))

        results_df = result.output_for_node("session_results")

        # Access materialization events for metadata
        materialization = result.get_asset_materialization_events()[0]
        metadata = materialization.materialization.metadata

        assert metadata["source"].value == "fastf1_api"
        assert metadata["num_results"].value == 3
        assert metadata["cache_performance"].value == "CACHE_MISS"
        assert metadata["first_driver"].value == "leclerc"
        assert metadata["last_driver"].value == "norris"
        assert metadata["bucket_backfill_status"].value is True
        assert metadata["redis_backfill_status"].value == "NA"

        pdt.assert_frame_equal(
            bucket_results_df.reset_index(drop=True),
            results_df.reset_index(drop=True),
            check_dtype=False,
        )

        # Cleanup
        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_results_l2_hit(
        self,
        mock_fastf1,
        bucket_client,
        sample_race_results_df,
    ):
        """Test session results L2 cache hit scenario"""

        results_bucket_path = BucketPath(
            bucket=bucket_client.raw_data_bucket,
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
            filename="results.parquet",
        )

        buffer = io.BytesIO()
        sample_race_results_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        _ = bucket_client.upload_file(
            bucket_path=results_bucket_path,
            file_obj=buffer,
        )

        mock_bucket_resource = MagicMock()
        mock_bucket_resource.get_client.return_value = bucket_client

        mock_session = MagicMock(spec=Session)
        mock_session.load = MagicMock()
        mock_fastf1.get_session.return_value = mock_session

        fastf1_resource = FastF1Resource.from_env()

        partition_key = "2024|Italian Grand Prix|Race"

        with DagsterInstance.ephemeral() as instance:
            # Register the dynamic partition
            instance.add_dynamic_partitions(
                partitions_def_name="f1_sessions",
                partition_keys=[partition_key],
            )

            result = materialize(
                [session_results],
                partition_key=partition_key,
                instance=instance,
                resources={
                    "fastf1_resource": fastf1_resource,
                    "bucket_resource": mock_bucket_resource,
                },
            )

        results_df = result.output_for_node("session_results")

        # Access materialization events for metadata
        materialization = result.get_asset_materialization_events()[0]
        metadata = materialization.materialization.metadata

        assert metadata["source"].value == "bucket_storage"
        assert metadata["num_results"].value == 3
        assert metadata["cache_performance"].value == "L2_HIT"
        assert metadata["redis_backfill_status"].value == "NA"

        pdt.assert_frame_equal(
            sample_race_results_df.reset_index(drop=True),
            results_df.reset_index(drop=True),
            check_dtype=False,
        )

        # Cleanup
        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )


@pytest.mark.integration
class TestSessionMessages:
    """Test session race control messages asset"""

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_messages_cache_miss(
        self,
        mock_fastf1,
        bucket_client,
        sample_messages_df,
    ):
        """Test session messages cache miss scenario"""

        mock_bucket_resource = MagicMock()
        mock_bucket_resource.get_client.return_value = bucket_client

        mock_session = MagicMock(spec=Session)
        mock_session.load = MagicMock()
        mock_session.race_control_messages = sample_messages_df
        mock_fastf1.get_session.return_value = mock_session

        fastf1_resource = FastF1Resource.from_env()

        partition_key = "2024|Italian Grand Prix|Race"

        with DagsterInstance.ephemeral() as instance:
            # Register the dynamic partition
            instance.add_dynamic_partitions(
                partitions_def_name="f1_sessions",
                partition_keys=[partition_key],
            )

            result = materialize(
                [session_messages],
                partition_key=partition_key,
                instance=instance,
                resources={
                    "fastf1_resource": fastf1_resource,
                    "bucket_resource": mock_bucket_resource,
                },
            )

        messages_bucket_path = BucketPath(
            bucket=bucket_client.raw_data_bucket,
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
            filename="messages.parquet",
        )
        bucket_messages_data = bucket_client.download_file(
            bucket_path=messages_bucket_path
        )
        bucket_messages_df = pd.read_parquet(io.BytesIO(bucket_messages_data))

        results_df = result.output_for_node("session_messages")

        # Access materialization events for metadata
        materialization = result.get_asset_materialization_events()[0]
        metadata = materialization.materialization.metadata

        assert metadata["source"].value == "fastf1_api"
        assert metadata["num_messages"].value == 3
        assert metadata["cache_performance"].value == "CACHE_MISS"
        assert metadata["bucket_backfill_status"].value is True
        assert metadata["redis_backfill_status"].value == "NA"

        pdt.assert_frame_equal(
            bucket_messages_df.reset_index(drop=True),
            results_df.reset_index(drop=True),
            check_dtype=False,
        )

        # Cleanup
        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_messages_l2_hit(
        self,
        mock_fastf1,
        bucket_client,
        sample_messages_df,
    ):
        """Test session messages L2 cache hit scenario"""

        messages_bucket_path = BucketPath(
            bucket=bucket_client.raw_data_bucket,
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
            filename="messages.parquet",
        )

        buffer = io.BytesIO()
        sample_messages_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        _ = bucket_client.upload_file(
            bucket_path=messages_bucket_path,
            file_obj=buffer,
        )

        mock_bucket_resource = MagicMock()
        mock_bucket_resource.get_client.return_value = bucket_client

        mock_session = MagicMock(spec=Session)
        mock_session.load = MagicMock()
        mock_fastf1.get_session.return_value = mock_session

        fastf1_resource = FastF1Resource.from_env()

        partition_key = "2024|Italian Grand Prix|Race"

        with DagsterInstance.ephemeral() as instance:
            # Register the dynamic partition
            instance.add_dynamic_partitions(
                partitions_def_name="f1_sessions",
                partition_keys=[partition_key],
            )

            result = materialize(
                [session_messages],
                partition_key=partition_key,
                instance=instance,
                resources={
                    "fastf1_resource": fastf1_resource,
                    "bucket_resource": mock_bucket_resource,
                },
            )

        results_df = result.output_for_node("session_messages")

        # Access materialization events for metadata
        materialization = result.get_asset_materialization_events()[0]
        metadata = materialization.materialization.metadata

        assert metadata["source"].value == "bucket_storage"
        assert metadata["num_messages"].value == 3
        assert metadata["cache_performance"].value == "L2_HIT"
        assert metadata["redis_backfill_status"].value == "NA"

        pdt.assert_frame_equal(
            sample_messages_df.reset_index(drop=True),
            results_df.reset_index(drop=True),
            check_dtype=False,
        )

        # Cleanup
        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )


@pytest.mark.integration
class TestSessionWeather:
    """Test session weather asset"""

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_weather_cache_miss(
        self,
        mock_fastf1,
        bucket_client,
        sample_weather_df,
    ):
        """Test session weather cache miss scenario"""

        mock_bucket_resource = MagicMock()
        mock_bucket_resource.get_client.return_value = bucket_client

        mock_session = MagicMock(spec=Session)
        mock_session.load = MagicMock()
        mock_session.weather_data = sample_weather_df
        mock_fastf1.get_session.return_value = mock_session

        fastf1_resource = FastF1Resource.from_env()

        partition_key = "2024|Italian Grand Prix|Race"

        with DagsterInstance.ephemeral() as instance:
            # Register the dynamic partition
            instance.add_dynamic_partitions(
                partitions_def_name="f1_sessions",
                partition_keys=[partition_key],
            )

            result = materialize(
                [session_weather],
                partition_key=partition_key,
                instance=instance,
                resources={
                    "fastf1_resource": fastf1_resource,
                    "bucket_resource": mock_bucket_resource,
                },
            )

        weather_bucket_path = BucketPath(
            bucket=bucket_client.raw_data_bucket,
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
            filename="weather.parquet",
        )
        bucket_weather_data = bucket_client.download_file(
            bucket_path=weather_bucket_path
        )
        bucket_weather_df = pd.read_parquet(io.BytesIO(bucket_weather_data))

        results_df = result.output_for_node("session_weather")

        # Access materialization events for metadata
        materialization = result.get_asset_materialization_events()[0]
        metadata = materialization.materialization.metadata

        assert metadata["source"].value == "fastf1_api"
        assert metadata["num_weather_data"].value == 3
        assert metadata["cache_performance"].value == "CACHE_MISS"
        assert metadata["bucket_backfill_status"].value is True
        assert metadata["redis_backfill_status"].value == "NA"

        pdt.assert_frame_equal(
            bucket_weather_df.reset_index(drop=True),
            results_df.reset_index(drop=True),
            check_dtype=False,
        )

        # Cleanup
        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )

    @patch("dagster_project.ingestion.resources.fastf1")
    def test_session_weather_l2_hit(
        self,
        mock_fastf1,
        bucket_client,
        sample_weather_df,
    ):
        """Test session weather L2 cache hit scenario"""

        weather_bucket_path = BucketPath(
            bucket=bucket_client.raw_data_bucket,
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
            filename="weather.parquet",
        )

        buffer = io.BytesIO()
        sample_weather_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        _ = bucket_client.upload_file(
            bucket_path=weather_bucket_path,
            file_obj=buffer,
        )

        mock_bucket_resource = MagicMock()
        mock_bucket_resource.get_client.return_value = bucket_client

        mock_session = MagicMock(spec=Session)
        mock_session.load = MagicMock()
        mock_fastf1.get_session.return_value = mock_session

        fastf1_resource = FastF1Resource.from_env()

        partition_key = "2024|Italian Grand Prix|Race"

        with DagsterInstance.ephemeral() as instance:
            # Register the dynamic partition
            instance.add_dynamic_partitions(
                partitions_def_name="f1_sessions",
                partition_keys=[partition_key],
            )

            result = materialize(
                [session_weather],
                partition_key=partition_key,
                instance=instance,
                resources={
                    "fastf1_resource": fastf1_resource,
                    "bucket_resource": mock_bucket_resource,
                },
            )

        results_df = result.output_for_node("session_weather")

        # Access materialization events for metadata
        materialization = result.get_asset_materialization_events()[0]
        metadata = materialization.materialization.metadata

        assert metadata["source"].value == "bucket_storage"
        assert metadata["num_weather_data"].value == 3
        assert metadata["cache_performance"].value == "L2_HIT"
        assert metadata["redis_backfill_status"].value == "NA"

        pdt.assert_frame_equal(
            sample_weather_df.reset_index(drop=True),
            results_df.reset_index(drop=True),
            check_dtype=False,
        )

        # Cleanup
        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )
