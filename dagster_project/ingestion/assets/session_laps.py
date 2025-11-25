"""
Session laps data asset for ingestion
"""

import io
from typing import Optional, Tuple

import pandas as pd
from dagster import AssetExecutionContext, asset

from dagster_project.ingestion.ops import parse_partition_key
from dagster_project.ingestion.partitions import F1_SESSIONS_PARTITION
from dagster_project.ingestion.resources import FastF1Resource
from dagster_project.shared.resources import BucketPath, BucketResource
from src.config.logging import get_logger

logger = get_logger("data_ingestion.laps")


@asset(
    description="F1 session laps with tiered caching",
    compute_kind="fastf1",
    partitions_def=F1_SESSIONS_PARTITION,
)
def session_laps(
    context: AssetExecutionContext,
    fastf1_resource: FastF1Resource,
    bucket_resource: BucketResource,
) -> Tuple[
    Optional[pd.DataFrame],
    Optional[pd.DataFrame],
    Optional[pd.DataFrame],
]:
    """
    Gets session laps with tiered caching:
    1. Check bucket storage (persistent)
    2. Fetch from FastF1 API (slow, authoritative)

    Backfills cache layers when data is found in slower tiers.
    """

    # Get partition from execution context
    partition_key = context.partition_key

    # Parse partition key to get the required arguments
    year, grand_prix, session = parse_partition_key(partition_key)

    # Initialize the resource clients
    bucket_client = bucket_resource.get_client()

    # Create bucket_paths
    laps_bucket_path = BucketPath(
        bucket=bucket_client.raw_data_bucket,
        year=year,
        grand_prix=grand_prix,
        session=session,
        filename="laps.parquet",
    )
    session_status_bucket_path = BucketPath(
        bucket=bucket_client.raw_data_bucket,
        year=year,
        grand_prix=grand_prix,
        session=session,
        filename="session_status.parquet",
    )
    track_status_bucket_path = BucketPath(
        bucket=bucket_client.raw_data_bucket,
        year=year,
        grand_prix=grand_prix,
        session=session,
        filename="track_status.parquet",
    )

    # Layer 1: Check bucket storage
    laps_bucket_hit = bucket_client.file_exists(bucket_path=laps_bucket_path)
    session_status_bucket_hit = bucket_client.file_exists(
        bucket_path=session_status_bucket_path
    )
    track_status_bucket_hit = bucket_client.file_exists(
        bucket_path=track_status_bucket_path
    )

    if all((laps_bucket_hit, session_status_bucket_hit, track_status_bucket_hit)):
        bucket_download = bucket_client.batch_download(
            files=[
                (laps_bucket_path, None, None, None),
                (session_status_bucket_path, None, None, None),
                (track_status_bucket_path, None, None, None),
            ]
        )

        laps_df = pd.read_parquet(
            io.BytesIO(bucket_download[laps_bucket_path.filename]["data"])
        )
        session_status_df = pd.read_parquet(
            io.BytesIO(bucket_download[session_status_bucket_path.filename]["data"])
        )
        track_status_df = pd.read_parquet(
            io.BytesIO(bucket_download[track_status_bucket_path.filename]["data"])
        )
        # Add logger message
        logger.info(
            "Laps, session & track status for %d %s %s loaded "
            "from bucket storage successfully",
            year,
            grand_prix,
            session,
        )
        # Add metadata
        context.add_output_metadata(
            {
                "source": "bucket_storage",
                "files_downloaded": ["laps", "session_status", "track_status"],
                "cache_performance": "L2_HIT",
                "redis_backfill_status": "NA",
            }
        )

        return laps_df, session_status_df, track_status_df

    # Layer 2: Fetch from API
    logger.info("Cache miss - fetching from API")
    api_laps_df, api_session_status_df, api_track_status_df = (
        fastf1_resource.get_session_laps(
            year=year,
            grand_prix=grand_prix,
            session=session,
        )
    )

    ## Backfill bucket storage layer
    # Laps
    laps_buffer = io.BytesIO()
    api_laps_df.to_parquet(laps_buffer, index=False)
    laps_buffer.seek(0)
    laps_bucket_upload_status = bucket_client.upload_file(
        bucket_path=laps_bucket_path,
        file_obj=laps_buffer,
    )
    # Session status
    session_status_buffer = io.BytesIO()
    api_session_status_df.to_parquet(session_status_buffer, index=False)
    session_status_buffer.seek(0)
    session_status_bucket_upload_status = bucket_client.upload_file(
        bucket_path=session_status_bucket_path,
        file_obj=session_status_buffer,
    )
    # Track status
    track_status_buffer = io.BytesIO()
    api_track_status_df.to_parquet(track_status_buffer, index=False)
    track_status_buffer.seek(0)
    track_status_bucket_upload_status = bucket_client.upload_file(
        bucket_path=track_status_bucket_path,
        file_obj=track_status_buffer,
    )
    # Add metadata
    context.add_output_metadata(
        {
            "source": "fastf1_api",
            "files_downloaded": ["laps", "session_status", "track_status"],
            "cache_performance": "CACHE_MISS",
            "bucket_backfill_status": {
                "laps": laps_bucket_upload_status,
                "session_status": session_status_bucket_upload_status,
                "track_status": track_status_bucket_upload_status,
            },
            "redis_backfill_status": "NA",
        }
    )
    return api_laps_df, api_session_status_df, api_track_status_df
