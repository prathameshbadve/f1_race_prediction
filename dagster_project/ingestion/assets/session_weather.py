"""
Session weather data asset for ingestion
"""

import io
from typing import Optional

import pandas as pd
from dagster import AssetExecutionContext, asset

from dagster_project.ingestion.ops import parse_partition_key
from dagster_project.ingestion.partitions import F1_SESSIONS_PARTITION
from dagster_project.ingestion.resources import FastF1Resource
from dagster_project.shared.resources import BucketPath, BucketResource
from src.config.logging import get_logger

logger = get_logger("data_ingestion.weather")


@asset(
    description="F1 session weather with tiered caching",
    compute_kind="fastf1",
    partitions_def=F1_SESSIONS_PARTITION,
)
def session_weather(
    context: AssetExecutionContext,
    fastf1_resource: FastF1Resource,
    bucket_resource: BucketResource,
) -> Optional[pd.DataFrame]:
    """
    Gets session weather with tiered caching:
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

    # Create BucketPath for storing the results
    weather_bucket_path = BucketPath(
        bucket=bucket_client.raw_data_bucket,
        year=year,
        grand_prix=grand_prix,
        session=session,
        filename="weather.parquet",
    )

    # Layer 1: Check bucket storage
    bucket_hit = bucket_client.file_exists(bucket_path=weather_bucket_path)
    if bucket_hit:
        # Retrieve weather from bucket storage
        weather_data = bucket_client.download_file(bucket_path=weather_bucket_path)
        weather_df = pd.read_parquet(io.BytesIO(weather_data))
        # Add logger message
        logger.info(
            "Weather for %d %s %s loaded from bucket storage successfully",
            year,
            grand_prix,
            session,
        )
        # Add metadata
        context.add_output_metadata(
            {
                "source": "bucket_storage",
                "num_weather_data": len(weather_df),
                "cache_performance": "L2_HIT",
                "redis_backfill_status": "NA",
            }
        )
        return weather_df

    # Layer 2: Fetch from API
    logger.info("Cache miss - fetching from API")
    api_weather_df = fastf1_resource.get_session_weather(
        year=year,
        grand_prix=grand_prix,
        session=session,
    )

    # Backfill bucket storage layer
    weather_buffer = io.BytesIO()
    api_weather_df.to_parquet(weather_buffer, index=False)
    weather_buffer.seek(0)
    bucket_upload_status = bucket_client.upload_file(
        bucket_path=weather_bucket_path,
        file_obj=weather_buffer,
    )
    # Add metadata
    context.add_output_metadata(
        {
            "source": "fastf1_api",
            "num_weather_data": len(api_weather_df),
            "cache_performance": "CACHE_MISS",
            "bucket_backfill_status": bucket_upload_status,
            "redis_backfill_status": "NA",
        }
    )
    return api_weather_df
