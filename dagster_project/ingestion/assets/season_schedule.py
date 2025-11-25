"""
Season schedule data asset for ingestion
"""

import io
from typing import Optional

import pandas as pd
from dagster import AssetExecutionContext, asset

from dagster_project.ingestion.partitions import F1_SEASON_PARTITION
from dagster_project.ingestion.resources import FastF1Resource
from dagster_project.shared.resources import (
    BucketResource,
    CacheDataType,
    RedisResource,
)
from src.config.logging import get_logger

logger = get_logger("data_ingestion")


@asset(
    description="F1 season schedule with tiered caching",
    compute_kind="fastf1",
    partitions_def=F1_SEASON_PARTITION,
)
def season_schedule(
    context: AssetExecutionContext,
    fastf1_resource: FastF1Resource,
    bucket_resource: BucketResource,
    redis_resource: RedisResource,
) -> Optional[pd.DataFrame]:
    """
    Gets season schedule with tiered caching:
    1. Check Redis cache (fast, ephemeral)
    2. Check bucket storage (persistent)
    3. Fetch from FastF1 API (slow, authoritative)

    Backfills cache layers when data is found in slower tiers.
    """

    # Get the year from the partition key
    year = int(context.partition_key)

    # Initialize the resource clients
    bucket_client = bucket_resource.get_client()
    redis_client = redis_resource.get_client()

    # Setup the cache and bucket object keys
    cache_key = f"f1:schedule:{year}"
    schedule_key = f"schedules/{year}/schedule.parquet"

    # Layer 1: Check Redis
    redis_hit = redis_client.exists(cache_key, CacheDataType.PARQUET)
    if redis_hit:
        # Retrieve data from redis
        redis_data = redis_client.get_parquet(key=cache_key)
        # Add logger message
        logger.info("Schedule for season %d loaded from redis cache successfully", year)
        # Add metadata
        context.add_output_metadata(
            {
                "source": "redis_cache",
                "num_events": len(redis_data),
                "cache_performance": "L1_HIT",
            }
        )
        return redis_data

    # Layer 2: Check Bucket Storage
    bucket_hit = bucket_client.file_exists(
        bucket_name=bucket_client.raw_data_bucket,
        object_key=schedule_key,
    )
    if bucket_hit:
        # Retrieve data from bucket storage
        schedule_data = bucket_client.download_file(
            bucket_name=bucket_client.raw_data_bucket, object_key=schedule_key
        )
        schedule_df = pd.read_parquet(io.BytesIO(schedule_data))
        # Add logger message
        logger.info(
            "Schedule for season %d loaded from bucket storage successfully", year
        )

        # Load data to redis cache
        logger.info("Backfilling Redis from bucket data")
        redis_upload_status = redis_client.cache_parquet(
            key=cache_key,
            df=schedule_df,
        )
        # Add metadata
        context.add_output_metadata(
            {
                "source": "bucket_storage",
                "num_events": len(schedule_df),
                "cache_performance": "L2_HIT",
                "redis_backfill_status": redis_upload_status,
            }
        )
        return schedule_df

    # Layer 3: Fetch from API
    logger.info("Cache miss - fetching from API")
    # Load season schedule from API
    api_df = fastf1_resource.get_season_schedule(year)

    ## Backfill both cache layers
    # Bucket storage
    api_df_buffer = io.BytesIO()
    api_df.to_parquet(api_df_buffer, index=False)
    api_df_buffer.seek(0)
    bucket_upload_status = bucket_client.upload_file(
        bucket_name=bucket_client.raw_data_bucket,
        object_key=schedule_key,
        file_obj=api_df_buffer,
    )
    # Redis cache
    redis_upload_status = redis_client.cache_parquet(
        key=cache_key,
        df=api_df,
    )
    # Add metadata
    context.add_output_metadata(
        {
            "source": "fastf1_api",
            "num_events": len(api_df),
            "cache_performance": "CACHE_MISS",
            "first_event": api_df.iloc[0]["EventName"],
            "last_event": api_df.iloc[-1]["EventName"],
            "bucket_backfill_status": bucket_upload_status,
            "redis_backfill_status": redis_upload_status,
        }
    )
    return api_df
