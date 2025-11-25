"""
Complete test asset for BucketResource and DatabaseResource integration.

This script demonstrates a complete data pipeline:
1. Fetch data from FastF1 API (or mock data)
2. Store raw data in MinIO bucket
3. Process the data
4. Store processed data in PostgreSQL database
5. Query and verify the data

Run this script to test both resources working together!
"""

import io
from typing import Optional

import pandas as pd
from dagster import AssetExecutionContext, StaticPartitionsDefinition, asset

from dagster_project.shared.resources import (
    BucketResource,
    CacheDataType,
    RedisResource,
)
from src.config.logging import get_logger

logger = get_logger("data_ingestion.example")

# Example Asset 1

example_partitions = StaticPartitionsDefinition(["2023", "2024"])


@asset(
    description="Example asset",
    compute_kind="api",
    partitions_def=example_partitions,
)
def example_asset_with_caching(
    context: AssetExecutionContext,
    bucket_resource: BucketResource,
    redis_resource: RedisResource,
) -> Optional[pd.DataFrame]:
    """Example asset"""

    year = int(context.partition_key)

    bucket_client = bucket_resource.get_client()
    redis_client = redis_resource.get_client()

    cache_key = f"f1:test:schedule:{year}"
    schedule_key = f"{year}/schedule.parquet"

    # Layer 1: Check Redis
    redis_hit = redis_client.exists(cache_key, CacheDataType.PARQUET)
    if redis_hit:
        redis_data = redis_client.get_parquet(key=cache_key)
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
        schedule_data = bucket_client.download_file(
            bucket_name="test-bucket", object_key=schedule_key
        )
        schedule_df = pd.read_parquet(io.BytesIO(schedule_data))

        logger.info(
            "Schedule for season %d loaded from bucket storage successfully", year
        )

        # Load data to redis cache
        logger.info("Backfilling Redis from bucket data")
        redis_upload_status = redis_client.cache_parquet(
            key=cache_key,
            df=schedule_df,
        )

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
    api_df = pd.DataFrame(
        {
            "RoundNumber": [1, 2, 3],
            "EventName": [
                "Australian Grand Prix",
                "Italian Grand Prix",
                "United States Grand Prix",
            ],
            "Format": [
                "conventional",
                "sprint",
                "conventional",
            ],
            "Year": [year, year, year],
        }
    )

    api_df_buffer = io.BytesIO()
    api_df.to_parquet(api_df_buffer, index=False)
    api_df_buffer.seek(0)

    # Backfill both cache layers
    bucket_upload_status = bucket_client.upload_file(
        bucket_name="test-bucket",
        object_key=schedule_key,
        file_obj=api_df_buffer,
    )
    redis_upload_status = redis_client.cache_parquet(
        key=cache_key,
        df=api_df,
    )

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
