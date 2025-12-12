"""
Dagster definitions for transformation module.
"""

from dagster import Definitions

from dagster_project.shared.resources import BucketResource, RedisResource
from dagster_project.transformation.assets import (
    catalog_f1_session_data_assets,
    validated_qualifying_results,
    validated_race_results,
    validated_weather_data,
)

defs = Definitions(
    assets=[
        catalog_f1_session_data_assets,
        validated_qualifying_results,
        validated_race_results,
        validated_weather_data,
    ],
    resources={
        "bucket_resource": BucketResource.from_env(),
        "redis_resource": RedisResource.from_env(),
    },
)
