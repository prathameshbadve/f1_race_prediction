"""
Dagster definitions module.
"""

from dagster import Definitions

from dagster_project.example.assets import example_asset_with_caching
from dagster_project.shared.resources import (
    BucketResource,
    DatabaseResource,
    RedisResource,
)

defs = Definitions(
    assets=[example_asset_with_caching],
    resources={
        "bucket_resource": BucketResource.from_env(),
        "db_resource": DatabaseResource.from_env(),
        "redis_resource": RedisResource.from_env(),
    },
)
