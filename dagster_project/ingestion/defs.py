"""
Dagster definitions for ingestion module.
"""

from dagster import Definitions

from dagster_project.shared.resources import BucketResource, RedisResource

defs = Definitions(
    assets=[],
    resources={
        "bucket_resource": BucketResource.from_env(),
        "redis_resource": RedisResource.from_env(),
    },
)
