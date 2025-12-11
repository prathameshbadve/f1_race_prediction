"""
Dagster definitions for transformation module.
"""

from dagster import Definitions

from dagster_project.shared.resources import BucketResource, RedisResource
from dagster_project.transformation.assets import catalog_f1_data_assets

defs = Definitions(
    assets=[
        catalog_f1_data_assets,
    ],
    resources={
        "bucket_resource": BucketResource.from_env(),
        "redis_resource": RedisResource.from_env(),
    },
)
