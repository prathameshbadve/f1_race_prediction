"""
Dagster definitions for ingestion module.
"""

from dagster import Definitions

from dagster_project.ingestion.assets import season_schedule, session_results
from dagster_project.ingestion.resources import FastF1Resource
from dagster_project.ingestion.sensors import (
    update_partitions_on_schedule_materialization,
)
from dagster_project.shared.resources import BucketResource, RedisResource

defs = Definitions(
    assets=[
        season_schedule,
        session_results,
    ],
    resources={
        "bucket_resource": BucketResource.from_env(),
        "redis_resource": RedisResource.from_env(),
        "fastf1_resource": FastF1Resource.from_env(),
    },
    sensors=[
        update_partitions_on_schedule_materialization,
    ],
)
