"""
Dagster definitions module.
"""

from dagster import Definitions

from dagster_project.example.assets import fetch_and_store, load_data_to_db
from dagster_project.shared.resources import BucketResource, DatabaseResource
from src.config.logging import setup_logging

# Setup logging configuration
setup_logging()

defs = Definitions(
    assets=[load_data_to_db, fetch_and_store],
    resources={
        "bucket_resource": BucketResource.from_env(),
        "db_resource": DatabaseResource.from_env(),
    },
)
