"""
Cataloging the downloaded data
"""

from dagster import AssetExecutionContext, asset

from dagster_project.ingestion.partitions import F1_SEASON_PARTITION
from dagster_project.shared.resources import BucketResource
from src.config.logging import get_logger
from src.validation.data_catalog import RaceDataCatalogBuilder

logger = get_logger("transformation")


@asset(
    partitions_def=F1_SEASON_PARTITION,
    compute_kind="python",
    description="Catalog the raw data downloaded from API",
)
def catalog_f1_data_assets(
    context: AssetExecutionContext,
    bucket_resource: BucketResource,
):
    """Create a catalog of the ingested data assets"""

    # Get the year from the partition key
    year = int(context.partition_key)

    # Initialize the resource clients
    bucket_client = bucket_resource.get_client()

    catalog_builder = RaceDataCatalogBuilder(
        bucket_client=bucket_client,
        logger=logger,
    )

    catalog_df = catalog_builder.update_data_catalog_with_year(year)

    return catalog_df
