"""
Dagster jobs for F1 data ingestion
"""

from dagster import AssetSelection, define_asset_job

from dagster_project.ingestion.assets import (
    session_laps,
    session_messages,
    session_results,
    session_weather,
)
from dagster_project.ingestion.partitions import F1_SESSIONS_PARTITION

session_assets = AssetSelection.assets(
    session_laps,
    session_results,
    session_messages,
    session_weather,
)

session_data_job = define_asset_job(  # pylint: disable=assignment-from-no-return
    name="session_data_ingestion_job",
    selection=session_assets,
    partitions_def=F1_SESSIONS_PARTITION,
    description="Ingest F1 session data (laps, results, weather, messages)",
)
