"""
Ingestion assets
"""

from dagster_project.ingestion.assets.season_schedule import season_schedule
from dagster_project.ingestion.assets.session_results import session_results

__all__ = [
    "season_schedule",
    "session_results",
]
