"""
Ingestion assets
"""

from dagster_project.ingestion.assets.season_schedule import season_schedule
from dagster_project.ingestion.assets.session_laps import session_laps
from dagster_project.ingestion.assets.session_messages import session_messages
from dagster_project.ingestion.assets.session_results import session_results
from dagster_project.ingestion.assets.session_weather import session_weather

__all__ = [
    "season_schedule",
    "session_laps",
    "session_messages",
    "session_results",
    "session_weather",
]
