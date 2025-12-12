"""
Utility functions for ETL activities
"""

from typing import List

from dagster_project.shared.resources import BucketClient
from src.config.logging import get_logger

logger = get_logger("etl")


def scan_year(year: int, bucket_client: BucketClient) -> List[str]:
    """
    Scans the bucket storage with the year prefix and
    returns all grands prix of the year
    """

    logger.info("| | Scanning year %d to get all available grands prix", year)

    prefix = f"{year}"
    try:
        objects = bucket_client.list_objects(
            bucket=bucket_client.raw_data_bucket,
            prefix=prefix,
        )

        grands_prix = []
        for object_key in objects:
            key_parts = object_key.split("/")
            grand_prix = key_parts[1] if len(key_parts) == 4 else key_parts[2]
            if grand_prix not in grands_prix:
                grands_prix.append(grand_prix)

        return grands_prix

    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            "Error trying to get the grands_prix for year %d: %s", year, str(e)
        )
        raise


def scan_grand_prix(
    year: int, grand_prix: str, bucket_client: BucketClient
) -> List[str]:
    """
    Scans a particular grand prix and returns the list of all sessions
    """

    logger.info(
        "| | Scanning grand prix %d %s to get all available sessions", year, grand_prix
    )

    prefix = f"{year}/{grand_prix}"
    try:
        objects = bucket_client.list_objects(
            bucket=bucket_client.raw_data_bucket,
            prefix=prefix,
        )

        sessions = []
        for object_key in objects:
            key_parts = object_key.split("/")
            session = key_parts[2] if len(key_parts) == 4 else key_parts[3]
            if session not in sessions:
                sessions.append(session)

        return sessions

    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            "Error trying to get the sessions for grand prix %d %s: %s",
            year,
            grand_prix,
            str(e),
        )
        raise


def scan_session(
    year: int, grand_prix: str, session: str, bucket_client: BucketClient
) -> List[str]:
    """
    Scan a particular session of a grand prix and return all available filenames
    """

    logger.info(
        "| | Scanning grand prix session %d %s %s to get all available files",
        year,
        grand_prix,
        session,
    )

    prefix = f"{year}/{grand_prix}/{session}"
    try:
        objects = bucket_client.list_objects(
            bucket=bucket_client.raw_data_bucket,
            prefix=prefix,
        )

        filenames = []
        for object_key in objects:
            key_parts = object_key.split("/")
            filename = key_parts[3] if len(key_parts) == 4 else key_parts[4]
            if filename not in filenames:
                filenames.append(filename)

        return filenames

    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            "Error trying to get the filenames for session %d %s %s: %s",
            year,
            grand_prix,
            session,
            str(e),
        )
        raise
