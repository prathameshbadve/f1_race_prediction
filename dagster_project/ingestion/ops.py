"""
Ops required for data ingestion
"""

from typing import Tuple

from dagster import op


@op(
    name="parse_f1_session_partition_key",
    description="Parses the partition key to give the year, grand prix and session.",
)
def parse_partition_key(partition_key: str) -> Tuple[int, str, str]:
    """
    Parse F1 session partition key

    Args:
        - partition_key: e.g. 2024|Italian Grand Prix|Race

    Returns:
        - year: 2024
        - grand_prix: Italian Grand Prix
        - session: Race
    """
    parts = partition_key.split("|")
    if len(parts) != 3:
        raise ValueError(f"Invalid partition key format: {partition_key}")

    return int(parts[0]), parts[1], parts[2]
