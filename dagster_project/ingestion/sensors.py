"""
Asset sensor to dynamically update F1 session partitions when schedules materialize
"""

import io

import pandas as pd
from dagster import (
    AddDynamicPartitionsRequest,
    AssetKey,
    EventLogEntry,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    asset_sensor,
)

from dagster_project.shared.resources import BucketResource


@asset_sensor(
    asset_key=AssetKey("season_schedule"),
    name="f1_sessions_partition_updater",
    description="Updates F1 session partitions when season_schedule is materialized",
    minimum_interval_seconds=30,  # Check shortly after materialization
)
def update_partitions_on_schedule_materialization(
    context: SensorEvaluationContext,
    asset_event: EventLogEntry,
    bucket_resource: BucketResource,
) -> SkipReason | SensorResult:
    """
    Triggered when season_schedule asset is materialized.
    Extracts session information and updates dynamic partitions.

    Creates partition keys in format: "year|grand_prix|session"
    Example: "2024|Bahrain|R", "2024|Monaco|Q"
    """

    # Get the materialization event
    assert asset_event.dagster_event and asset_event.dagster_event.asset_materialization

    bucket_client = bucket_resource.get_client()
    bucket_name = bucket_client.raw_data_bucket

    try:
        # Get all schedule files (in case multiple years were materialized)
        all_objects = bucket_client.list_objects(bucket_name, prefix="schedules/")
        schedule_files = [
            obj for obj in all_objects if obj.endswith("schedule.parquet")
        ]

        if not schedule_files:
            return SkipReason("No schedule files found in bucket")

        # Collect all session partition keys
        all_partition_keys = set()

        for schedule_file in schedule_files:
            context.log.info(f"Processing schedule file: {schedule_file}")

            # Download schedule
            file_content = bucket_client.download_file(
                bucket_path=None,
                bucket_name=bucket_name,
                object_key=schedule_file,
                local_path=None,
            )

            if not file_content:
                context.log.warning(f"Could not download {schedule_file}")
                continue

            # Read parquet from bytes
            schedule_df = pd.read_parquet(io.BytesIO(file_content))

            # Extract sessions from schedule
            for _, row in schedule_df.iterrows():
                year = int(row.get("EventDate").year)
                grand_prix = row["EventName"]
                sessions = [
                    row["Session1"],
                    row["Session2"],
                    row["Session3"],
                    row["Session4"],
                    row["Session5"],
                ]

                # Create partition keys for each session
                for session in sessions:
                    partition_key = f"{year}|{grand_prix}|{session}"
                    all_partition_keys.add(partition_key)

        # Get existing partitions
        existing_partitions = set(
            context.instance.get_dynamic_partitions("f1_sessions")
        )

        # Find new partitions to add
        new_partitions = all_partition_keys - existing_partitions

        if new_partitions:
            context.log.info(
                f"Found {len(new_partitions)} new session partitions to add. "
                f"Total partitions will be: {len(all_partition_keys)}"
            )

            # Log some examples
            example_partitions = list(new_partitions)[:5]
            context.log.info(f"Example new partitions: {example_partitions}")

            return SensorResult(
                dynamic_partitions_requests=[
                    AddDynamicPartitionsRequest(
                        partitions_def_name="f1_sessions",
                        partition_keys=list(new_partitions),
                    )
                ]
            )

        return SkipReason(
            f"All {len(existing_partitions)} partitions already exist. "
            f"No new sessions to add from this materialization."
        )

    except Exception as e:  # pylint: disable=broad-except
        context.log.error(f"Failed to update partitions: {e}", exc_info=True)
        return SkipReason(f"Error processing schedule: {str(e)}")
