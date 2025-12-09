"""
Ingestion schedules for F1 session data
"""

from dagster import RunRequest, SkipReason, schedule

from dagster_project.ingestion.jobs import session_data_job


@schedule(
    job=session_data_job,
    cron_schedule="0 * * * *",
    name="yearly_batched_session_ingestion",
    description="Ingest session data year by year in batches of ~50 partitions",
)
def yearly_session_ingestion_schedule(context):
    """
    Schedule that processes sessions year by year, ~50 partitions at a time.

    Cursor format: "year:index"
    - year: Current year being processed
    - index: Starting index within that year's partitions

    When a year is complete, automatically moves to the next year.
    When all years are complete, cycles back to the first year.
    """

    BATCH_SIZE = 50  # noqa: N806 # pylint: disable=invalid-name

    instance = context.instance
    all_partitions = list(instance.get_dynamic_partitions("f1_sessions"))

    if not all_partitions:
        return SkipReason(
            "No session partitions available. Materialize season_schedule first."
        )

    # Extract available years and sort them
    available_years = sorted(set(p.split("|")[0] for p in all_partitions))

    if not available_years:
        return SkipReason("Could not determine years from partitions")

    # Parse cursor (default to first year, index 0)
    cursor = context.cursor or f"{available_years[0]}:0"

    try:
        current_year, start_idx = cursor.split(":")
        start_idx = int(start_idx)
    except ValueError:
        current_year = available_years[0]
        start_idx = 0

    # Get partitions for current year
    year_partitions = sorted(
        [p for p in all_partitions if p.startswith(f"{current_year}|")]
    )

    # If current year is exhausted or invalid, move to next year
    if start_idx >= len(year_partitions) or current_year not in available_years:
        # Find next year in sequence
        try:
            current_year_idx = available_years.index(current_year)
            next_year_idx = (current_year_idx + 1) % len(available_years)
        except ValueError:
            next_year_idx = 0

        current_year = available_years[next_year_idx]
        start_idx = 0
        year_partitions = sorted(
            [p for p in all_partitions if p.startswith(f"{current_year}|")]
        )

        if not year_partitions:
            return SkipReason(f"No partitions found for year {current_year}")

    # Calculate batch boundaries
    end_idx = min(start_idx + BATCH_SIZE, len(year_partitions))
    batch_partitions = year_partitions[start_idx:end_idx]

    if not batch_partitions:
        return SkipReason("No partitions to process in this batch")

    # Determine next cursor
    if end_idx >= len(year_partitions):
        # Year complete, move to next year
        try:
            current_year_idx = available_years.index(current_year)
            next_year_idx = (current_year_idx + 1) % len(available_years)
            next_cursor = f"{available_years[next_year_idx]}:0"
        except ValueError:
            next_cursor = f"{available_years[0]}:0"
    else:
        next_cursor = f"{current_year}:{end_idx}"

    # Log progress
    total_year_partitions = len(year_partitions)
    batch_num = (start_idx // BATCH_SIZE) + 1
    total_batches = (total_year_partitions + BATCH_SIZE - 1) // BATCH_SIZE

    context.log.info(
        f"Year {current_year}: Processing batch {batch_num}/{total_batches} "
        f"(partitions {start_idx}-{end_idx - 1} of {total_year_partitions})"
    )

    # Create run requests
    run_requests = [
        RunRequest(
            run_key=f"{current_year}_batch{batch_num}_{partition_key.replace('|', '_')}",  # noqa: E501 # pylint: disable=line-too-long
            partition_key=partition_key,
            tags={
                "year": current_year,
                "batch_number": str(batch_num),
                "total_batches": str(total_batches),
                "batch_size": str(len(batch_partitions)),
            },
        )
        for partition_key in batch_partitions
    ]

    # Update cursor and return
    context.update_cursor(next_cursor)

    return run_requests
