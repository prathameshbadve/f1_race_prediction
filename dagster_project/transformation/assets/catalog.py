"""
Cataloging the downloaded data
"""

from io import BytesIO

import pandas as pd
from dagster import AssetExecutionContext, asset

from dagster_project.ingestion.partitions import F1_SEASON_PARTITION
from dagster_project.shared.resources import BucketPath, BucketResource
from src.config.logging import get_logger
from src.data.catalog_builder import SessionCatalogEntry, SessionFiles
from src.data.utils import scan_grand_prix, scan_session, scan_year

logger = get_logger("transformation")


@asset(
    name="data_cataloger",
    key_prefix="transformation",
    partitions_def=F1_SEASON_PARTITION,
    compute_kind="python",
    description="Catalog the raw data downloaded from API",
)
def catalog_f1_session_data_assets(
    context: AssetExecutionContext,
    bucket_resource: BucketResource,
):
    """Create a catalog of the ingested data assets for every session"""

    # Initialize the collection of SessionCatalogEntries
    # and metadata for asset materialization
    entries = []
    metadata = {
        "expected_grands_prix": 0,
        "total_grands_prix": 0,
        "total_sessions": 0,
        "sessions_with_all_critical_files": 0,
        "sessions_without_all_critical_files": 0,
        "catalog_upload_status": None,
    }

    # Initialize the resource clients
    bucket_client = bucket_resource.get_client()

    # Get the year from the context
    year = int(context.partition_key)

    # Get the schedule of the year
    schedule_data = bucket_client.download_file(
        bucket_name=bucket_client.raw_data_bucket,
        object_key=f"schedules/{year}/schedule.parquet",
    )
    schedule_df = pd.read_parquet(BytesIO(schedule_data))
    metadata["expected_grands_prix"] = len(schedule_df)

    # Find the available grands prix for the year and iterate over them
    available_grands_prix = scan_year(year=year, bucket_client=bucket_client)
    metadata["total_grands_prix"] = len(available_grands_prix)
    for grand_prix in available_grands_prix:
        # Find the available sessions for the grand prix and iterate over them
        available_sessions = scan_grand_prix(
            year=year, grand_prix=grand_prix, bucket_client=bucket_client
        )
        metadata["total_sessions"] += len(available_sessions)
        for session in available_sessions:
            # Initialize a session files object
            session_files = SessionFiles()

            # File all available filenames and iterate over them
            filenames = scan_session(
                year=year,
                grand_prix=grand_prix,
                session=session,
                bucket_client=bucket_client,
            )
            for filename in filenames:
                setattr(session_files, filename.split(".")[0], True)

            # Now collect the data required from the files
            results_bucket_path = BucketPath(
                bucket=bucket_client.raw_data_bucket,
                year=year,
                grand_prix=grand_prix,
                session=session,
                filename="results.parquet",
            )
            results_data = bucket_client.download_file(bucket_path=results_bucket_path)
            results_df = pd.read_parquet(BytesIO(results_data))

            total_drivers = len(results_df)
            if session not in ["Race", "Sprint"]:
                total_laps = 0
            else:
                total_laps = int(
                    results_df[results_df["ClassifiedPosition"] == "1"]["Laps"].iloc[0]
                )

            grand_prix_schedule_entry = schedule_df[
                schedule_df["EventName"] == grand_prix
            ]
            # Round number for the catalog entry
            round_number = int(grand_prix_schedule_entry["RoundNumber"].iloc[0])
            # Event format for the catalog entry
            event_format = grand_prix_schedule_entry["EventFormat"].iloc[0]

            sessions = {
                grand_prix_schedule_entry["Session1"].iloc[0]: 1,
                grand_prix_schedule_entry["Session2"].iloc[0]: 2,
                grand_prix_schedule_entry["Session3"].iloc[0]: 3,
                grand_prix_schedule_entry["Session4"].iloc[0]: 4,
                grand_prix_schedule_entry["Session5"].iloc[0]: 5,
            }

            if session_files.has_all_critical_files():
                metadata["sessions_with_all_critical_files"] += 1
            else:
                metadata["sessions_without_all_critical_files"] += 1

            session_catalog_entry = SessionCatalogEntry(
                round_number=round_number,
                year=year,
                grand_prix=grand_prix,
                session_number=sessions[session],
                session=session,
                file_path=f"{year}/{grand_prix}/{session}",
                total_drivers=total_drivers,
                total_laps=total_laps,
                event_format=event_format,
                files=session_files,
                has_results=session_files.results,
                has_laps=session_files.laps,
                has_session_status=session_files.session_status,
                has_track_status=session_files.track_status,
                has_weather=session_files.weather,
                has_messages=session_files.messages,
                completeness_score=session_files.completeness_score(),
            )

            entries.append(session_catalog_entry)

    catalog_data = [entry.to_dict() for entry in entries]
    catalog_df = pd.DataFrame(catalog_data)
    catalog_df = catalog_df.sort_values(
        ["year", "round_number", "session_number"]
    ).reset_index(drop=True)

    catalog_df["session_id"] = catalog_df.apply(
        lambda r: f"{r['year']}_R{r['round_number']:02d}_S{r['session_number']}",
        axis=1,
    )

    # Store catalod df to bucket
    catalog_df_path = f"catalogs/catalog_{year}.parquet"

    buffer = BytesIO()
    catalog_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    upload_status = bucket_client.upload_file(
        bucket_name=bucket_client.processed_data_bucket,
        object_key=catalog_df_path,
        file_obj=buffer,
    )

    if upload_status:
        metadata["catalog_upload_status"] = "Success"
    else:
        metadata["catalog_upload_status"] = "Failure"

    context.add_output_metadata(metadata=metadata)
    context.add_asset_metadata(metadata=metadata)

    return catalog_df
