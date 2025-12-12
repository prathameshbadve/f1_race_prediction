"""
Assets for data validation
"""

from io import BytesIO

import pandas as pd
from dagster import AssetExecutionContext, asset
from pydantic import ValidationError

from dagster_project.ingestion.partitions import F1_SEASON_PARTITION
from dagster_project.shared.resources import BucketResource
from src.config.logging import get_logger
from src.data.schemas import QualifyingResultSchema, RaceResultSchema, WeatherSchema

logger = get_logger("validator")


@asset(
    name="race_results_validator",
    key_prefix="transformation",
    partitions_def=F1_SEASON_PARTITION,
    compute_kind="python",
    description="Validates the race results of all grands prix",
)
def validated_race_results(
    context: AssetExecutionContext,
    bucket_resource: BucketResource,
):
    """Validates the race results of available grands prix"""

    featuers_to_keep = [
        "DriverNumber",
        "Abbreviation",
        "TeamId",
        "FullName",
        "Position",
        "ClassifiedPosition",
        "GridPosition",
        "Time",
        "Status",
        "Points",
        "Laps",
    ]

    # Get the year from the partition key in the context
    year = int(context.partition_key)

    # Initialize the resource clients
    bucket_client = bucket_resource.get_client()

    # Get the season schedule
    schedule_key = f"schedules/{year}/schedule.parquet"
    schedule_data = bucket_client.download_file(
        bucket_name=bucket_client.raw_data_bucket,
        object_key=schedule_key,
    )
    schedule_df = pd.read_parquet(BytesIO(schedule_data))

    # Create a copy of the catalog_df
    catalog_key = f"catalogs/catalog_{year}.parquet"
    catalog_data = bucket_client.download_file(
        bucket_name=bucket_client.processed_data_bucket,
        object_key=catalog_key,
    )
    catalog_df = pd.read_parquet(BytesIO(catalog_data))
    catalog_race_df = catalog_df[catalog_df["session"] == "Race"]

    metadata = {
        "expected_races": len(schedule_df),
        "available_races": len(catalog_race_df),
        "total_validated_files": 0,
        "total_failed_files": 0,
        "total_errors": 0,
        "errors": {},
    }

    for i in range(len(catalog_race_df)):
        file_path = catalog_race_df.iloc[i]["file_path"]
        object_key = f"{file_path}/results.parquet"

        race_results_data = bucket_client.download_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key=object_key,
        )

        race_results_df = pd.read_parquet(BytesIO(race_results_data))
        # Automatically find all timedelta columns and replace NaT with None
        timedelta_cols = race_results_df.select_dtypes(include=["timedelta64"]).columns
        for col in timedelta_cols:
            race_results_df[col] = race_results_df[col].replace({pd.NaT: None})

        valid_indices = []
        for idx, row in race_results_df.iterrows():
            try:
                row_dict = row.to_dict()
                RaceResultSchema.model_validate(row_dict)
                valid_indices.append(idx)

            except ValidationError as e:
                error_msg = f"Row {idx}: {str(e)}"
                for error in e.errors():
                    metadata["total_errors"] += 1
                    erred_col = error["loc"][0]
                    if erred_col in metadata["errors"]:
                        metadata["errors"][erred_col] += 1
                    else:
                        metadata["errors"][erred_col] = 1
                logger.warning("Error while validating %s: %s", object_key, error_msg)

        # Filter to valid rows only
        validated_race_results_df = (
            race_results_df.loc[valid_indices].copy()
            if valid_indices
            else pd.DataFrame()
        )

        validated_race_results_df = validated_race_results_df[featuers_to_keep]

        buffer = BytesIO()
        validated_race_results_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        validated_results_upload_status = bucket_client.upload_file(
            bucket_name=bucket_client.processed_data_bucket,
            object_key=f"silver/{object_key}",
            file_obj=buffer,
        )

        if validated_results_upload_status:
            metadata["total_validated_files"] += 1
        else:
            metadata["total_failed_files"] += 1

    context.add_output_metadata(metadata=metadata)
    context.add_asset_metadata(metadata=metadata)

    return metadata


@asset(
    name="qualifying_results_validator",
    key_prefix="transformation",
    partitions_def=F1_SEASON_PARTITION,
    compute_kind="python",
    description="Validates the qualifying results of all grands prix",
)
def validated_qualifying_results(
    context: AssetExecutionContext,
    bucket_resource: BucketResource,
):
    """Validates the qualifying results of available grands prix"""

    featuers_to_keep = [
        "DriverNumber",
        "Abbreviation",
        "TeamId",
        "FullName",
        "Position",
        "Q1",
        "Q2",
        "Q3",
    ]

    # Get the year from the partition key in the context
    year = int(context.partition_key)

    # Initialize the resource clients
    bucket_client = bucket_resource.get_client()

    # Get the season schedule
    schedule_key = f"schedules/{year}/schedule.parquet"
    schedule_data = bucket_client.download_file(
        bucket_name=bucket_client.raw_data_bucket,
        object_key=schedule_key,
    )
    schedule_df = pd.read_parquet(BytesIO(schedule_data))

    # Create a copy of the catalog_df
    catalog_key = f"catalogs/catalog_{year}.parquet"
    catalog_data = bucket_client.download_file(
        bucket_name=bucket_client.processed_data_bucket,
        object_key=catalog_key,
    )
    catalog_df = pd.read_parquet(BytesIO(catalog_data))
    catalog_race_df = catalog_df[catalog_df["session"] == "Qualifying"]

    metadata = {
        "expected_races": len(schedule_df),
        "available_races": len(catalog_race_df),
        "total_validated_files": 0,
        "total_failed_files": 0,
        "total_errors": 0,
        "errors": {},
    }

    for i in range(len(catalog_race_df)):
        file_path = catalog_race_df.iloc[i]["file_path"]
        object_key = f"{file_path}/results.parquet"

        qualifying_results_data = bucket_client.download_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key=object_key,
        )

        qualifying_results_df = pd.read_parquet(BytesIO(qualifying_results_data))
        # Automatically find all timedelta columns and replace NaT with None
        timedelta_cols = qualifying_results_df.select_dtypes(
            include=["timedelta64"]
        ).columns
        for col in timedelta_cols:
            qualifying_results_df[col] = qualifying_results_df[col].replace(
                {pd.NaT: None}
            )

        valid_indices = []
        for idx, row in qualifying_results_df.iterrows():
            try:
                row_dict = row.to_dict()
                QualifyingResultSchema.model_validate(row_dict)
                valid_indices.append(idx)

            except ValidationError as e:
                error_msg = f"Row {idx}: {str(e)}"
                for error in e.errors():
                    metadata["total_errors"] += 1
                    erred_col = error["loc"][0]
                    if erred_col in metadata["errors"]:
                        metadata["errors"][erred_col] += 1
                    else:
                        metadata["errors"][erred_col] = 1
                logger.warning("Error while validating %s: %s", object_key, error_msg)

        # Filter to valid rows only
        validated_qualifying_results_df = (
            qualifying_results_df.loc[valid_indices].copy()
            if valid_indices
            else pd.DataFrame()
        )

        validated_qualifying_results_df = validated_qualifying_results_df[
            featuers_to_keep
        ]

        buffer = BytesIO()
        validated_qualifying_results_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        validated_results_upload_status = bucket_client.upload_file(
            bucket_name=bucket_client.processed_data_bucket,
            object_key=f"silver/{object_key}",
            file_obj=buffer,
        )

        if validated_results_upload_status:
            metadata["total_validated_files"] += 1
        else:
            metadata["total_failed_files"] += 1

    context.add_output_metadata(metadata=metadata)
    context.add_asset_metadata(metadata=metadata)

    return metadata


@asset(
    name="weather_data_validator",
    key_prefix="transformation",
    partitions_def=F1_SEASON_PARTITION,
    compute_kind="python",
    description="Validates the weather data of all grands prix sessions",
)
def validated_weather_data(
    context: AssetExecutionContext,
    bucket_resource: BucketResource,
):
    """Validates the qualifying results of available grands prix"""

    # Get the year from the partition key in the context
    year = int(context.partition_key)

    # Initialize the resource clients
    bucket_client = bucket_resource.get_client()

    # Get the season schedule
    schedule_key = f"schedules/{year}/schedule.parquet"
    schedule_data = bucket_client.download_file(
        bucket_name=bucket_client.raw_data_bucket,
        object_key=schedule_key,
    )
    schedule_df = pd.read_parquet(BytesIO(schedule_data))

    # Create a copy of the catalog_df
    catalog_key = f"catalogs/catalog_{year}.parquet"
    catalog_data = bucket_client.download_file(
        bucket_name=bucket_client.processed_data_bucket,
        object_key=catalog_key,
    )
    catalog_df = pd.read_parquet(BytesIO(catalog_data))

    metadata = {
        "expected_sessions": len(schedule_df) * 5,
        "available_sessions": len(catalog_df),
        "total_validated_files": 0,
        "total_failed_files": 0,
        "total_errors": 0,
        "errors": {},
    }

    for i in range(len(catalog_df)):
        file_path = catalog_df.iloc[i]["file_path"]
        object_key = f"{file_path}/weather.parquet"

        weather_data = bucket_client.download_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key=object_key,
        )

        weather_df = pd.read_parquet(BytesIO(weather_data))
        # Automatically find all timedelta columns and replace NaT with None
        timedelta_cols = weather_df.select_dtypes(include=["timedelta64"]).columns
        for col in timedelta_cols:
            weather_df[col] = weather_df[col].replace({pd.NaT: None})

        valid_indices = []
        for idx, row in weather_df.iterrows():
            try:
                row_dict = row.to_dict()
                WeatherSchema.model_validate(row_dict)
                valid_indices.append(idx)

            except ValidationError as e:
                error_msg = f"Row {idx}: {str(e)}"
                for error in e.errors():
                    metadata["total_errors"] += 1
                    erred_col = error["loc"][0]
                    if erred_col in metadata["errors"]:
                        metadata["errors"][erred_col] += 1
                    else:
                        metadata["errors"][erred_col] = 1
                logger.warning("Error while validating %s: %s", object_key, error_msg)

        # Filter to valid rows only
        validated_weather_df = (
            weather_df.loc[valid_indices].copy() if valid_indices else pd.DataFrame()
        )

        buffer = BytesIO()
        validated_weather_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        validated_weather_upload_status = bucket_client.upload_file(
            bucket_name=bucket_client.processed_data_bucket,
            object_key=f"silver/{object_key}",
            file_obj=buffer,
        )

        if validated_weather_upload_status:
            metadata["total_validated_files"] += 1
        else:
            metadata["total_failed_files"] += 1

    context.add_output_metadata(metadata=metadata)
    context.add_asset_metadata(metadata=metadata)

    return metadata
