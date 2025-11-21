"""
Complete test asset for BucketResource and DatabaseResource integration.

This script demonstrates a complete data pipeline:
1. Fetch data from FastF1 API (or mock data)
2. Store raw data in MinIO bucket
3. Process the data
4. Store processed data in PostgreSQL database
5. Query and verify the data

Run this script to test both resources working together!
"""

import io
from datetime import datetime

from dagster import asset

from dagster_project.shared.resources import (
    BucketPath,
    BucketResource,
    DatabaseResource,
)
from src.config.logging import get_logger

logger = get_logger("data_ingestion.example")

# Asset 1


@asset
def load_data_to_db(
    db_resource: DatabaseResource,
):
    """Load data to database (if needed)"""

    db_client = db_resource.get_client()

    logger.info("📡 Fetching race data...")

    # Mock race data (replace with actual FastF1 API call)
    race_data = {
        "name": "Australian Grand Prix",
        "year": 2024,
        "round": 2,
        "country": "Australia",
        "circuit": "Albert Park Circuit",
        "date": datetime(2024, 3, 16, 15, 0, 0),
    }

    insert_status = db_client.insert(
        table_name="races", data=race_data, return_ids=True
    )

    return {
        "status": "success",
        "race_name": race_data["name"],
        "race_year": race_data["year"],
        "insert_ids": insert_status if insert_status else [],
    }


# Asset 2: Fetch from db and store in bucket
@asset(deps=["load_data_to_db"])
def fetch_and_store(
    db_resource: DatabaseResource,
    bucket_resource: BucketResource,
):
    """Fetch data from db and store in bucket"""

    db_client = db_resource.get_client()
    bucket_client = bucket_resource.get_client()

    logger.info("🔍 Query db")

    df = db_client.query_to_dataframe(table_name="races")

    df_buffer = io.BytesIO()
    df.to_parquet(df_buffer, index=False)
    df_buffer.seek(0)

    object_key = BucketPath(
        bucket="test-bucket",
        year=2024,
        grand_prix="Australian Grand Prix",
        session="Race",
        filename="schedule.parquet",
    )

    upload_status = bucket_client.upload_file(
        bucket_path=object_key,
        file_obj=df_buffer,
    )

    return {
        "status": upload_status,
        "rows_fetched": len(df),
    }


# CREATE TABLE IF NOT EXISTS races (
#     id SERIAL PRIMARY KEY,
#     name VARCHAR(100) NOT NULL,
#     year INTEGER NOT NULL,
#     round INTEGER NOT NULL,
#     country VARHCAR(50),
#     circuit VARCHAR(100),
#     date TIMESTAMP,
#     create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#     UNIQUE (year, round),
# );
