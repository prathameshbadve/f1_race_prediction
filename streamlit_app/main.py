"""
Main streamlit script
"""

from io import BytesIO

import pandas as pd
import streamlit as st

from dagster_project.shared.resources import BucketClient
from src.config.settings import BucketConfig

bucket_config = BucketConfig(
    endpoint_url="http://localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    region_name="ap-south-1",
    raw_data_bucket="f1-data-raw",
    processed_data_bucket="f1-data-processed",
    model_bucket="f1-model-artifacts",
)

bucket_client = BucketClient(config=bucket_config)

st.set_page_config(
    layout="wide",
    page_title="Formula 1",
    initial_sidebar_state="collapsed",
)

st.title("F1 Race Prediction")

st.header("Season Schedule Viewer")

c1, c2, c3 = st.columns([1, 1, 1])

with c1:
    season_selector = st.selectbox(
        label="Season",
        options=list(range(2018, 2026)),
        index=0,
        key="season_selector",
    )

if season_selector:
    season_data = bucket_client.download_file(
        bucket_name=bucket_client.raw_data_bucket,
        object_key=f"schedules/{season_selector}/schedule.parquet",
    )
    season_df = pd.read_parquet(BytesIO(season_data))

    columns_to_show = [
        "RoundNumber",
        "EventDate",
        "Country",
        "Location",
        "EventName",
        "EventFormat",
        "Session1",
        "Session2",
        "Session3",
        "Session4",
        "Session5",
    ]

    st.dataframe(season_df[columns_to_show], hide_index=True)
