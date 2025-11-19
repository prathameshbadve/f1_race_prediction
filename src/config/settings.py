"""
All configuration settings for the project are defined in this module.
"""

import os
from enum import Enum
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from src.utils.helpers import get_project_root

project_root = get_project_root()
env_path = project_root / ".env"

load_dotenv(dotenv_path=env_path)


class Environment(str, Enum):
    """Environment types"""

    DEVELOPMENT = "development"
    PRODUCTION = "production"


class BaseConfig(BaseModel):
    """Base configuration class for the project."""

    environment: str = Field(
        default_factory=lambda: os.getenv("ENVIRONMENT", Environment.DEVELOPMENT.value)
    )
    project_root: Path = Field(default_factory=get_project_root)


class BucketConfig(BaseConfig):
    """
    Configuration for MinIO/S3 Bucket Storage.
    """

    endpoint: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    raw_data_bucket: str | None = None
    processed_data_bucket: str | None = None
    region: str | None = None
    secure: bool | None = False

    @classmethod
    def from_env(cls):
        """Factory to create StorageConfig from environment variables."""

        if cls.environment == Environment.DEVELOPMENT.value:
            # In development, use local MinIO settings
            return cls(
                endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
                access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
                secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
                raw_data_bucket=os.getenv("RAW_DATA_BUCKET", "f1-data-raw"),
                processed_data_bucket=os.getenv(
                    "PROCESSED_DATA_BUCKET", "f1-data-processed"
                ),
                region=os.getenv("REGION", "ap-south-1"),
                secure=os.getenv("SECURE", "False").lower() == "true",
            )

        # In production, use environment-provided AWS settings
        return cls(
            endpoint=os.getenv("AWS_S3_ENDPOINT_URL"),
            access_key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            raw_data_bucket=os.getenv("RAW_DATA_BUCKET"),
            processed_data_bucket=os.getenv("PROCESSED_DATA_BUCKET"),
            region=os.getenv("REGION", "ap-south-1"),
            secure=os.getenv("AWS_SECURE", "False").lower() == "true",
        )


class FastF1Config(BaseConfig):
    """Configuration settings required for FastF1"""

    # Cache settings
    cache_enabled: bool | None = None
    cache_dir: str | None = None
    force_renew_cache: bool | None = None

    # Log settings
    log_level: str | None = None

    # Connection settings
    request_timeout: int | None = None
    max_retries: int | None = None
    retry_delay: int | None = None

    # Testing sessions
    include_testing: bool | None = None

    @classmethod
    def from_env(cls):
        """Factory to create FastF1Config from environment variables."""

        return cls(
            cache_enabled=(
                os.getenv("FASTF1_CACHE_ENABLED", "False").lower() == "true"
            ),
            cache_dir=os.getenv("FASTF1_CACHE_DIR", "data/external/fastf1_cache"),
            force_renew_cache=(
                os.getenv("FASTF1_FORCE_RENEW_CACHE", "False").lower() == "true"
            ),
            log_level=os.getenv("FASTF1_LOG_LEVEL", "DEBUG"),
            request_timeout=int(os.getenv("FASTF1_REQUEST_TIMEOUT", "30")),
            max_retries=int(os.getenv("FASTF1_MAX_RETRIES", "3")),
            retry_delay=int(os.getenv("FASTF1_RETRY_DELAY", "5")),
            include_testing=(
                os.getenv("FASTF1_INCLUDE_TESTING", "False").lower() == "true"
            ),
        )
