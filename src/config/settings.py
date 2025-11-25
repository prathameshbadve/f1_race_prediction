"""
All configuration settings for the project are defined in this module.
"""

import os
from enum import Enum
from pathlib import Path
from typing import Optional

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

    endpoint_url: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    region_name: str = "ap-south-1"
    raw_data_bucket: str = "f1-data-raw"
    processed_data_bucket: str = "f1-data-processed"
    model_bucket: str = "f1-model-artifacts"

    @classmethod
    def from_env(cls):
        """Factory to create StorageConfig from environment variables."""

        return cls(
            endpoint_url=os.getenv("DOCKER_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("SECRET_KEY", "minioadmin"),
            region_name=os.getenv("REGION", "ap-south-1"),
            raw_data_bucket=os.getenv("RAW_DATA_BUCKET", "f1-data-raw"),
            processed_data_bucket=os.getenv(
                "PROCESSED_DATA_BUCKET", "f1-data-processed"
            ),
            model_bucket=os.getenv("MODEL_BUCKET", "f1-model-artifacts"),
        )


class DatabaseConfig(BaseConfig):
    """Configuration for Database connection."""

    connection_string: Optional[str] = None
    host: Optional[str] = None
    port: int = 5432
    user: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    backend: str = "postgresql"

    @classmethod
    def from_env(cls):
        """Factory to create DatabaseConfig from environment variables."""

        return cls(
            connection_string=os.getenv("DB_URL"),
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            user=os.getenv("DB_USER", "user"),
            password=os.getenv("DB_PASSWORD", "password"),
            database=os.getenv("DB_NAME", "data_warehouse"),
            backend=os.getenv("DB_BACKEND", "postgresql"),
        )


class RedisConfig(BaseConfig):
    """Congifuration for Redis Cache"""

    host: Optional[str] = None
    port: int = 6379
    db: int = 0
    password: Optional[str] = None

    @classmethod
    def from_env(cls):
        """Factory to create RedisConfig from environment variables."""

        return cls(
            host=os.getenv("REDIS_DOCKER_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            password=os.getenv("REDIS_PASSWORD", None),
        )


class FastF1Config(BaseConfig):
    """Configuration settings required for FastF1"""

    # Cache settings
    cache_enabled: Optional[bool] = None
    cache_dir: Optional[str] = None
    force_renew_cache: Optional[bool] = None

    # Log settings
    log_level: Optional[str] = None

    # Connection settings
    request_timeout: Optional[int] = None
    max_retries: Optional[int] = None
    retry_delay: Optional[int] = None

    # Testing sessions
    include_testing: Optional[bool] = None

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
