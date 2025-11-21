"""
Bucket Resource for MinIO/S3 operations.

This module provides:
1. BucketClient - Standalone client for MinIO/S3 operations
2. BucketResource - Dagster-aware resource wrapper
3. Support for both development (MinIO) and production (S3) environments
"""

import io
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import BinaryIO, Dict, List, Optional, Tuple

import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from dagster import ConfigurableResource

from src.config.logging import get_logger
from src.config.settings import BucketConfig


class SupportedFormats(Enum):
    """Supported file formats for upload/download"""

    CSV = ".csv"
    PARQUET = ".parquet"
    JSON = ".json"
    TXT = ".txt"
    PICKLE = ".pkl"


@dataclass
class BucketPath:
    """
    Structured path for bucket objects following the hierarchy:
    bucket/year/grand_prix/session/filename
    """

    bucket: str
    year: int
    grand_prix: str
    session: str
    filename: str

    def to_key(self) -> str:
        """Convert to S3/MinIO object key."""

        return f"{self.year}/{self.grand_prix}/{self.session}/{self.filename}"

    @classmethod
    def from_key(cls, bucket: str, key: str) -> "BucketPath":
        """Create BucketPath from object key."""

        parts = key.split("/")
        if len(parts) != 4:
            raise ValueError(
                f"Invalid key format: {key}. Expected: year/grand_prix/session/filename"
            )
        return cls(
            bucket=bucket,
            year=parts[0],
            grand_prix=parts[1],
            session=parts[2],
            filename=parts[3],
        )


class BucketClient:
    """
    Standalone client for MinIO/S3 bucket operations.

    Can be used independently in Streamlit, notebooks, or other applications.
    """

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region_name: str = "ap-south-1",
        use_ssl: bool = True,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """
        Initialize BucketClient.

        Args:
            endpoint_url: MinIO endpoint (e.g., 'http://localhost:9000'). None for S3.
            access_key: Access key for authentication
            secret_key: Secret key for authentication
            region_name: AWS region (default: ap-south-1)
            use_ssl: Use SSL for cnxs (default: True for S3, False for local MinIO)
            max_retries: Maximum number of retry attempts for failed operations
            retry_delay: Initial delay between retries (uses exponential backoff)
        """

        self.endpoint_url = endpoint_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = get_logger("resources.bucket")

        # Determine if using MinIO or S3
        self.is_minio = endpoint_url is not None

        # Configure S3 client
        self.s3_client = boto3.client(
            service_name="s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region_name,
            use_ssl=use_ssl,
            config=Config(signature_version="s3v4"),
        )

        self.logger.info(
            "BucketClient initialized (%s)",
            "MinIO" if self.is_minio else "S3",
        )

    def _validate_file_format(self, filename: str) -> bool:
        """
        Validate if file format is supported.

        Args:
            filename: Name of the file to validate

        Returns:
            True if format is supported, False otherwise
        """

        file_extension = Path(filename).suffix.lower()
        supported_extensions = [fmt.value for fmt in SupportedFormats]

        if file_extension not in supported_extensions:
            self.logger.warning(
                "Unsupported file format: %s. Supported formats: %s",
                file_extension,
                supported_extensions,
            )
            return False
        return True

    def _retry_with_backoff(self, operation, *args, **kwargs):
        """
        Execute operation with exponential backoff retry logic.

        Args:
            operation: Function to execute
            *args: Positional arguments for the operation
            **kwargs: Keyword arguments for the operation

        Returns:
            Result of the operation

        Raises:
            Exception: If all retry attempts fail
        """

        last_exception = None

        for attempt in range(self.max_retries):
            try:
                return operation(*args, **kwargs)
            except (ClientError, EndpointConnectionError, S3UploadFailedError) as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2**attempt)  # Exponential backoff
                    self.logger.warning(
                        "Attempt %d/%d failed: %s. Retrying in %ds...",
                        attempt + 1,
                        self.max_retries,
                        str(e),
                        delay,
                    )
                    time.sleep(delay)
                else:
                    self.logger.error(
                        "All %d attempts failed for operation: %s",
                        self.max_retries,
                        operation.__name__,
                    )

        raise last_exception

    def upload_file(
        self,
        bucket_path: BucketPath,
        file_path: Optional[Path] = None,
        file_obj: Optional[BinaryIO] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Upload a file to the bucket.

        Args:
            bucket_path: Structured path for the object
            file_path: Path to local file (mutually exclusive with file_obj)
            file_obj: File-like object (mutually exclusive with file_path)
            metadata: Optional metadata to attach to the object

        Returns:
            True if upload successful, False otherwise
        """

        if not self._validate_file_format(bucket_path.filename):
            # _validate_file_format handles the logging.
            # If file is valid, no message is logged.
            return False

        if file_path is None and file_obj is None:
            self.logger.error("Either file_path or file_obj must be provided")
            return False

        if file_path is not None and file_obj is not None:
            self.logger.error("Only one of file_path or file_obj should be provided")
            return False

        try:
            key = bucket_path.to_key()
            extra_args = {"Metadata": metadata} if metadata else {}

            if file_path:
                self._retry_with_backoff(
                    self.s3_client.upload_file,
                    str(file_path),
                    bucket_path.bucket,
                    key,
                    ExtraArgs=extra_args,
                )
                self.logger.info(
                    "Uploaded %s to %s/%s",
                    file_path,
                    bucket_path.bucket,
                    key,
                )
            else:
                self._retry_with_backoff(
                    self.s3_client.upload_fileobj,
                    file_obj,
                    bucket_path.bucket,
                    key,
                    ExtraArgs=extra_args,
                )
                self.logger.info(
                    "Uploaded file object to %s/%s",
                    bucket_path.bucket,
                    key,
                )

            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to upload file: %s", str(e))
            return False

    def download_file(
        self,
        bucket_path: BucketPath,
        local_path: Optional[Path] = None,
    ) -> Optional[bytes]:
        """
        Download a file from the bucket.

        Args:
            bucket_path: Structured path for the object
            local_path: Optional path to save the file locally

        Returns:
            File content as bytes if local_path is None, otherwise None
        """

        try:
            key = bucket_path.to_key()

            if local_path:
                self._retry_with_backoff(
                    self.s3_client.download_file,
                    bucket_path.bucket,
                    key,
                    str(local_path),
                )
                self.logger.info(
                    "Downloaded %s/%s to %s",
                    bucket_path.bucket,
                    key,
                    local_path,
                )
                return None
            else:
                buffer = io.BytesIO()
                self._retry_with_backoff(
                    self.s3_client.download_fileobj,
                    bucket_path.bucket,
                    key,
                    buffer,
                )
                self.logger.info("Downloaded %s/%s to memory", bucket_path.bucket, key)
                return buffer.getvalue()

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to download file: %s", str(e))
            return None

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        max_keys: int = 1000,
    ) -> Optional[List[str]]:
        """
        List objects in a bucket with optional prefix.

        Args:
            bucket: Bucket name
            prefix: Prefix to filter objects (e.g., "2024/bahrain/")
            max_keys: Maximum number of keys to return

        Returns:
            List of object keys, or None if operation fails
        """

        try:
            response = self._retry_with_backoff(
                self.s3_client.list_objects_v2,
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=max_keys,
            )

            if "Contents" not in response:
                self.logger.info(
                    "No objects found in %s with prefix '%s'", bucket, prefix
                )
                return []

            keys = [obj["Key"] for obj in response["Contents"]]
            self.logger.info("Found %d objects in %s/%s", len(keys), bucket, prefix)
            return keys

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to list objects: %s", str(e))
            return None

    def delete_file(self, bucket_path: BucketPath) -> bool:
        """
        Delete a file from the bucket.

        Args:
            bucket_path: Structured path for the object

        Returns:
            True if deletion successful, False otherwise
        """

        try:
            key = bucket_path.to_key()
            self._retry_with_backoff(
                self.s3_client.delete_object,
                Bucket=bucket_path.bucket,
                Key=key,
            )
            self.logger.info("Deleted %s/%s", bucket_path.bucket, key)
            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to delete file: %s", str(e))
            return False

    def file_exists(self, bucket_path: BucketPath) -> bool:
        """
        Check if a file exists in the bucket.

        Args:
            bucket_path: Structured path for the object

        Returns:
            True if file exists, False otherwise
        """

        try:
            key = bucket_path.to_key()
            self.s3_client.head_object(Bucket=bucket_path.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                self.logger.error("Error checking file existence: %s", str(e))
                return False

    def create_bucket(self, bucket_name: str) -> bool:
        """
        Create a new bucket.

        Args:
            bucket_name: Name of the bucket to create

        Returns:
            True if bucket created successfully, False otherwise
        """

        try:
            if self.is_minio:
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                # For AWS S3, need to specify location constraint
                # for regions other than ap-south-1
                location = {"LocationConstraint": self.s3_client.meta.region_name}
                self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration=location,
                )
            self.logger.info("Created bucket: %s", bucket_name)
            return True

        except ClientError as e:
            if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
                self.logger.warning("Bucket %s already exists", bucket_name)
                return True
            else:
                self.logger.error("Failed to create bucket: %s", str(e))
                return False

    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """
        Delete a bucket.

        Args:
            bucket_name: Name of the bucket to delete
            force: If True, delete all objects in bucket before deleting bucket

        Returns:
            True if bucket deleted successfully, False otherwise
        """

        try:
            if force:
                # Delete all objects first
                objects = self.list_objects(bucket_name)
                if objects:
                    for key in objects:
                        bucket_path = BucketPath.from_key(bucket_name, key)
                        self.delete_file(bucket_path)

            self.s3_client.delete_bucket(Bucket=bucket_name)
            self.logger.info("Deleted bucket: %s", bucket_name)
            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to delete bucket: %s", str(e))
            return False

    def batch_upload(
        self,
        files: List[Tuple[BucketPath, Path]],
        metadata: Optional[Dict[str, str]] = None,
    ) -> Dict[str, bool]:
        """
        Upload multiple files in batch.

        Args:
            files: List of tuples (BucketPath, local_file_path)
            metadata: Optional metadata to attach to all objects

        Returns:
            Dictionary mapping file paths to upload success status
        """

        results = {}
        for bucket_path, file_path in files:
            success = self.upload_file(
                bucket_path, file_path=file_path, metadata=metadata
            )
            results[str(file_path)] = success

        successful = sum(1 for v in results.values() if v)
        self.logger.info(
            "Batch upload completed: %d/%d successful",
            successful,
            len(files),
        )
        return results

    def batch_download(
        self,
        files: List[Tuple[BucketPath, Path]],
    ) -> Dict[str, bool]:
        """
        Download multiple files in batch.

        Args:
            files: List of tuples (BucketPath, local_save_path)

        Returns:
            Dictionary mapping bucket paths to download success status
        """

        results = {}
        for bucket_path, local_path in files:
            content = self.download_file(bucket_path, local_path=local_path)
            results[bucket_path.to_key()] = content is not None or local_path.exists()

        successful = sum(1 for v in results.values() if v)
        self.logger.info(
            "Batch download completed: %d/%d successful",
            successful,
            len(files),
        )
        return results


class BucketResource(ConfigurableResource):
    """
    Dagster resource wrapper for BucketClient.

    This resource can be configured in Dagster and used across assets, ops, and jobs.
    """

    def get_config(self) -> BucketConfig:
        """Get fully loaded pydantic configuration from env variables"""

        return BucketConfig.from_env()

    def get_client(self):
        """
        Get boto3 S3 client
        Called automatically by Dagster before job execution.
        """

        config = self.get_config()

        return BucketClient(
            endpoint_url=config.endpoint_url,
            access_key=config.access_key,
            secret_key=config.secret_key,
            region_name=config.region_name,
            use_ssl=True if config.endpoint_url is None else False,
        )

    @classmethod
    def from_env(cls):
        """Factory to create BucketResource from environment variables."""

        return cls(config=BucketConfig.from_env())


# Factory function for easy standalone usage
def create_bucket_client(
    endpoint_url: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region_name: str = "ap-south-1",
    use_ssl: bool = True,
) -> BucketClient:
    """
    Factory function to create a BucketClient instance.

    Useful for Streamlit apps, notebooks, or scripts.

    Example:
        >>> from dagster_project.resources.bucket_resource import create_bucket_client
        >>> client = create_bucket_client(
        ...     endpoint_url="http://localhost:9000",
        ...     access_key="minioadmin",
        ...     secret_key="minioadmin",
        ...     use_ssl=False,
        ... )
        >>> # Use the client
        >>> path = BucketPath("f1-data-raw","2024","bahrain","practice1","data.parquet")
        >>> client.upload_file(path, file_path=Path("data.parquet"))
    """

    return BucketClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        region_name=region_name,
        use_ssl=use_ssl,
    )
