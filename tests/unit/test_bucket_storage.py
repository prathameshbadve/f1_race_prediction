"""
Tests for BucketResource and BucketClient

This file demonstrates:
1. Unit tests for BucketPath
2. Integration tests with actual MinIO
3. Mocked tests for S3 operations
4. Dagster asset tests
"""

import io
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest
import pandas as pd
from botocore.exceptions import ClientError

from dagster_project.resources.bucket_resource import (
    BucketClient,
    BucketResource,
    BucketPath,
    SupportedFormats,
    create_bucket_client,
)


# =============================================================================
# Unit Tests for BucketPath
# =============================================================================


class TestBucketPath:
    """Unit tests for BucketPath dataclass."""

    def test_bucket_path_to_key(self):
        """Test converting BucketPath to S3 key."""

        path = BucketPath(
            bucket="f1-data-raw",
            year="2024",
            grand_prix="Bahrain Grand Prix",
            session="Practice 1",
            filename="data.parquet",
        )
        assert path.to_key() == "2024/Bahrain Grand Prix/Practice 1/data.parquet"

    def test_bucket_path_from_key(self):
        """Test creating BucketPath from S3 key."""

        key = "2024/Monaco Grand Prix/Qualifying/telemetry.parquet"
        path = BucketPath.from_key("f1-data-raw", key)

        assert path.bucket == "f1-data-raw"
        assert path.year == "2024"
        assert path.grand_prix == "Monaco Grand Prix"
        assert path.session == "Qualifying"
        assert path.filename == "telemetry.parquet"

    def test_bucket_path_from_key_invalid(self):
        """Test that invalid key format raises error."""

        with pytest.raises(ValueError, match="Invalid key format"):
            BucketPath.from_key("bucket", "invalid/key")

    def test_bucket_path_roundtrip(self):
        """Test that to_key and from_key are inverses."""

        original = BucketPath(
            bucket="test-bucket",
            year="2023",
            grand_prix="British Grand Prix",
            session="Race",
            filename="results.csv",
        )

        key = original.to_key()
        reconstructed = BucketPath.from_key("test-bucket", key)

        assert original == reconstructed


# =============================================================================
# Unit Tests for BucketClient (with mocking)
# =============================================================================


class TestBucketClientUnit:
    """Unit tests for BucketClient using mocks."""

    @pytest.fixture
    def bucket_client(self, mock_s3_client):
        """Create BucketClient with mocked S3."""

        return BucketClient(
            endpoint_url="http://localhost:9000",
            access_key="test",
            secret_key="test",
            use_ssl=False,
        )

    # pylint: disable=protected-access
    def test_validate_file_format_supported(self, bucket_client):
        """Test file format validation for supported formats."""

        assert bucket_client._validate_file_format("data.parquet") is True
        assert bucket_client._validate_file_format("data.csv") is True
        assert bucket_client._validate_file_format("data.json") is True

    def test_validate_file_format_unsupported(self, bucket_client):
        """Test file format validation for unsupported formats."""

        assert bucket_client._validate_file_format("data.xlsx") is False
        assert bucket_client._validate_file_format("data.doc") is False

    def test_upload_file_success(self, bucket_client, mock_s3_client, tmp_path):
        """Test successful file upload."""

        # Create a test file
        test_file = tmp_path / "test.parquet"
        test_file.write_text("test data")

        bucket_path = BucketPath(
            bucket="test-bucket",
            year="2024",
            grand_prix="test",
            session="test",
            filename="test.parquet",
        )

        result = bucket_client.upload_file(bucket_path, file_path=test_file)

        assert result is True
        mock_s3_client.upload_file.assert_called_once()

    def test_upload_file_unsupported_format(self, bucket_client, tmp_path):
        """Test upload with unsupported file format."""

        test_file = tmp_path / "test.xlsx"
        test_file.write_text("test data")

        bucket_path = BucketPath(
            bucket="test-bucket",
            year="2024",
            grand_prix="test",
            session="test",
            filename="test.xlsx",
        )

        result = bucket_client.upload_file(bucket_path, file_path=test_file)

        assert result is False

    def test_upload_file_no_source(self, bucket_client):
        """Test upload without file_path or file_obj."""

        bucket_path = BucketPath(
            bucket="test-bucket",
            year="2024",
            grand_prix="test",
            session="test",
            filename="test.parquet",
        )

        result = bucket_client.upload_file(bucket_path)

        assert result is False

    def test_file_exists_true(self, bucket_client, mock_s3_client):
        """Test file_exists when file exists."""

        mock_s3_client.head_object.return_value = {}

        bucket_path = BucketPath(
            bucket="test-bucket",
            year="2024",
            grand_prix="test",
            session="test",
            filename="test.parquet",
        )

        assert bucket_client.file_exists(bucket_path) is True

    def test_file_exists_false(self, bucket_client, mock_s3_client):
        """Test file_exists when file doesn't exist."""

        error_response = {"Error": {"Code": "404"}}
        mock_s3_client.head_object.side_effect = ClientError(
            error_response, "head_object"
        )

        bucket_path = BucketPath(
            bucket="test-bucket",
            year="2024",
            grand_prix="test",
            session="test",
            filename="test.parquet",
        )

        assert bucket_client.file_exists(bucket_path) is False

    def test_list_objects_success(self, bucket_client, mock_s3_client):
        """Test listing objects successfully."""

        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "2024/bahrain/practice1/data.parquet"},
                {"Key": "2024/bahrain/practice2/data.parquet"},
            ]
        }

        objects = bucket_client.list_objects("test-bucket", prefix="2024/bahrain/")

        assert len(objects) == 2
        assert "2024/bahrain/practice1/data.parquet" in objects

    def test_list_objects_empty(self, bucket_client, mock_s3_client):
        """Test listing objects when no objects exist."""

        mock_s3_client.list_objects_v2.return_value = {}

        objects = bucket_client.list_objects("test-bucket")

        assert objects == []

    def test_retry_with_backoff_success(self, bucket_client):
        """Test retry mechanism succeeds on second attempt."""

        mock_operation = Mock(side_effect=[ClientError({}, "test"), "success"])

        result = bucket_client._retry_with_backoff(mock_operation)

        assert result == "success"
        assert mock_operation.call_count == 2

    def test_retry_with_backoff_all_fail(self, bucket_client):
        """Test retry mechanism fails after max attempts."""

        mock_operation = Mock(side_effect=ClientError({}, "test"))

        with pytest.raises(ClientError):
            bucket_client._retry_with_backoff(mock_operation)

        assert mock_operation.call_count == bucket_client.max_retries


# =============================================================================
# Integration Tests (requires Docker services)
# =============================================================================


@pytest.mark.integration
class TestBucketClientIntegration:
    """Integration tests for BucketClient with actual MinIO."""

    @pytest.fixture(scope="class")
    def minio_client(self):
        """Create BucketClient connected to test MinIO instance."""
        client = create_bucket_client(
            endpoint_url="http://localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            use_ssl=False,
        )

        # Create test bucket
        client.create_bucket("test-integration-bucket")

        yield client

        # Cleanup
        client.delete_bucket("test-integration-bucket", force=True)

    def test_upload_download_roundtrip(self, minio_client, tmp_path):
        """Test uploading and downloading a file."""
        # Create test data
        df = pd.DataFrame({"lap": [1, 2, 3], "time": [90.1, 89.5, 88.9]})

        # Save locally
        local_file = tmp_path / "test_data.parquet"
        df.to_parquet(local_file)

        # Upload
        bucket_path = BucketPath(
            bucket="test-integration-bucket",
            year="2024",
            grand_prix="test",
            session="test",
            filename="test_data.parquet",
        )

        upload_success = minio_client.upload_file(bucket_path, file_path=local_file)
        assert upload_success is True

        # Verify file exists
        assert minio_client.file_exists(bucket_path) is True

        # Download
        download_path = tmp_path / "downloaded.parquet"
        downloaded_data = minio_client.download_file(
            bucket_path, local_path=download_path
        )

        # Verify content
        downloaded_df = pd.read_parquet(download_path)
        pd.testing.assert_frame_equal(df, downloaded_df)

    def test_batch_upload_download(self, minio_client, tmp_path):
        """Test batch upload and download operations."""
        # Create test files
        files_to_upload = []
        for i in range(3):
            df = pd.DataFrame({"value": [i]})
            file_path = tmp_path / f"file_{i}.parquet"
            df.to_parquet(file_path)

            bucket_path = BucketPath(
                bucket="test-integration-bucket",
                year="2024",
                grand_prix="test",
                session="test",
                filename=f"file_{i}.parquet",
            )
            files_to_upload.append((bucket_path, file_path))

        # Batch upload
        results = minio_client.batch_upload(files_to_upload)

        assert all(results.values())
        assert len(results) == 3

    def test_list_objects_integration(self, minio_client, tmp_path):
        """Test listing objects in MinIO."""
        # Upload some test files
        for session in ["practice1", "practice2"]:
            df = pd.DataFrame({"data": [1, 2, 3]})
            file_path = tmp_path / f"{session}.parquet"
            df.to_parquet(file_path)

            bucket_path = BucketPath(
                bucket="test-integration-bucket",
                year="2024",
                grand_prix="test",
                session=session,
                filename="data.parquet",
            )
            minio_client.upload_file(bucket_path, file_path=file_path)

        # List objects
        objects = minio_client.list_objects(
            "test-integration-bucket", prefix="2024/test/"
        )

        assert len(objects) == 2
        assert any("practice1" in obj for obj in objects)
        assert any("practice2" in obj for obj in objects)

    def test_delete_file_integration(self, minio_client, tmp_path):
        """Test deleting a file from MinIO."""
        # Upload a file
        df = pd.DataFrame({"data": [1, 2, 3]})
        file_path = tmp_path / "delete_test.parquet"
        df.to_parquet(file_path)

        bucket_path = BucketPath(
            bucket="test-integration-bucket",
            year="2024",
            grand_prix="test",
            session="test",
            filename="delete_test.parquet",
        )

        minio_client.upload_file(bucket_path, file_path=file_path)
        assert minio_client.file_exists(bucket_path) is True

        # Delete
        delete_success = minio_client.delete_file(bucket_path)
        assert delete_success is True
        assert minio_client.file_exists(bucket_path) is False


# =============================================================================
# Dagster Tests
# =============================================================================


@pytest.mark.dagster
class TestBucketResourceDagster:
    """Tests for BucketResource in Dagster context."""

    def test_bucket_resource_configuration(self):
        """Test BucketResource can be configured properly."""
        resource = BucketResource(
            endpoint_url="http://localhost:9000",
            access_key="test",
            secret_key="test",
            region_name="us-east-1",
            use_ssl=False,
        )

        assert resource.endpoint_url == "http://localhost:9000"
        assert resource.access_key == "test"
        assert resource.use_ssl is False

    def test_bucket_resource_get_client(self):
        """Test getting BucketClient from resource."""
        resource = BucketResource(
            endpoint_url="http://localhost:9000",
            access_key="test",
            secret_key="test",
            use_ssl=False,
        )

        client = resource.get_client()

        assert isinstance(client, BucketClient)
        assert client.endpoint_url == "http://localhost:9000"
        assert client.is_minio is True


# =============================================================================
# Factory Function Tests
# =============================================================================


def test_create_bucket_client_factory():
    """Test factory function creates client correctly."""
    client = create_bucket_client(
        endpoint_url="http://localhost:9000",
        access_key="test",
        secret_key="test",
        use_ssl=False,
    )

    assert isinstance(client, BucketClient)
    assert client.endpoint_url == "http://localhost:9000"
    assert client.is_minio is True


def test_create_bucket_client_s3():
    """Test factory function for S3 (no endpoint_url)."""
    client = create_bucket_client(
        access_key="test",
        secret_key="test",
    )

    assert isinstance(client, BucketClient)
    assert client.endpoint_url is None
    assert client.is_minio is False


# =============================================================================
# Pytest Configuration
# =============================================================================


def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (require Docker)"
    )
    config.addinivalue_line("markers", "dagster: marks tests as Dagster-specific tests")
