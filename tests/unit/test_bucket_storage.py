"""
Unit tests for BucketClient
"""

# pylint: disable=protected-access, unused-argument

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from dagster_project.shared.resources import BucketClient, BucketPath
from src.config.settings import BucketConfig


@pytest.mark.unit
class TestBucketPath:
    """Test BucketPath dataclass"""

    def test_bucket_path_creation(self):
        """Test creating a BucketPath"""

        path = BucketPath(
            bucket="test-bucket",
            year=2024,
            grand_prix="Bahrain Grand Prix",
            session="Race",
            filename="laps.parquet",
        )

        assert path.bucket == "test-bucket"
        assert path.year == 2024
        assert path.grand_prix == "Bahrain Grand Prix"
        assert path.session == "Race"
        assert path.filename == "laps.parquet"

    def test_bucket_path_to_key(self):
        """Test converting BucketPath to object key"""

        path = BucketPath(
            bucket="test-bucket",
            year=2024,
            grand_prix="Bahrain Grand Prix",
            session="Race",
            filename="laps.parquet",
        )

        expected_key = "2024/Bahrain Grand Prix/Race/laps.parquet"
        assert path.to_key() == expected_key

    def test_bucket_path_from_key(self):
        """Test creating BucketPath from object key"""

        key = "2024/Bahrain Grand Prix/Race/laps.parquet"
        path = BucketPath.from_key("test-bucket", key)

        assert path.bucket == "test-bucket"
        assert path.year == "2024"
        assert path.grand_prix == "Bahrain Grand Prix"
        assert path.session == "Race"
        assert path.filename == "laps.parquet"

    def test_bucket_path_from_key_invalid(self):
        """Test creating BucketPath from invalid key"""

        with pytest.raises(ValueError, match="Invalid key format"):
            BucketPath.from_key("test-bucket", "invalid/key")


@pytest.mark.unit
class TestBucketClientInit:
    """Test BucketClient initialization"""

    def test_init_with_config(self, bucket_config_dict: dict):
        """Test initializing BucketClient with config"""

        config = BucketConfig(**bucket_config_dict)
        client = BucketClient(config=config)

        assert client.endpoint_url == "http://localhost:9000"
        assert client.raw_data_bucket == "test-bucket"
        assert client.is_minio is True
        assert client.max_retries == 3

    def test_init_from_env(self, mock_env_vars: dict[str, str]):
        """Test initializing BucketClient from environment"""

        client = BucketClient.from_env()

        assert client.endpoint_url == "http://localhost:9000"
        assert client.raw_data_bucket == "test-bucket"

    def test_init_custom_retries(self, bucket_config_dict: dict):
        """Test custom retry configuration"""

        config = BucketConfig(**bucket_config_dict)
        client = BucketClient(config=config, max_retries=5, retry_delay=2.0)

        assert client.max_retries == 5
        assert client.retry_delay == 2.0


@pytest.mark.unit
class TestBucketClientValidation:
    """Test BucketClient validation methods"""

    def test_validate_file_format_valid(self, bucket_config_dict: dict):
        """Test validating supported file formats"""

        config = BucketConfig(**bucket_config_dict)
        client = BucketClient(config=config)

        assert client._validate_file_format("data.parquet") is True
        assert client._validate_file_format("data.csv") is True
        assert client._validate_file_format("data.json") is True
        assert client._validate_file_format("data.pkl") is True

    def test_validate_file_format_invalid(self, bucket_config_dict: dict):
        """Test validating unsupported file formats"""

        config = BucketConfig(**bucket_config_dict)
        client = BucketClient(config=config)

        with pytest.raises(ValueError, match="Unsupported file format"):
            client._validate_file_format("data.xlsx")

        with pytest.raises(ValueError, match="Unsupported file format"):
            client._validate_file_format("data.pdf")


@pytest.mark.unit
class TestBucketClientUpload:
    """Test BucketClient upload operations"""

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_upload_file_from_path(
        self, mock_boto_client, bucket_config_dict: dict, tmp_path: Path
    ):
        """Test uploading file from local path"""
        # Setup
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        # Create test file
        test_file = tmp_path / "test.parquet"
        test_file.write_text("test data")

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="test.parquet",
        )

        # Execute
        result = client.upload_file(bucket_path=bucket_path, file_path=test_file)

        # Assert
        assert result is True
        mock_s3.upload_file.assert_called_once()

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_upload_file_from_object(
        self,
        mock_boto_client,
        bucket_config_dict: dict,
        sample_schedule_df: pd.DataFrame,
    ):
        """Test uploading file from file object"""
        # Setup
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        # Create file object
        buffer = io.BytesIO()
        sample_schedule_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="schedule.parquet",
        )

        # Execute
        result = client.upload_file(bucket_path=bucket_path, file_obj=buffer)

        # Assert
        assert result is True
        mock_s3.upload_fileobj.assert_called_once()

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_upload_file_invalid_format(
        self, mock_boto_client, bucket_config_dict: dict
    ):
        """Test uploading file with invalid format"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="data.xlsx",
        )

        result = client.upload_file(
            bucket_path=bucket_path, file_obj=io.BytesIO(b"test")
        )

        assert result is False
        mock_s3.upload_fileobj.assert_not_called()

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_upload_file_with_metadata(
        self, mock_boto_client, bucket_config_dict: dict
    ):
        """Test uploading file with metadata"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="data.parquet",
        )

        metadata = {"source": "fastf1", "version": "1.0"}

        result = client.upload_file(
            bucket_path=bucket_path, file_obj=io.BytesIO(b"test"), metadata=metadata
        )

        assert result is True
        call_args = mock_s3.upload_fileobj.call_args
        assert call_args[1]["ExtraArgs"]["Metadata"] == metadata

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_upload_file_using_bucket_name_and_key(
        self, mock_boto_client, bucket_config_dict: dict, tmp_path: Path
    ):
        """Test uploading file using bucket name and object key"""

        # Setup
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        # Create test file
        test_file = tmp_path / "test.parquet"
        test_file.write_text("test data")

        # Execute
        result = client.upload_file(
            bucket_name="test-raw", object_key="test.parquet", file_path=test_file
        )

        # Assert
        assert result is True
        mock_s3.upload_file.assert_called_once()

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_upload_file_failure_from_path_and_object(
        self,
        mock_boto_client,
        bucket_config_dict: dict,
        tmp_path: Path,
        sample_schedule_df: pd.DataFrame,
    ):
        """Test uploading file from local path"""
        # Setup
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        # Create test file
        test_file = tmp_path / "test.parquet"
        test_file.write_text("test data")

        # Create file object
        buffer = io.BytesIO()
        sample_schedule_df.to_parquet(buffer, index=False)
        buffer.seek(0)

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="test.parquet",
        )

        # Execute
        result = client.upload_file(
            bucket_path=bucket_path,
            file_path=test_file,
            file_obj=buffer,
        )

        # Assert
        assert result is False
        mock_s3.upload_file.assert_not_called()

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_upload_file_failure_from_no_path_and_no_object(
        self,
        mock_boto_client,
        bucket_config_dict: dict,
    ):
        """Test uploading file from local path"""
        # Setup
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="test.parquet",
        )

        # Execute
        result = client.upload_file(
            bucket_path=bucket_path,
            file_path=None,
            file_obj=None,
        )

        # Assert
        assert result is False
        mock_s3.upload_file.assert_not_called()


@pytest.mark.unit
class TestBucketClientDownload:
    """Test BucketClient download operations"""

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_download_file_to_memory(
        self, mock_boto_client, bucket_config_dict: dict, sample_parquet_bytes: bytes
    ):
        """Test downloading file to memory"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Mock download
        def mock_download(bucket, key, file_obj):
            file_obj.write(sample_parquet_bytes)

        mock_s3.download_fileobj.side_effect = mock_download

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="data.parquet",
        )

        result = client.download_file(bucket_path=bucket_path)

        assert result is not None
        assert isinstance(result, bytes)
        assert len(result) > 0
        mock_s3.download_fileobj.assert_called_once()

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_download_file_to_disk(
        self, mock_boto_client, bucket_config_dict: dict, tmp_path: Path
    ):
        """Test downloading file to local path"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="data.parquet",
        )

        local_path = tmp_path / "downloaded.parquet"
        result = client.download_file(bucket_path=bucket_path, local_path=local_path)

        assert result is None  # Returns None when saving to disk
        mock_s3.download_file.assert_called_once()

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_download_file_not_found(self, mock_boto_client, bucket_config_dict: dict):
        """Test downloading non-existent file"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Mock 404 error
        error = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "download_fileobj"
        )
        mock_s3.download_fileobj.side_effect = error

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="nonexistent.parquet",
        )

        result = client.download_file(bucket_path=bucket_path)

        assert result is None

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_download_file_to_disk_using_bucket_name_and_key(
        self, mock_boto_client, bucket_config_dict: dict, tmp_path: Path
    ):
        """Test downloading file to local path"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        local_path = tmp_path / "downloaded.parquet"
        result = client.download_file(
            bucket_name="test-raw", object_key="data.parquet", local_path=local_path
        )

        assert result is None  # Returns None when saving to disk
        mock_s3.download_file.assert_called_once()


@pytest.mark.unit
class TestBucketClientOperations:
    """Test BucketClient other operations"""

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_file_exists_true(self, mock_boto_client, bucket_config_dict: dict):
        """Test checking if file exists (true case)"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        mock_s3.head_object.return_value = {"ContentLength": 1024}

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="data.parquet",
        )

        result = client.file_exists(bucket_path=bucket_path)

        assert result is True

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_file_exists_false(self, mock_boto_client, bucket_config_dict: dict):
        """Test checking if file exists (false case)"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        error = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "head_object"
        )
        mock_s3.head_object.side_effect = error

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="nonexistent.parquet",
        )

        result = client.file_exists(bucket_path=bucket_path)

        assert result is False

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_file_exists_true_with_bucket_name_and_key(
        self, mock_boto_client, bucket_config_dict: dict
    ):
        """Test checking if file exists (true case)"""

        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        mock_s3.head_object.return_value = {"ContentLength": 1024}

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        result = client.file_exists(
            bucket_name="test-raw",
            object_key="data.parquet",
        )

        assert result is True

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_list_objects(self, mock_boto_client, bucket_config_dict: dict):
        """Test listing objects in bucket"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "2024/Bahrain/Race/laps.parquet"},
                {"Key": "2024/Bahrain/Race/results.parquet"},
            ]
        }

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        result = client.list_objects("test-raw", prefix="2024/Bahrain/Race/")

        assert result is not None
        assert len(result) == 2
        assert "2024/Bahrain/Race/laps.parquet" in result

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_delete_file(self, mock_boto_client, bucket_config_dict: dict):
        """Test deleting file"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="data.parquet",
        )

        result = client.delete_file(bucket_path=bucket_path)

        assert result is True
        mock_s3.delete_object.assert_called_once()

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_delete_file_using_bucket_name_and_key(
        self, mock_boto_client, bucket_config_dict: dict
    ):
        """Test deleting file"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        result = client.delete_file(
            bucket_name="test-raw",
            object_key="data.parquet",
        )

        assert result is True
        mock_s3.delete_object.assert_called_once()

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_resolve_bucket_and_key_failure_no_args(
        self,
        mock_boto_client,
        bucket_config_dict: dict,
    ):
        """Test bucket and key resolution"""
        # Setup
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        with pytest.raises(ValueError, match="Either bucket_path"):
            client._resolve_bucket_and_key()

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_resolve_bucket_and_key_failure_all_args(
        self,
        mock_boto_client,
        bucket_config_dict: dict,
    ):
        """Test bucket and key resolution"""
        # Setup
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="data.parquet",
        )

        with pytest.raises(ValueError, match="Only one of"):
            client._resolve_bucket_and_key(
                bucket_path=bucket_path,
                bucket_name="test-raw",
                object_key="data.parquet",
            )


@pytest.mark.unit
class TestBucketClientRetry:
    """Test BucketClient retry logic"""

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_retry_success_on_second_attempt(
        self, mock_boto_client, bucket_config_dict: dict
    ):
        """Test successful retry after initial failure"""
        # Setup
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # First call fails, second succeeds
        mock_s3.upload_fileobj.side_effect = [
            ClientError({"Error": {"Code": "500"}}, "upload"),
            None,  # Success
        ]

        client = BucketClient(config=config, max_retries=2, retry_delay=0.1)
        client.s3_client = mock_s3

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="data.parquet",
        )

        result = client.upload_file(
            bucket_path=bucket_path, file_obj=io.BytesIO(b"test")
        )

        assert result is True
        assert mock_s3.upload_fileobj.call_count == 2

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_retry_exhausted(self, mock_boto_client, bucket_config_dict: dict):
        """Test retry logic when all attempts fail"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # All calls fail
        mock_s3.upload_fileobj.side_effect = [
            ClientError({"Error": {"Code": "500"}}, "upload"),
            ClientError({"Error": {"Code": "500"}}, "upload"),
            ClientError({"Error": {"Code": "500"}}, "upload"),
        ]

        client = BucketClient(config=config, max_retries=3, retry_delay=0.1)
        client.s3_client = mock_s3

        bucket_path = BucketPath(
            bucket="test-raw",
            year=2024,
            grand_prix="Bahrain",
            session="Race",
            filename="data.parquet",
        )

        result = client.upload_file(
            bucket_path=bucket_path, file_obj=io.BytesIO(b"test")
        )

        assert result is False
        assert mock_s3.upload_fileobj.call_count == 3

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_retry_exhausted_raise_exception(
        self, mock_boto_client, bucket_config_dict: dict
    ):
        """Test retry logic when all attempts fail"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config, max_retries=3, retry_delay=0.1)
        client.s3_client = mock_s3

        def mock_operation():
            raise ClientError({"Error": {"Code": "500"}}, "upload")

        with pytest.raises(ClientError):
            client._retry_with_backoff(mock_operation)


@pytest.mark.unit
class TestBucketClientBatch:
    """Test BucketClient batch operations"""

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_batch_upload(self, mock_boto_client, bucket_config_dict: dict):
        """Test batch upload operation"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        files = [
            (
                None,
                "test-raw",
                "2024/Bahrain Grand Prix/Race/file1.parquet",
                None,
                io.BytesIO(b"data1"),
            ),
            (
                BucketPath("test-raw", 2024, "Bahrain", "Race", "file2.parquet"),
                None,
                None,
                None,
                io.BytesIO(b"data2"),
            ),
        ]

        result = client.batch_upload(files)

        assert len(result) == 2
        assert all(v is True for v in result.values())
        assert mock_s3.upload_fileobj.call_count == 2

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_batch_download(
        self,
        mock_boto_client,
        bucket_config_dict: dict,
        sample_parquet_bytes: bytes,
    ):
        """Test batch upload operation"""
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        # Mock download
        def mock_download(bucket, key, file_obj):
            file_obj.write(sample_parquet_bytes)

        mock_s3.download_fileobj.side_effect = mock_download

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        files = [
            (
                None,
                "test-raw",
                "2024/Bahrain Grand Prix/Race/file1.parquet",
                None,
            ),
            (
                BucketPath("test-raw", 2024, "Bahrain", "Race", "file2.parquet"),
                None,
                None,
                None,
            ),
        ]

        result = client.batch_download(files)

        assert len(result) == 2
        assert all(v["status"] is True for v in result.values())
        assert mock_s3.download_fileobj.call_count == 2


class TestBucketClientBuckets:
    """Test BucketClient bucket creation and deletion"""

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_bucket_creation(
        self,
        mock_boto_client,
        bucket_config_dict: dict,
    ):
        """Test bucket creation operation"""

        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        result = client.create_bucket(bucket_name="test-bucket")

        assert result is True
        mock_s3.create_bucket.assert_called_once()

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_empty_bucket_deletion(
        self,
        mock_boto_client,
        bucket_config_dict: dict,
    ):
        """Test bucket deletion operation"""

        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        result = client.delete_bucket(bucket_name="test-bucket", force=False)

        assert result is True
        mock_s3.delete_bucket.assert_called_once()

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_non_empty_bucket_deletion(
        self,
        mock_boto_client,
        bucket_config_dict: dict,
    ):
        """Test bucket deletion operation"""

        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        client.list_objects = MagicMock(return_value=["file1.parquet", "file2.parquet"])
        client.delete_file = MagicMock(return_value=True)

        result = client.delete_bucket(bucket_name="test-bucket", force=True)

        assert result is True
        mock_s3.delete_bucket.assert_called_once()

    @patch("dagster_project.shared.resources.bucket_resource.boto3.client")
    def test_non_empty_bucket_deletion_fail(
        self,
        mock_boto_client,
        bucket_config_dict: dict,
    ):
        """Test bucket deletion operation"""

        # Setup
        config = BucketConfig(**bucket_config_dict)
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "file1.parquet"},
                {"Key": "file2.parquet"},
            ]
        }

        client = BucketClient(config=config)
        client.s3_client = mock_s3

        result = client.delete_bucket(bucket_name="test-bucket", force=False)

        assert result is False
        mock_s3.delete_bucket.assert_not_called()
