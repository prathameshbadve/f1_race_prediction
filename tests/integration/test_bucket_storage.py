"""
Integartion tests for bucket storage
"""

import io
from unittest.mock import MagicMock

import pandas as pd
import pandas.testing as pdt
import pytest
from botocore.exceptions import ClientError

from dagster_project.shared.resources import BucketPath


@pytest.mark.integration
class TestBucketClientBucketOperations:
    """Test bucket client bucket operations for integration"""

    def test_bucket_creation(self, bucket_client):
        """Test that a bucket is created successfully"""

        new_bucket_name = "test-bucket-new"
        bucket_creation_status = bucket_client.create_bucket(
            bucket_name=new_bucket_name
        )

        assert bucket_creation_status is True

        # Cleanup
        bucket_client.delete_bucket(bucket_name=new_bucket_name)

    def test_bucket_creation_already_exists(self, bucket_client):
        """Test creating a bucket that already exists raises an error"""

        # Create a new bucket
        new_bucket_name = "test-bucket-new"
        _ = bucket_client.create_bucket(bucket_name=new_bucket_name)

        # Try creating it again
        with pytest.raises(ClientError):
            bucket_client.create_bucket(bucket_name=new_bucket_name)

        # Cleanup
        bucket_client.delete_bucket(bucket_name=new_bucket_name)

    def test_bucket_exists(self, bucket_client):
        """Test created bucket exists"""

        new_bucket_name = "test-bucket-new"
        _ = bucket_client.create_bucket(bucket_name=new_bucket_name)
        bucket_check = bucket_client.bucket_exists(bucket_name=new_bucket_name)

        assert bucket_check is True

        # Cleanup
        bucket_client.delete_bucket(bucket_name=new_bucket_name)

    def test_bucket_exists_false_non_existent(self, bucket_client):
        """Test that non existent bucket returns False"""

        bucket_check = bucket_client.bucket_exists(bucket_name="nonexistent")

        assert bucket_check is False

    def test_bucket_deletion(self, bucket_client):
        """Test that an existing bucket is deleted successfully"""

        # Create bucket to test deletion
        new_bucket_name = "test-bucket-new"
        _ = bucket_client.create_bucket(bucket_name=new_bucket_name)

        # Delete the bucket
        bucket_delete_status = bucket_client.delete_bucket(bucket_name=new_bucket_name)

        assert bucket_delete_status is True

    def test_non_empty_bucket_deletion_failure(self, bucket_client):
        """Test that a non empty bucket is not deleted with force=False"""

        # Create a new bucket again
        new_bucket_name = "test-bucket-new"
        _ = bucket_client.create_bucket(bucket_name=new_bucket_name)

        # Add a file to this bucket
        buffer = io.BytesIO(b"test")
        bucket_client.upload_file(
            bucket_name=new_bucket_name,
            object_key="test_folder/test.parquet",
            file_obj=buffer,
        )

        # Try deleting the non-empty bucket
        bucket_delete_status = bucket_client.delete_bucket(
            bucket_name=new_bucket_name, force=False
        )

        assert bucket_delete_status is False

        # Cleanup
        bucket_client.delete_bucket(bucket_name=new_bucket_name, force=True)

    def test_non_empty_bucket_deletion_success(self, bucket_client):
        """Test that non empty bucket is deleted when force=True"""

        # Create a new bucket again
        new_bucket_name = "test-bucket-new"
        _ = bucket_client.create_bucket(bucket_name=new_bucket_name)

        # Add a file to this bucket
        buffer = io.BytesIO(b"test")
        bucket_client.upload_file(
            bucket_name=new_bucket_name,
            object_key="test_folder/test.parquet",
            file_obj=buffer,
        )

        # Try deleting the non-empty bucket
        bucket_delete_status = bucket_client.delete_bucket(
            bucket_name=new_bucket_name, force=True
        )

        assert bucket_delete_status is True

        # Cleanup
        bucket_client.delete_bucket(bucket_name=new_bucket_name)

    def test_delete_nonexistent_bucket(self, bucket_client):
        """Test deleting a non-existent bucket returns False"""

        bucket_client.logger = MagicMock()
        bucket_deletion_status = bucket_client.delete_bucket(bucket_name="nonexistent")

        assert bucket_deletion_status is False
        bucket_client.logger.error.assert_called()


@pytest.mark.integration
class TestBucketClientUploadDownload:
    """Test bucket client upload and download operations"""

    def test_bucket_storage_roundtrip_parquet(self, bucket_client):
        """Test that the df is uploaded and is the same after downloa"""

        df = pd.DataFrame(
            {
                "driver": ["Charles", "Lando"],
                "number": [16, 4],
                "team": ["Scuderia Ferrari", "McLaren F1"],
            }
        )

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        upload_status = bucket_client.upload_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/sample_df.parquet",
            file_obj=buffer,
        )

        downloaded_data = bucket_client.download_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/sample_df.parquet",
        )
        downloaded_df = pd.read_parquet(io.BytesIO(downloaded_data))

        assert upload_status is True
        pdt.assert_frame_equal(
            df.reset_index(drop=True),
            downloaded_df.reset_index(drop=True),
        )

        # Cleanup
        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )

    def test_bucket_storage_roundtrip_csv(self, bucket_client):
        """Test that the df is uploaded and is the same after downloa"""

        df = pd.DataFrame(
            {
                "driver": ["Charles", "Lando"],
                "number": [16, 4],
                "team": ["Scuderia Ferrari", "McLaren F1"],
            }
        )

        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        upload_status = bucket_client.upload_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/sample_df.csv",
            file_obj=buffer,
        )

        downloaded_data = bucket_client.download_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/sample_df.csv",
        )
        downloaded_df = pd.read_csv(io.BytesIO(downloaded_data))

        assert upload_status is True
        pdt.assert_frame_equal(
            df.reset_index(drop=True),
            downloaded_df.reset_index(drop=True),
        )

        # Cleanup
        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )

    def test_upload_to_nonexistent_bucket(self, bucket_client):
        """Test uploading to nonexistent buckets"""

        df = pd.DataFrame(
            {
                "driver": ["Charles", "Lando"],
                "number": [16, 4],
                "team": ["Scuderia Ferrari", "McLaren F1"],
            }
        )

        buffer = io.BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        upload_status = bucket_client.upload_file(
            bucket_name="nonexistent",
            object_key="integration/sample_df.csv",
            file_obj=buffer,
        )

        assert upload_status is False

    def test_download_from_nonexistent_bucket(self, bucket_client):
        """Test downloading from nonexistent bucket"""

        download_status = bucket_client.download_file(
            bucket_name="nonexistent", object_key="integration/test.csv"
        )

        assert download_status is None

    def test_downloading_nonexistent_object(self, bucket_client):
        """Test downloading a non-existent object"""

        download_status = bucket_client.download_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/nonexistent.csv",
        )

        assert download_status is None

    def test_upload_unsupported_file(self, bucket_client):
        """Test uploading an unsupported file"""

        upload_status = bucket_client.upload_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/test.pdf",
            file_obj=io.BytesIO(b"test"),
        )

        assert upload_status is False

    def test_upload_download_using_bucket_path(self, bucket_client):
        """Test upload and download using bucket path"""

        bucket_path = BucketPath(
            bucket=bucket_client.raw_data_bucket,
            year=2024,
            grand_prix="Italian Grand Prix",
            session="Race",
            filename="test.parquet",
        )

        df = pd.DataFrame(
            {
                "driver": ["Charles", "Lando"],
                "number": [16, 4],
                "team": ["Scuderia Ferrari", "McLaren F1"],
            }
        )
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        upload_status = bucket_client.upload_file(
            bucket_path=bucket_path,
            file_obj=buffer,
        )

        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)

        downloaded_data = bucket_client.download_file(bucket_path=bucket_path)
        downloaded_df = pd.read_parquet(io.BytesIO(downloaded_data))

        delete_status = bucket_client.delete_file(bucket_path=bucket_path)

        assert upload_status is True
        assert len(objects) == 1
        assert "2024/Italian Grand Prix/Race/test.parquet" in objects
        pdt.assert_frame_equal(
            df.reset_index(drop=True),
            downloaded_df.reset_index(drop=True),
        )
        assert delete_status is True

    def test_batch_upload_download_operations(self, bucket_client):
        """Test batch upload/download operations"""

        df1 = pd.DataFrame(
            {
                "driver": ["Charles", "Lando"],
                "number": [16, 4],
                "team": ["Scuderia Ferrari", "McLaren F1"],
            }
        )
        buffer1 = io.BytesIO()
        df1.to_parquet(buffer1, index=False)
        buffer1.seek(0)

        df2 = pd.DataFrame(
            {
                "driver": ["Max", "Lewis"],
                "number": [33, 44],
                "team": ["Redbull Racing", "Scuderia Ferrari"],
            }
        )
        buffer2 = io.BytesIO()
        df2.to_parquet(buffer2, index=False)
        buffer2.seek(0)

        files_to_upload = [
            (
                None,
                bucket_client.raw_data_bucket,
                "integration/file1.parquet",
                None,
                buffer1,
            ),
            (
                None,
                bucket_client.raw_data_bucket,
                "integration/file2.parquet",
                None,
                buffer2,
            ),
        ]

        upload_status = bucket_client.batch_upload(files=files_to_upload)

        files_to_download = [
            (
                None,
                bucket_client.raw_data_bucket,
                "integration/file1.parquet",
                None,
            ),
            (
                None,
                bucket_client.raw_data_bucket,
                "integration/file2.parquet",
                None,
            ),
        ]

        downloaded_data = bucket_client.batch_download(files=files_to_download)

        assert upload_status["integration/file1.parquet"] is True
        assert upload_status["integration/file2.parquet"] is True
        assert downloaded_data["integration/file1.parquet"]["status"] is True
        assert downloaded_data["integration/file1.parquet"]["data"] is not None
        assert downloaded_data["integration/file2.parquet"]["status"] is True
        assert downloaded_data["integration/file2.parquet"]["data"] is not None

        # Cleanup
        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )


@pytest.mark.integration
class TestBucketClientOperations:
    """Test general operations"""

    def test_no_objects_in_bucket(self, bucket_client):
        """Test that there are no objects at the beginning"""

        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)

        assert len(objects) == 0

    def test_list_objects(self, bucket_client):
        """Test that the correct list is provided by list_objects"""

        df1 = pd.DataFrame(
            {
                "driver": ["Charles", "Lando"],
                "number": [16, 4],
                "team": ["Scuderia Ferrari", "McLaren F1"],
            }
        )
        buffer1 = io.BytesIO()
        df1.to_parquet(buffer1, index=False)
        buffer1.seek(0)

        _ = bucket_client.upload_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/df1.parquet",
            file_obj=buffer1,
        )

        df2 = pd.DataFrame(
            {
                "driver": ["Max", "Lewis"],
                "number": [33, 44],
                "team": ["Redbull Racing", "Scuderia Ferrari"],
            }
        )
        buffer2 = io.BytesIO()
        df2.to_parquet(buffer2, index=False)
        buffer2.seek(0)

        _ = bucket_client.upload_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/df2.parquet",
            file_obj=buffer2,
        )

        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)

        assert len(objects) == 2
        assert "integration/df1.parquet" in objects
        assert "integration/df2.parquet" in objects

        # Cleanup
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )

    def test_list_objects_with_prefix(self, bucket_client):
        """Test objects are filtered as per prefix"""

        df1 = pd.DataFrame(
            {
                "driver": ["Charles", "Lando"],
                "number": [16, 4],
                "team": ["Scuderia Ferrari", "McLaren F1"],
            }
        )
        buffer1 = io.BytesIO()
        df1.to_parquet(buffer1, index=False)
        buffer1.seek(0)

        _ = bucket_client.upload_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/folder1/df1.parquet",
            file_obj=buffer1,
        )

        df2 = pd.DataFrame(
            {
                "driver": ["Max", "Lewis"],
                "number": [33, 44],
                "team": ["Redbull Racing", "Scuderia Ferrari"],
            }
        )
        buffer2 = io.BytesIO()
        df2.to_parquet(buffer2, index=False)
        buffer2.seek(0)

        _ = bucket_client.upload_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/folder2/df2.parquet",
            file_obj=buffer2,
        )

        objects = bucket_client.list_objects(
            bucket=bucket_client.raw_data_bucket,
            prefix="integration/folder1",
        )

        assert len(objects) == 1
        assert "integration/folder1/df1.parquet" in objects
        assert "integration/folder2/df2.parquet" not in objects

        # Cleanup
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )

    def test_file_exists(self, bucket_client):
        """Test the file exists method"""

        df1 = pd.DataFrame(
            {
                "driver": ["Charles", "Lando"],
                "number": [16, 4],
                "team": ["Scuderia Ferrari", "McLaren F1"],
            }
        )
        buffer1 = io.BytesIO()
        df1.to_parquet(buffer1, index=False)
        buffer1.seek(0)

        _ = bucket_client.upload_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/folder1/df1.parquet",
            file_obj=buffer1,
        )

        file_check_1 = bucket_client.file_exists(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/folder1/df1.parquet",
        )

        file_check_2 = bucket_client.file_exists(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/folder2/df2.parquet",
        )

        assert file_check_1 is True
        assert file_check_2 is False

        # Cleanup
        objects = bucket_client.list_objects(bucket=bucket_client.raw_data_bucket)
        for object_key in objects:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )

    def test_delete_single_object(self, bucket_client):
        """Test deletion of single object"""

        df1 = pd.DataFrame(
            {
                "driver": ["Charles", "Lando"],
                "number": [16, 4],
                "team": ["Scuderia Ferrari", "McLaren F1"],
            }
        )
        buffer1 = io.BytesIO()
        df1.to_parquet(buffer1, index=False)
        buffer1.seek(0)

        _ = bucket_client.upload_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/folder1/df1.parquet",
            file_obj=buffer1,
        )

        df2 = pd.DataFrame(
            {
                "driver": ["Max", "Lewis"],
                "number": [33, 44],
                "team": ["Redbull Racing", "Scuderia Ferrari"],
            }
        )
        buffer2 = io.BytesIO()
        df2.to_parquet(buffer2, index=False)
        buffer2.seek(0)

        _ = bucket_client.upload_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/folder2/df2.parquet",
            file_obj=buffer2,
        )

        objects_list_1 = bucket_client.list_objects(
            bucket=bucket_client.raw_data_bucket
        )

        delete_status = bucket_client.delete_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/folder2/df2.parquet",
        )

        objects_list_2 = bucket_client.list_objects(
            bucket=bucket_client.raw_data_bucket
        )

        assert len(objects_list_1) == 2
        assert delete_status is True
        assert len(objects_list_2) == 1

        # Cleanup
        for object_key in objects_list_2:
            bucket_client.delete_file(
                bucket_name=bucket_client.raw_data_bucket,
                object_key=object_key,
            )

    def test_delete_nonexistent_file(self, bucket_client):
        """Test deleting a non-existent file/object"""

        delete_status = bucket_client.delete_file(
            bucket_name=bucket_client.raw_data_bucket,
            object_key="integration/nonexistent.csv",
        )

        assert (
            delete_status is True
        )  # boto3 doesn't raise an error while trying to delete a non-existent object
