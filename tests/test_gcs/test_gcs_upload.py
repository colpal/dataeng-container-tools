"""Tests for the GCS upload functionality."""

import contextlib
import io
import json
import tempfile
from collections.abc import Generator
from pathlib import Path

import pandas as pd
import pytest
from google.cloud import storage

from dataeng_container_tools.modules.gcs import GCSFileIO


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def gcs_client() -> storage.Client:
    """Create a GCS client for the emulator."""
    return storage.Client()


@pytest.fixture
def test_bucket(gcs_client: storage.Client) -> Generator[storage.Bucket, None, None]:
    """Create a test bucket."""
    bucket_name = "test-bucket-upload"
    bucket = gcs_client.bucket(bucket_name)

    # Create bucket if it doesn't exist
    with contextlib.suppress(Exception):
        bucket.create()

    yield bucket

    # Cleanup: delete all blobs in bucket
    with contextlib.suppress(Exception):
        blobs = list(bucket.list_blobs())
        for blob in blobs:
            blob.delete()
        bucket.delete()


@pytest.fixture
def gcs_file_io() -> GCSFileIO:
    """Create a GCSFileIO instance."""
    return GCSFileIO(local=True)


def test_gcs_file_io_init_local() -> None:
    """Test GCSFileIO initialization in local mode."""
    gcs_io = GCSFileIO(local=True)
    assert gcs_io.local is True
    assert gcs_io.client is not None


def test_upload_file_single(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
    temp_dir: Path,
) -> None:
    """Test uploading a single file from local path."""
    # Setup: Create a test file
    test_content = "Hello, World!"
    test_file = temp_dir / "test.txt"
    test_file.write_text(test_content)

    # Test: Upload the file
    blob_name = "uploaded.txt"
    test_uri = f"gs://{test_bucket.name}/{blob_name}"

    gcs_file_io.upload_file([(test_file, test_uri)])

    # Verify: Check the uploaded content
    blob = test_bucket.blob(blob_name)
    assert blob.exists()
    assert blob.download_as_text() == test_content


def test_upload_file_multiple(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
    temp_dir: Path,
) -> None:
    """Test uploading multiple files from local paths."""
    # Setup: Create test files
    test_files = {
        "file1.txt": "Content of file 1",
        "file2.txt": "Content of file 2",
    }

    src_dst = []
    for filename, content in test_files.items():
        local_file = temp_dir / filename
        local_file.write_text(content)
        test_uri = f"gs://{test_bucket.name}/uploaded_{filename}"
        src_dst.append((local_file, test_uri))

    # Test: Upload the files
    gcs_file_io.upload_file(src_dst)

    # Verify: Check the uploaded content
    for filename, content in test_files.items():
        blob = test_bucket.blob(f"uploaded_{filename}")
        assert blob.exists()
        assert blob.download_as_text() == content


def test_upload_object_dataframe_parquet(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test uploading DataFrame object as Parquet file."""
    # Setup: Create test DataFrame
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Test: Upload the DataFrame
    blob_name = "test.parquet"
    test_uri = f"gs://{test_bucket.name}/{blob_name}"

    gcs_file_io.upload_object([(test_data, test_uri)])

    # Verify: Check the uploaded content
    blob = test_bucket.blob(blob_name)
    assert blob.exists()

    # Download and compare
    parquet_bytes = blob.download_as_bytes()
    downloaded_data = pd.read_parquet(io.BytesIO(parquet_bytes))
    pd.testing.assert_frame_equal(downloaded_data, test_data)


def test_upload_object_dataframe_csv(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test uploading DataFrame object as CSV file."""
    # Setup: Create test DataFrame
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Test: Upload the DataFrame
    blob_name = "test.csv"
    test_uri = f"gs://{test_bucket.name}/{blob_name}"

    gcs_file_io.upload_object([(test_data, test_uri)])

    # Verify: Check the uploaded content
    blob = test_bucket.blob(blob_name)
    assert blob.exists()

    csv_content = blob.download_as_text()
    downloaded_data = pd.read_csv(io.StringIO(csv_content))
    pd.testing.assert_frame_equal(downloaded_data, test_data)


def test_upload_object_dataframe_xlsx(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test uploading DataFrame object as Excel file."""
    # Setup: Create test DataFrame
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Test: Upload the DataFrame
    blob_name = "test.xlsx"
    test_uri = f"gs://{test_bucket.name}/{blob_name}"

    gcs_file_io.upload_object([(test_data, test_uri)])

    # Verify: Check the uploaded content
    blob = test_bucket.blob(blob_name)
    assert blob.exists()

    excel_bytes = blob.download_as_bytes()
    downloaded_data = pd.read_excel(io.BytesIO(excel_bytes))
    pd.testing.assert_frame_equal(downloaded_data, test_data)


def test_upload_object_string_json(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test uploading string object as JSON file."""
    # Setup: Create test string data (JSON string)
    test_data = '{"key": "value", "number": 42}'

    # Test: Upload the string as JSON
    blob_name = "test.json"
    test_uri = f"gs://{test_bucket.name}/{blob_name}"

    gcs_file_io.upload_object([(test_data, test_uri)])

    # Verify: Check the uploaded content
    blob = test_bucket.blob(blob_name)
    assert blob.exists()

    json_content = blob.download_as_text()

    uploaded_as_json = json.loads(json_content)
    assert uploaded_as_json == test_data


def test_upload_object_unsupported_type(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test uploading unsupported object types fails."""
    # Test: Try to upload unsupported objects
    unsupported_objects = [
        ({"key": "value"}, f"gs://{test_bucket.name}/dict.json"),  # Dict without .json processing
        ([1, 2, 3], f"gs://{test_bucket.name}/list.csv"),  # List
        ("string", f"gs://{test_bucket.name}/string.txt"),  # String
        (123, f"gs://{test_bucket.name}/number.csv"),  # Number
    ]

    for obj, uri in unsupported_objects:
        with pytest.raises((ValueError, TypeError, AttributeError)):
            gcs_file_io.upload_object([(obj, uri)])


def test_upload_object_multiple(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test uploading multiple objects."""
    # Setup: Create test objects
    test_data1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    test_data2 = pd.DataFrame({"x": [5, 6], "y": [7, 8]})

    src_dst = [
        (test_data1, f"gs://{test_bucket.name}/data1.parquet"),
        (test_data2, f"gs://{test_bucket.name}/data2.csv"),
    ]

    # Test: Upload the objects
    gcs_file_io.upload_object(src_dst)

    # Verify: Check all uploaded content
    # Check parquet file
    blob1 = test_bucket.blob("data1.parquet")
    assert blob1.exists()
    downloaded_data1 = pd.read_parquet(io.BytesIO(blob1.download_as_bytes()))
    pd.testing.assert_frame_equal(downloaded_data1, test_data1)

    # Check CSV file
    blob2 = test_bucket.blob("data2.csv")
    assert blob2.exists()
    downloaded_data2 = pd.read_csv(io.StringIO(blob2.download_as_text()))
    pd.testing.assert_frame_equal(downloaded_data2, test_data2)


def test_upload_object_unknown_extension_fails(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test uploading DataFrame with unknown extension fails."""
    # Setup: Create test DataFrame
    test_data = pd.DataFrame({"col1": [1, 2, 3]})

    # Test: Upload with unsupported extension should fail
    test_uri = f"gs://{test_bucket.name}/test.unknown"

    with pytest.raises((ValueError, NotImplementedError)):
        gcs_file_io.upload_object([(test_data, test_uri)])


def test_upload_mixed_types(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
    temp_dir: Path,
) -> None:
    """Test uploading with mixed file and object types."""
    # Setup: Create test file and object
    test_file_content = "File content"
    test_file = temp_dir / "test.txt"
    test_file.write_text(test_file_content)

    test_data = pd.DataFrame({"col": [1, 2, 3]})

    # Test: Upload mixed types using the general upload method
    src_dst = [
        (test_file, f"gs://{test_bucket.name}/from_file.txt"),
        (test_data, f"gs://{test_bucket.name}/from_object.parquet"),
    ]

    gcs_file_io.upload(src_dst)

    # Verify: Check both uploads
    # Check file upload
    blob1 = test_bucket.blob("from_file.txt")
    assert blob1.exists()
    assert blob1.download_as_text() == test_file_content

    # Check object upload
    blob2 = test_bucket.blob("from_object.parquet")
    assert blob2.exists()
    downloaded_data = pd.read_parquet(io.BytesIO(blob2.download_as_bytes()))
    pd.testing.assert_frame_equal(downloaded_data, test_data)


def test_upload_invalid_input_type(gcs_file_io: GCSFileIO) -> None:
    """Test upload with invalid input type raises TypeError."""
    with pytest.raises(TypeError):
        gcs_file_io.upload(123)  # type: ignore[arg-type]


def test_upload_with_metadata(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
    temp_dir: Path,
) -> None:
    """Test uploading file with custom metadata."""
    # Setup: Create a test file
    test_content = "Hello with metadata!"
    test_file = temp_dir / "test_meta.txt"
    test_file.write_text(test_content)

    # Test: Upload with metadata
    blob_name = "test_with_metadata.txt"
    test_uri = f"gs://{test_bucket.name}/{blob_name}"
    metadata = {"custom-field": "test-value", "author": "pytest"}

    gcs_file_io.upload_file([(test_file, test_uri)], metadata=metadata)

    # Verify: Check the uploaded content and metadata
    blob = test_bucket.blob(blob_name)
    assert blob.exists()
    assert blob.download_as_text() == test_content

    # Reload to get fresh metadata
    blob.reload()
    assert blob.metadata is not None
    assert blob.metadata["custom-field"] == "test-value"
    assert blob.metadata["author"] == "pytest"
