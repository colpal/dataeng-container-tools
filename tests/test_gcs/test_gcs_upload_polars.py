"""Tests for the GCS upload functionality with Polars engine."""

import io
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
from google.cloud import storage

from dataeng_container_tools.modules.gcs import GCSFileIO

try:
    import polars as pl
except ImportError:
    pytest.skip("Polars not installed", allow_module_level=True)


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
    bucket_name = "test-bucket-upload-polars"
    bucket = gcs_client.bucket(bucket_name)

    # Create bucket if it doesn't exist
    if not bucket.exists():
        bucket.create()

    yield bucket

    # Cleanup: delete all blobs in bucket
    blobs = list(bucket.list_blobs())
    for blob in blobs:
        blob.delete()
    bucket.delete()


@pytest.fixture
def gcs_file_io() -> GCSFileIO:
    """Create a GCSFileIO instance with Polars engine."""
    return GCSFileIO(local=True, engine="polars")


def test_upload_object_dataframe_parquet(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test uploading Polars DataFrame object as Parquet file."""
    # Setup: Create test DataFrame
    test_data = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Test: Upload the DataFrame
    blob_name = "test.parquet"
    test_uri = f"gs://{test_bucket.name}/{blob_name}"

    gcs_file_io.upload([(test_data, test_uri)])

    # Verify: Check the uploaded content
    blob = test_bucket.blob(blob_name)
    assert blob.exists()

    # Download and compare
    parquet_bytes = blob.download_as_bytes()
    downloaded_data = pl.read_parquet(io.BytesIO(parquet_bytes))
    assert downloaded_data.equals(test_data)


def test_upload_object_dataframe_csv(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test uploading Polars DataFrame object as CSV file."""
    # Setup: Create test DataFrame
    test_data = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Test: Upload the DataFrame
    blob_name = "test.csv"
    test_uri = f"gs://{test_bucket.name}/{blob_name}"

    gcs_file_io.upload([(test_data, test_uri)])

    # Verify: Check the uploaded content
    blob = test_bucket.blob(blob_name)
    assert blob.exists()

    csv_content = blob.download_as_bytes()
    downloaded_data = pl.read_csv(io.BytesIO(csv_content))
    assert downloaded_data.equals(test_data)


def test_upload_object_dataframe_xlsx(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test uploading Polars DataFrame object as Excel file."""
    # Setup: Create test DataFrame
    test_data = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Test: Upload the DataFrame
    blob_name = "test.xlsx"
    test_uri = f"gs://{test_bucket.name}/{blob_name}"

    gcs_file_io.upload([(test_data, test_uri)])

    # Verify: Check the uploaded content
    blob = test_bucket.blob(blob_name)
    assert blob.exists()

    excel_bytes = blob.download_as_bytes()
    downloaded_data = pl.read_excel(io.BytesIO(excel_bytes))
    assert downloaded_data.equals(test_data)


def test_upload_object_multiple(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test uploading multiple Polars objects."""
    # Setup: Create test objects
    test_data1 = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
    test_data2 = pl.DataFrame({"x": [5, 6], "y": [7, 8]})

    src_dst = [
        (test_data1, f"gs://{test_bucket.name}/data1.parquet"),
        (test_data2, f"gs://{test_bucket.name}/data2.csv"),
    ]

    # Test: Upload the objects
    gcs_file_io.upload(src_dst)

    # Verify: Check all uploaded content
    # Check parquet file
    blob1 = test_bucket.blob("data1.parquet")
    assert blob1.exists()
    downloaded_data1 = pl.read_parquet(io.BytesIO(blob1.download_as_bytes()))
    assert downloaded_data1.equals(test_data1)

    # Check CSV file
    blob2 = test_bucket.blob("data2.csv")
    assert blob2.exists()
    downloaded_data2 = pl.read_csv(io.BytesIO(blob2.download_as_bytes()))
    assert downloaded_data2.equals(test_data2)


def test_upload_mixed_types(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
    temp_dir: Path,
) -> None:
    """Test uploading with mixed file and Polars object types."""
    # Setup: Create test file and object
    test_file_content = "File content"
    test_file = temp_dir / "test.txt"
    test_file.write_text(test_file_content)

    test_data = pl.DataFrame({"col": [1, 2, 3]})

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
    downloaded_data = pl.read_parquet(io.BytesIO(blob2.download_as_bytes()))
    assert downloaded_data.equals(test_data)
