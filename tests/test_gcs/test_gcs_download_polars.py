"""Tests for the GCS download functionality with Polars engine."""

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
    bucket_name = "test-bucket-download-polars"
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


def test_gcs_file_io_init_polars() -> None:
    """Test GCSFileIO initialization with Polars engine."""
    gcs_io = GCSFileIO(local=True, engine="polars")
    assert gcs_io.local is True
    assert gcs_io.engine == "polars"


def test_download_to_file_single(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
    temp_dir: Path,
) -> None:
    """Test downloading a single file to local path with Polars engine."""
    # Setup: Upload a test file to GCS
    test_content = "Hello, World!"
    blob_name = "test.txt"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(test_content)

    # Test: Download the file
    test_uri = f"gs://{test_bucket.name}/{blob_name}"
    test_file = temp_dir / "downloaded.txt"

    gcs_file_io.download([(test_uri, test_file)])

    # Verify: Check the downloaded content
    assert test_file.exists()
    assert test_file.read_text() == test_content


def test_download_to_object_parquet(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test downloading Parquet file to Polars DataFrame object."""
    # Setup: Create and upload a test parquet file
    test_data = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Create parquet bytes
    parquet_buffer = io.BytesIO()
    test_data.write_parquet(parquet_buffer)
    parquet_bytes = parquet_buffer.getvalue()

    blob_name = "test.parquet"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(parquet_bytes)

    # Test: Download the file as object
    test_uri = f"gs://{test_bucket.name}/{blob_name}"
    result = gcs_file_io.download(test_uri)

    # Verify: Check the result
    expected_key = f"{test_bucket.name}/{blob_name}"
    assert expected_key in result
    result_data = result[expected_key]
    assert isinstance(result_data, pl.DataFrame)
    assert result_data.equals(test_data)


def test_download_to_object_csv(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test downloading CSV file to Polars DataFrame object."""
    # Setup: Create and upload a test CSV file
    test_data = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    csv_buffer = io.BytesIO()
    test_data.write_csv(csv_buffer)
    csv_content = csv_buffer.getvalue()

    blob_name = "test.csv"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(csv_content)

    # Test: Download the file as object
    test_uri = f"gs://{test_bucket.name}/{blob_name}"
    result = gcs_file_io.download(test_uri)

    # Verify: Check the result
    expected_key = f"{test_bucket.name}/{blob_name}"
    assert expected_key in result
    result_data = result[expected_key]
    assert isinstance(result_data, pl.DataFrame)
    assert result_data.equals(test_data)


def test_download_to_object_xlsx(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test downloading Excel file to Polars DataFrame object."""
    # Setup: Create and upload a test Excel file
    test_data = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Create Excel bytes
    excel_buffer = io.BytesIO()
    test_data.write_excel(excel_buffer)
    excel_bytes = excel_buffer.getvalue()

    blob_name = "test.xlsx"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(excel_bytes)

    # Test: Download the file as object
    test_uri = f"gs://{test_bucket.name}/{blob_name}"
    result = gcs_file_io.download(test_uri)

    # Verify: Check the result
    expected_key = f"{test_bucket.name}/{blob_name}"
    assert expected_key in result
    result_data = result[expected_key]
    assert isinstance(result_data, pl.DataFrame)
    assert result_data.equals(test_data)


def test_download_to_object_json(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test downloading JSON file to Polars DataFrame object."""
    # Setup: Create and upload a test JSON file
    test_data = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    json_buffer = io.BytesIO()
    test_data.write_json(json_buffer)
    json_content = json_buffer.getvalue()

    blob_name = "test.json"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(json_content)

    # Test: Download the file as object
    test_uri = f"gs://{test_bucket.name}/{blob_name}"
    result = gcs_file_io.download(test_uri)

    # Verify: Check the result
    expected_key = f"{test_bucket.name}/{blob_name}"
    assert expected_key in result
    result_data = result[expected_key]
    assert isinstance(result_data, pl.DataFrame)
    assert result_data.equals(test_data)


def test_download_with_dtype_parameter(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test downloading with explicit dtype parameter (schema overrides for Polars)."""
    # Setup: Create CSV with mixed types
    test_data = pl.DataFrame(
        {
            "id": ["001", "002", "003"],
            "value": [1.5, 2.0, 3.5],
        },
    )
    csv_buffer = io.BytesIO()
    test_data.write_csv(csv_buffer)
    csv_content = csv_buffer.getvalue()

    blob_name = "typed_data.csv"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(csv_content)

    # Test: Download with explicit dtype (schema_overrides)
    test_uri = f"gs://{test_bucket.name}/{blob_name}"
    dtype_spec = {"id": pl.String, "value": pl.Float64}
    result = gcs_file_io.download(test_uri, dtype=dtype_spec)

    # Verify: Check types are preserved
    expected_key = f"{test_bucket.name}/{blob_name}"
    assert expected_key in result
    result_data = result[expected_key]
    assert isinstance(result_data, pl.DataFrame)
    assert result_data["id"].dtype == pl.String
    assert result_data["value"].dtype == pl.Float64
    assert result_data.equals(test_data)
