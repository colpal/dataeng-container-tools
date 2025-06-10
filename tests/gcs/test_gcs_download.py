"""Tests for the GCS download functionality."""

import contextlib
import io
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
    bucket_name = "test-bucket-download"
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


def test_download_invalid_input_type(gcs_file_io: GCSFileIO) -> None:
    """Test download with invalid input type raises TypeError."""
    with pytest.raises(TypeError, match="src_dst must be a sequence of tuples, string, or list of strings"):
        gcs_file_io.download(123)  # type: ignore[arg-type]


def test_download_to_file_single(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
    temp_dir: Path,
) -> None:
    """Test downloading a single file to local path."""
    # Setup: Upload a test file to GCS
    test_content = "Hello, World!"
    blob_name = "test.txt"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(test_content)

    # Test: Download the file
    test_uri = f"gs://{test_bucket.name}/{blob_name}"
    test_file = temp_dir / "downloaded.txt"

    gcs_file_io.download_to_file([(test_uri, test_file)])

    # Verify: Check the downloaded content
    assert test_file.exists()
    assert test_file.read_text() == test_content


def test_download_to_file_multiple(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
    temp_dir: Path,
) -> None:
    """Test downloading multiple files to local paths."""
    # Setup: Upload test files to GCS
    test_files = {
        "file1.txt": "Content of file 1",
        "file2.txt": "Content of file 2",
    }

    for blob_name, content in test_files.items():
        blob = test_bucket.blob(blob_name)
        blob.upload_from_string(content)

    # Test: Download the files
    src_dst = [
        (f"gs://{test_bucket.name}/file1.txt", temp_dir / "downloaded1.txt"),
        (f"gs://{test_bucket.name}/file2.txt", temp_dir / "downloaded2.txt"),
    ]

    gcs_file_io.download_to_file(src_dst)

    # Verify: Check the downloaded content
    assert (temp_dir / "downloaded1.txt").read_text() == "Content of file 1"
    assert (temp_dir / "downloaded2.txt").read_text() == "Content of file 2"


def test_download_to_object_parquet(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test downloading Parquet file to DataFrame object."""
    # Setup: Create and upload a test parquet file
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Create parquet bytes
    parquet_buffer = io.BytesIO()
    test_data.to_parquet(parquet_buffer, index=False)
    parquet_bytes = parquet_buffer.getvalue()

    blob_name = "test.parquet"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(parquet_bytes)

    # Test: Download the file as object
    test_uri = f"gs://{test_bucket.name}/{blob_name}"
    result = gcs_file_io.download_to_object(test_uri)

    # Verify: Check the result
    assert blob_name in result
    result_data = result[blob_name]
    assert isinstance(result_data, pd.DataFrame)
    pd.testing.assert_frame_equal(result_data, test_data)


def test_download_to_object_csv(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test downloading CSV file to DataFrame object."""
    # Setup: Create and upload a test CSV file
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    csv_content = test_data.to_csv(index=False)

    blob_name = "test.csv"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(csv_content)

    # Test: Download the file as object
    test_uri = f"gs://{test_bucket.name}/{blob_name}"
    result = gcs_file_io.download_to_object(test_uri)

    # Verify: Check the result
    assert blob_name in result
    result_data = result[blob_name]
    assert isinstance(result_data, pd.DataFrame)
    pd.testing.assert_frame_equal(result_data, test_data)


def test_download_to_object_xlsx(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test downloading Excel file to DataFrame object."""
    # Setup: Create and upload a test Excel file
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    # Create Excel bytes
    excel_buffer = io.BytesIO()
    test_data.to_excel(excel_buffer, index=False)
    excel_bytes = excel_buffer.getvalue()

    blob_name = "test.xlsx"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(excel_bytes)

    # Test: Download the file as object
    test_uri = f"gs://{test_bucket.name}/{blob_name}"
    result = gcs_file_io.download_to_object(test_uri)

    # Verify: Check the result
    assert blob_name in result
    result_data = result[blob_name]
    assert isinstance(result_data, pd.DataFrame)
    pd.testing.assert_frame_equal(result_data, test_data)


def test_download_to_object_json(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test downloading JSON file to DataFrame object."""
    # Setup: Create and upload a test JSON file
    test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    json_content = test_data.to_json(orient="records")

    blob_name = "test.json"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(json_content)

    # Test: Download the file as object
    test_uri = f"gs://{test_bucket.name}/{blob_name}"
    result = gcs_file_io.download_to_object(test_uri)

    # Verify: Check the result
    assert blob_name in result
    result_data = result[blob_name]
    assert isinstance(result_data, pd.DataFrame)
    pd.testing.assert_frame_equal(result_data.reset_index(drop=True), test_data.reset_index(drop=True))


def test_download_mixed_extensions(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test downloading files with various known extensions."""
    # Setup: Create test data
    test_data = pd.DataFrame({"col1": [1, 2], "col2": ["x", "y"]})

    # Upload different file types
    test_files = {}

    # Parquet
    parquet_buffer = io.BytesIO()
    test_data.to_parquet(parquet_buffer, index=False)
    blob = test_bucket.blob("data.parquet")
    blob.upload_from_string(parquet_buffer.getvalue())
    test_files["data.parquet"] = test_data

    # CSV
    csv_content = test_data.to_csv(index=False)
    blob = test_bucket.blob("data.csv")
    blob.upload_from_string(csv_content)
    test_files["data.csv"] = test_data

    # Excel
    excel_buffer = io.BytesIO()
    test_data.to_excel(excel_buffer, index=False)
    blob = test_bucket.blob("data.xlsx")
    blob.upload_from_string(excel_buffer.getvalue())
    test_files["data.xlsx"] = test_data

    # JSON
    json_content = test_data.to_json(orient="records")
    blob = test_bucket.blob("data.json")
    blob.upload_from_string(json_content)
    test_files["data.json"] = test_data

    # Test: Download all files
    uris = [f"gs://{test_bucket.name}/{filename}" for filename in test_files]
    result = gcs_file_io.download_to_object(uris)

    # Verify: Check all results
    assert len(result) == 4
    for filename, expected_data in test_files.items():
        assert filename in result
        result_data = result[filename]
        assert isinstance(result_data, pd.DataFrame)
        pd.testing.assert_frame_equal(result_data.reset_index(drop=True), expected_data.reset_index(drop=True))


def test_download_unsupported_extension_returns_bytesio(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test that files with unsupported extensions return BytesIO."""
    # Setup: Upload files with various unsupported extensions
    test_content = b"some binary content"
    unsupported_files = ["data.bin", "data.dat", "data.log", "data.txt"]

    for filename in unsupported_files:
        blob = test_bucket.blob(filename)
        blob.upload_from_string(test_content)

    # Test: Download all files
    uris = [f"gs://{test_bucket.name}/{filename}" for filename in unsupported_files]
    result = gcs_file_io.download_to_object(uris)

    # Verify: All should return BytesIO
    assert len(result) == len(unsupported_files)
    for filename in unsupported_files:
        assert filename in result
        result_data = result[filename]
        assert isinstance(result_data, io.BytesIO)
        assert result_data.getvalue() == test_content


def test_download_with_wildcard(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test downloading with wildcard patterns matching different file types."""
    # Setup: Upload files with various extensions and patterns
    test_data = pd.DataFrame({"value": [1, 2, 3]})

    # Upload CSV files (for *.csv pattern)
    csv_files = {
        "data1.csv": pd.DataFrame({"a": [1, 2], "b": [3, 4]}),
        "data2.csv": pd.DataFrame({"x": [5, 6], "y": [7, 8]}),
    }

    for filename, content in csv_files.items():
        blob = test_bucket.blob(filename)
        csv_content = content.to_csv(index=False)
        blob.upload_from_string(csv_content)

    # Upload files with same prefix but different extensions (for report.* pattern)
    csv_content = test_data.to_csv(index=False)
    blob = test_bucket.blob("report.csv")
    blob.upload_from_string(csv_content)

    parquet_buffer = io.BytesIO()
    test_data.to_parquet(parquet_buffer, index=False)
    blob = test_bucket.blob("report.parquet")
    blob.upload_from_string(parquet_buffer.getvalue())

    # Upload binary and text files
    binary_content = b"binary report data"
    blob = test_bucket.blob("report.bin")
    blob.upload_from_string(binary_content)

    blob = test_bucket.blob("other.txt")
    blob.upload_from_string("not a csv")

    # Test: Download only CSV files with *.csv pattern
    csv_pattern_uri = f"gs://{test_bucket.name}/*.csv"
    csv_result = gcs_file_io.download_to_object(csv_pattern_uri)

    # Verify: Only CSV files should be downloaded
    assert len(csv_result) == 3  # data1.csv, data2.csv, report.csv
    assert "data1.csv" in csv_result
    assert "data2.csv" in csv_result
    assert "report.csv" in csv_result
    assert "other.txt" not in csv_result
    assert "report.bin" not in csv_result

    # Verify CSV content
    for filename, expected_data in csv_files.items():
        result_data = csv_result[filename]
        assert isinstance(result_data, pd.DataFrame)
        pd.testing.assert_frame_equal(result_data, expected_data)

    # Test: Download files with mixed types using report.* pattern
    report_pattern_uri = f"gs://{test_bucket.name}/report.*"
    report_result = gcs_file_io.download_to_object(report_pattern_uri)

    # Verify: Mixed return types based on extension
    assert len(report_result) == 3  # report.csv, report.parquet, report.bin

    # Structured data should be DataFrames
    assert isinstance(report_result["report.csv"], pd.DataFrame)
    assert isinstance(report_result["report.parquet"], pd.DataFrame)
    pd.testing.assert_frame_equal(report_result["report.csv"], test_data)
    pd.testing.assert_frame_equal(report_result["report.parquet"], test_data)

    # Binary data should be BytesIO
    assert isinstance(report_result["report.bin"], io.BytesIO)
    assert report_result["report.bin"].getvalue() == binary_content

def test_download_with_dtype_parameter(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test downloading with explicit dtype parameter."""
    # Setup: Create CSV with mixed types
    test_data = pd.DataFrame(
        {
            "id": ["001", "002", "003"],
            "value": [1.5, 2.0, 3.5],
        },
    )
    csv_content = test_data.to_csv(index=False)

    blob_name = "typed_data.csv"
    blob = test_bucket.blob(blob_name)
    blob.upload_from_string(csv_content)

    # Test: Download with explicit dtype
    test_uri = f"gs://{test_bucket.name}/{blob_name}"
    dtype_spec = {"id": str, "value": float}
    result = gcs_file_io.download_to_object(test_uri, dtype=dtype_spec)

    # Verify: Check types are preserved
    assert blob_name in result
    result_data = result[blob_name]
    assert isinstance(result_data, pd.DataFrame)
    assert result_data["id"].dtype == "object"  # String type
    assert result_data["value"].dtype == "float64"
    pd.testing.assert_frame_equal(result_data, test_data)
