"""Tests for the GCS download/upload mixed functionality.

Reflects closer to actual use case using upload/download methods.
"""

import contextlib
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
    bucket_name = "test-bucket-mixed"
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


def test_upload_download_roundtrip_consistency(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test upload and download consistency for all supported formats using public methods."""
    # Setup: Base DataFrame
    test_data = pd.DataFrame(
        {
            "int_col": [1, 2, 3],
            "str_col": ["a", "b", "c"],
            "float_col": [1.1, 2.2, 3.3],
        },
    )

    files = ["test.parquet", "test.csv", "test.xlsx"]

    for filename in files:
        # Test: Upload using public upload method
        test_uri = f"gs://{test_bucket.name}/{filename}"
        gcs_file_io.upload((test_data, test_uri))

        # Test: Download using public download method
        result = gcs_file_io.download(test_uri)

        # Verify: Check data consistency
        assert filename in result
        downloaded_data = result[filename]
        assert isinstance(downloaded_data, pd.DataFrame)

        # Data should be identical for all formats now
        pd.testing.assert_frame_equal(downloaded_data, test_data)


def test_data_processing_workflow(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
    temp_dir: Path,
) -> None:
    """Test a realistic data processing workflow: local -> GCS -> process -> GCS -> local."""
    initial_data = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "score": [85.5, 92.0, 78.5, 95.0, 88.5],
            "category": ["A", "B", "A", "C", "B"],
        },
    )

    # Setup: Save to local CSV file
    input_file = temp_dir / "raw_data.csv"
    initial_data.to_csv(input_file, index=False)

    # Test: Upload raw data to GCS
    raw_uri = f"gs://{test_bucket.name}/raw/data.csv"
    gcs_file_io.upload((input_file, raw_uri))

    # Test: Download and process data
    downloaded_data = gcs_file_io.download(raw_uri)
    raw_df = downloaded_data["raw/data.csv"]

    # Calculate average score by category
    processed_df = raw_df.groupby("category")["score"].mean().reset_index()
    processed_df.columns = ["category", "avg_score"]

    # Test: Upload processed data back to GCS
    processed_uri = f"gs://{test_bucket.name}/processed/summary.parquet"
    gcs_file_io.upload((processed_df, processed_uri))

    # Test: Download final results and save locally
    final_results = gcs_file_io.download(processed_uri)
    final_df = final_results["processed/summary.parquet"]

    # Save final results to local file
    output_file = temp_dir / "final_summary.parquet"
    gcs_file_io.download((processed_uri, output_file))

    # Verify: Check the workflow
    assert output_file.exists()
    local_final = pd.read_parquet(output_file)
    pd.testing.assert_frame_equal(local_final, final_df)

    # Verify: Check processed data is correct
    expected_summary = pd.DataFrame(
        {
            "category": ["A", "B", "C"],
            "avg_score": [82.0, 90.25, 95.0],
        },
    )
    pd.testing.assert_frame_equal(final_df.sort_values("category").reset_index(drop=True), expected_summary)


def test_batch_file_management(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
    temp_dir: Path,
) -> None:
    """Test batch upload/download of mixed file types (files + objects)."""
    # Setup: Local Files
    config_file = temp_dir / "config.txt"
    config_file.write_text("debug=true\nversion=1.0")

    readme_file = temp_dir / "readme.md"
    readme_file.write_text("# Project Documentation\nThis is a test project.")

    # Setup: Local Objects
    users_df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        },
    )

    metrics_df = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "visits": [100, 150, 120],
        },
    )

    # Test: Batch Upload Mix of Files and Objects
    upload_batch = [
        (config_file, f"gs://{test_bucket.name}/config/settings.txt"),
        (readme_file, f"gs://{test_bucket.name}/docs/README.md"),
        (users_df, f"gs://{test_bucket.name}/data/users.csv"),
        (metrics_df, f"gs://{test_bucket.name}/data/metrics.parquet"),
    ]
    gcs_file_io.upload(upload_batch)

    # Test: Batch Download Mix of Files and Objects
    download_batch = [
        (f"gs://{test_bucket.name}/config/settings.txt", temp_dir / "downloaded_config.txt"),
        (f"gs://{test_bucket.name}/docs/README.md", temp_dir / "downloaded_readme.md"),
        f"gs://{test_bucket.name}/data/users.csv",
        f"gs://{test_bucket.name}/data/metrics.parquet",
    ]
    downloaded_objects = gcs_file_io.download(download_batch)

    # Verify: Check file downloads
    assert (temp_dir / "downloaded_config.txt").read_text() == "debug=true\nversion=1.0"
    assert (temp_dir / "downloaded_readme.md").read_text() == "# Project Documentation\nThis is a test project."

    # Verify: Check object downloads
    assert "data/users.csv" in downloaded_objects
    assert "data/metrics.parquet" in downloaded_objects

    downloaded_users = downloaded_objects["data/users.csv"]
    downloaded_metrics = downloaded_objects["data/metrics.parquet"]

    pd.testing.assert_frame_equal(downloaded_users, users_df)
    pd.testing.assert_frame_equal(downloaded_metrics, metrics_df)


def test_data_format_conversion_workflow(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test converting between different data formats via GCS."""
    # Setup: Base DataFrame
    source_data = pd.DataFrame(
        {
            "product": ["Widget A", "Widget B", "Widget C"],
            "price": [29.99, 45.50, 12.75],
            "in_stock": [True, False, True],
        },
    )

    # Test: Upload as CSV
    csv_uri = f"gs://{test_bucket.name}/products.csv"
    gcs_file_io.upload((source_data, csv_uri))

    # Test: Download CSV and re-upload as different formats
    csv_data = gcs_file_io.download(csv_uri)
    products_df = csv_data["products.csv"]

    # Test: Convert to multiple formats
    conversion_uploads = [
        (products_df, f"gs://{test_bucket.name}/products.parquet"),
        (products_df, f"gs://{test_bucket.name}/products.xlsx"),
    ]
    gcs_file_io.upload(conversion_uploads)

    # Test: Download all formats and verify consistency
    all_formats = [
        f"gs://{test_bucket.name}/products.csv",
        f"gs://{test_bucket.name}/products.parquet",
        f"gs://{test_bucket.name}/products.xlsx",
    ]
    all_data = gcs_file_io.download(all_formats)

    # Verify: All formats should contain the same data
    csv_result = all_data["products.csv"]
    parquet_result = all_data["products.parquet"]
    excel_result = all_data["products.xlsx"]

    pd.testing.assert_frame_equal(csv_result, source_data)
    pd.testing.assert_frame_equal(parquet_result, source_data)
    pd.testing.assert_frame_equal(excel_result, source_data)


def test_wildcard_batch_operations(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
) -> None:
    """Test wildcard downloads after batch uploads."""
    # Setup: Upload multiple files with consistent naming
    datasets = {
        "sales_2024_01": pd.DataFrame({"amount": [100, 200], "region": ["US", "EU"]}),
        "sales_2024_02": pd.DataFrame({"amount": [150, 250], "region": ["US", "EU"]}),
        "sales_2024_03": pd.DataFrame({"amount": [120, 220], "region": ["US", "EU"]}),
        "inventory_2024_01": pd.DataFrame({"items": [50, 30], "location": ["A", "B"]}),
    }

    # Test: Batch upload all datasets
    upload_batch = []
    for name, data in datasets.items():
        uri = f"gs://{test_bucket.name}/reports/{name}.csv"
        upload_batch.append((data, uri))
    gcs_file_io.upload(upload_batch)

    # Test: Use wildcard to download only sales data
    sales_pattern = f"gs://{test_bucket.name}/reports/sales_*.csv"
    sales_data = gcs_file_io.download(sales_pattern)

    # Verify: Check only sales files were downloaded
    assert len(sales_data) == 3
    assert "reports/sales_2024_01.csv" in sales_data
    assert "reports/sales_2024_02.csv" in sales_data
    assert "reports/sales_2024_03.csv" in sales_data
    assert "reports/inventory_2024_01.csv" not in sales_data

    # Verify: Check content
    for name in ["sales_2024_01", "sales_2024_02", "sales_2024_03"]:
        filename = f"reports/{name}.csv"
        expected_data = datasets[name]
        actual_data = sales_data[filename]
        pd.testing.assert_frame_equal(actual_data, expected_data)


def test_error_handling_mixed_operations(
    gcs_file_io: GCSFileIO,
    test_bucket: storage.Bucket,
    temp_dir: Path,
) -> None:
    """Test error handling in mixed upload/download scenarios."""
    # Setup: Create non-existent file
    invalid_file = temp_dir / "nonexistent.txt"  # This file doesn't exist

    # Test: Try to upload non-existent file (should fail)
    with pytest.raises((FileNotFoundError, OSError)):
        gcs_file_io.upload((invalid_file, f"gs://{test_bucket.name}/invalid.txt"))

    # Test: Upload valid data, then test successful and failed downloads
    valid_data = pd.DataFrame({"test": [1, 2, 3]})
    gcs_file_io.upload((valid_data, f"gs://{test_bucket.name}/exists.csv"))

    # Test: Download existing file
    result = gcs_file_io.download(f"gs://{test_bucket.name}/exists.csv")

    # Verify: Confirm file exists
    assert "exists.csv" in result
