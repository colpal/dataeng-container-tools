"""Tests for the GCSUriUtils module."""

import pytest

from dataeng_container_tools.modules.gcs import GCSUriUtils


def test_gcs_uri_utils_prefix() -> None:
    """Test that the PREFIX constant is correctly defined."""
    assert GCSUriUtils.PREFIX == "gs://"


def test_normalize_uri_basic() -> None:
    """Test basic URI normalization without path components."""
    test_uri = "gs://my-bucket/file.txt"
    result = GCSUriUtils.normalize_uri(test_uri)
    assert result == "gs://my-bucket/file.txt"


def test_normalize_uri_with_path_components() -> None:
    """Test URI normalization with path components."""
    test_uri = "gs://my-bucket/folder/../file.txt"
    result = GCSUriUtils.normalize_uri(test_uri)
    assert result == "gs://my-bucket/file.txt"


def test_normalize_uri_complex_path() -> None:
    """Test URI normalization with complex path components."""
    test_uri = "gs://my-bucket/folder/subfolder/../../other/file.txt"
    result = GCSUriUtils.normalize_uri(test_uri)
    assert result == "gs://my-bucket/other/file.txt"


def test_normalize_uri_removes_and_adds_prefix() -> None:
    """Test that normalize_uri properly handles the gs:// prefix."""
    test_uri = "gs://my-bucket/./file.txt"
    result = GCSUriUtils.normalize_uri(test_uri)
    assert result.startswith("gs://")
    assert result == "gs://my-bucket/file.txt"


def test_get_components_valid_uri() -> None:
    """Test extracting components from a valid GCS URI."""
    test_uri = "gs://my-bucket/path/to/file.txt"
    bucket, file_path = GCSUriUtils.get_components(test_uri)

    assert bucket == "my-bucket"
    assert file_path == "path/to/file.txt"


def test_get_components_root_file() -> None:
    """Test extracting components from a URI with file at bucket root."""
    test_uri = "gs://my-bucket/file.txt"
    bucket, file_path = GCSUriUtils.get_components(test_uri)

    assert bucket == "my-bucket"
    assert file_path == "file.txt"


def test_get_components_nested_path() -> None:
    """Test extracting components from a URI with deeply nested path."""
    test_uri = "gs://my-bucket/folder/subfolder/another/file.parquet"
    bucket, file_path = GCSUriUtils.get_components(test_uri)

    assert bucket == "my-bucket"
    assert file_path == "folder/subfolder/another/file.parquet"


def test_get_components_invalid_uri_no_prefix() -> None:
    """Test that get_components raises ValueError for URI without gs:// prefix."""
    test_uri = "s3://my-bucket/file.txt"

    with pytest.raises(ValueError, match=r"Invalid GCS URI: 's3://my-bucket/file.txt'. URI must start with 'gs://'"):
        GCSUriUtils.get_components(test_uri)


def test_get_components_invalid_uri_missing_prefix() -> None:
    """Test that get_components raises ValueError for URI missing prefix entirely."""
    test_uri = "my-bucket/file.txt"

    with pytest.raises(ValueError, match=r"Invalid GCS URI: 'my-bucket/file.txt'. URI must start with 'gs://'"):
        GCSUriUtils.get_components(test_uri)


def test_get_components_empty_after_prefix() -> None:
    """Test get_components with URI that has only the prefix."""
    test_uri = "gs://"

    # This should not raise an error but will return empty strings
    bucket, file_path = GCSUriUtils.get_components(test_uri)
    assert bucket == ""
    assert file_path == ""


def test_get_components_bucket_only() -> None:
    """Test get_components with URI that has only bucket name."""
    test_uri = "gs://my-bucket/"

    bucket, file_path = GCSUriUtils.get_components(test_uri)
    assert bucket == "my-bucket"
    assert file_path == ""


def test_normalize_uri_edge_cases() -> None:
    """Test normalize_uri with various edge cases."""
    # Test with just bucket
    assert GCSUriUtils.normalize_uri("gs://bucket/") == "gs://bucket"

    # Test with multiple slashes
    assert GCSUriUtils.normalize_uri("gs://bucket//file.txt") == "gs://bucket/file.txt"

    # Test with current directory references
    assert GCSUriUtils.normalize_uri("gs://bucket/./file.txt") == "gs://bucket/file.txt"


def test_integration_normalize_then_get_components() -> None:
    """Test that normalize_uri and get_components work together correctly."""
    original_uri = "gs://my-bucket/folder/../subfolder/./file.txt"
    normalized_uri = GCSUriUtils.normalize_uri(original_uri)
    bucket, file_path = GCSUriUtils.get_components(normalized_uri)

    assert bucket == "my-bucket"
    assert file_path == "subfolder/file.txt"
    assert normalized_uri == "gs://my-bucket/subfolder/file.txt"
