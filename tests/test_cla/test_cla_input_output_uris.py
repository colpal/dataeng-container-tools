"""Tests for the cla input and output files functionality."""

import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture
def mock_file_path() -> Path:
    """Set up test fixtures."""
    return Path(__file__).parent / "mock_files" / "input_output.py"


def test_input_output_single_file(mock_file_path: Path) -> None:
    """Test that single input and output files work correctly."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--input_bucket_names",
            "input-bucket",
            "--input_paths",
            "input/path",
            "--input_filenames",
            "test.csv",
            "--output_bucket_names",
            "output-bucket",
            "--output_paths",
            "output/path",
            "--output_filenames",
            "result.csv",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Expected successful exit code. stderr: {result.stderr}"
    assert "Input URIs:" in result.stderr
    assert "Output URIs:" in result.stderr


def test_input_output_multiple_files(mock_file_path: Path) -> None:
    """Test that multiple input and output files work correctly."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--input_bucket_names",
            "bucket1",
            "bucket2",
            "--input_paths",
            "path1",
            "path2",
            "--input_filenames",
            "file1.csv",
            "file2.csv",
            "--output_bucket_names",
            "out-bucket1",
            "out-bucket2",
            "--output_paths",
            "out-path1",
            "out-path2",
            "--output_filenames",
            "out-file1.csv",
            "out-file2.csv",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Expected successful exit code with multiple files. stderr: {result.stderr}"


def test_input_missing_required_args(mock_file_path: Path) -> None:
    """Test that missing required input arguments cause error."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--input_bucket_names",
            "input-bucket",
            # Missing input_paths and input_filenames
            "--output_bucket_names",
            "output-bucket",
            "--output_paths",
            "output/path",
            "--output_filenames",
            "result.csv",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0, "Expected non-zero exit code when input arguments are missing"
    assert "required" in result.stderr.lower()


def test_output_missing_required_args(mock_file_path: Path) -> None:
    """Test that missing required output arguments cause error."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--input_bucket_names",
            "input-bucket",
            "--input_paths",
            "input/path",
            "--input_filenames",
            "test.csv",
            "--output_bucket_names",
            "output-bucket",
            # Missing output_paths and output_filenames
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0, "Expected non-zero exit code when output arguments are missing"
    assert "required" in result.stderr.lower()


def test_mismatched_input_lengths(mock_file_path: Path) -> None:
    """Test behavior when input argument lists have different lengths."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--input_bucket_names",
            "bucket1",
            "bucket2",
            "--input_paths",
            "path1",  # Only one path for two buckets
            "--input_filenames",
            "file1.csv",
            "file2.csv",
            "--output_bucket_names",
            "output-bucket",
            "--output_paths",
            "output/path",
            "--output_filenames",
            "result.csv",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    # This should fail because bucket_names length (2) must be 1 or equal to filenames length (2)
    # but paths length (1) doesn't match, causing a mismatch in URI building
    assert result.returncode != 0, "Expected non-zero exit code when argument lists have mismatched lengths"


def test_empty_arguments(mock_file_path: Path) -> None:
    """Test that empty argument lists cause appropriate errors."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--input_bucket_names",
            "--input_paths",
            "input/path",
            "--input_filenames",
            "test.csv",
            "--output_bucket_names",
            "output-bucket",
            "--output_paths",
            "output/path",
            "--output_filenames",
            "result.csv",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0, "Expected non-zero exit code with empty bucket names"


def test_all_input_output_missing(mock_file_path: Path) -> None:
    """Test that completely missing input and output arguments cause error."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            # All arguments omitted
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0, "Expected non-zero exit code when all required arguments are missing"
    assert "required" in result.stderr.lower()
