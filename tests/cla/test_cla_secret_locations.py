"""Tests for the cla secret_locations functionality."""

import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture
def mock_file_path() -> Path:
    """Set up test fixtures."""
    return Path(__file__).parent / "mock_files" / "secret_locations.py"


def test_secret_locations_valid_json(mock_file_path: Path) -> None:
    """Test that valid JSON secret locations are parsed correctly."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--secret_locations",
            '{"GCS": "/vault/secrets/gcs", "SF": "/vault/secrets/snowflake"}',
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Expected successful exit code. stderr: {result.stderr}"
    assert "Secret locations:" in result.stderr


def test_secret_locations_empty_json(mock_file_path: Path) -> None:
    """Test that empty JSON object is accepted."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--secret_locations",
            "{}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Expected successful exit code with empty JSON. stderr: {result.stderr}"


def test_secret_locations_invalid_json(mock_file_path: Path) -> None:
    """Test that invalid JSON causes proper error."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--secret_locations",
            "not_valid_json",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0, "Expected non-zero exit code with invalid JSON"


def test_secret_locations_missing_required(mock_file_path: Path) -> None:
    """Test that missing required secret_locations argument causes error."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            # --secret_locations is omitted but required
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0, "Expected non-zero exit code when required argument is omitted"
    assert "required" in result.stderr.lower()
    assert "secret_locations" in result.stderr.lower()


def test_secret_locations_complex_json(mock_file_path: Path) -> None:
    """Test that complex JSON structures are handled correctly."""
    complex_json = '{"GCS": "/vault/secrets/gcs", "SF": "/vault/secrets/sf", "DS": "/vault/secrets/datastore"}'
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--secret_locations",
            complex_json,
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Expected successful exit code with complex JSON. stderr: {result.stderr}"


def test_secret_locations_malformed_json(mock_file_path: Path) -> None:
    """Test that malformed JSON (missing quotes, etc.) causes error."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--secret_locations",
            "{GCS: /vault/secrets/gcs}",  # Missing quotes
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0, "Expected non-zero exit code with malformed JSON"
