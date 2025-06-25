"""Tests for the cla identifying_tags functionality."""

import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture
def mock_file_path() -> Path:
    """Set up test fixtures."""
    return Path(__file__).parent / "mock_files" / "tags.py"


def test_tags_all_provided(mock_file_path: Path) -> None:
    """Test that all identifying tags work when provided."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--dag_id",
            "test_dag",
            "--run_id",
            "test_run_123",
            "--namespace",
            "test_namespace",
            "--pod_name",
            "test_pod",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Expected successful exit code. stderr: {result.stderr}"
    assert "DAG_ID: test_dag" in result.stderr
    assert "RUN_ID: test_run_123" in result.stderr
    assert "NAMESPACE: test_namespace" in result.stderr
    assert "POD_NAME: test_pod" in result.stderr


def test_tags_missing_required(mock_file_path: Path) -> None:
    """Test that missing required tags cause error."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--dag_id",
            "test_dag",
            "--run_id",
            "test_run_123",
            # Missing namespace and pod_name
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0, "Expected non-zero exit code when required tags are missing"
    assert "required" in result.stderr.lower()


def test_tags_empty_values(mock_file_path: Path) -> None:
    """Test that empty string values are accepted for tags."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--dag_id",
            "",
            "--run_id",
            "",
            "--namespace",
            "",
            "--pod_name",
            "",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Expected successful exit code with empty values. stderr: {result.stderr}"


def test_tags_special_characters(mock_file_path: Path) -> None:
    """Test that special characters in tags are handled correctly."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--dag_id",
            "test-dag_with.special-chars",
            "--run_id",
            "run-2023.01.01_12:34:56",
            "--namespace",
            "test-namespace-123",
            "--pod_name",
            "pod-name-with-uuid-abc123",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Expected successful exit code with special characters. stderr: {result.stderr}"


def test_tags_long_values(mock_file_path: Path) -> None:
    """Test that long tag values are handled correctly."""
    long_value = "a" * 100  # 100 character string
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--dag_id",
            long_value,
            "--run_id",
            long_value,
            "--namespace",
            long_value,
            "--pod_name",
            long_value,
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Expected successful exit code with long values. stderr: {result.stderr}"


def test_tags_only_dag_id_missing(mock_file_path: Path) -> None:
    """Test that missing only dag_id causes error when all are required."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            # --dag_id omitted
            "--run_id",
            "test_run_123",
            "--namespace",
            "test_namespace",
            "--pod_name",
            "test_pod",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0, "Expected non-zero exit code when dag_id is missing"
    assert "required" in result.stderr.lower()
    assert "dag_id" in result.stderr.lower()


def test_tags_numeric_values(mock_file_path: Path) -> None:
    """Test that numeric values are converted to strings correctly."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--dag_id",
            "123",
            "--run_id",
            "456",
            "--namespace",
            "789",
            "--pod_name",
            "000",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Expected successful exit code with numeric values. stderr: {result.stderr}"
    assert "DAG_ID: 123" in result.stderr
