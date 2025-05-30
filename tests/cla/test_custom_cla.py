"""Tests for the cla.CustomCommandLineArgument module."""

import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture
def mock_file_path() -> Path:
    """Set up test fixtures."""
    return Path(__file__).parent / "mock_files" / "custom_cla.py"


def test_custom_args_exist(mock_file_path: Path) -> None:
    """Test that custom arguments with valid inputs execute successfully."""
    # Test with valid inputs for both arguments
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--some_arg",
            "value1",
            "value2",
            "value3",
            "--some_arg2",
            "42",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Expected successful exit code 0, got {result.returncode}. stderr: {result.stderr}"


def test_wrong_type(mock_file_path: Path) -> None:
    """Test that incorrect argument types cause proper validation failure."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--some_arg",
            "value1",
            "--some_arg2",
            "not_an_integer",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0, "Expected non-zero exit code when providing string to int argument"
    assert "invalid int value" in result.stderr.lower()


def test_custom_parameters(mock_file_path: Path) -> None:
    """Test that nargs='+' functionality works correctly with multiple values."""
    # Test with 5 arguments
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--some_arg",
            "val1",
            "val2",
            "val3",
            "val4",
            "val5",
            "--some_arg2",
            "123",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"Expected successful exit code with multiple args. stderr: {result.stderr}"


def test_custom_parameters2(mock_file_path: Path) -> None:
    """Test that nargs='+' requires at least one value and fails appropriately."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--some_arg",
            "--some_arg2",
            "42",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0, "Expected non-zero exit code when no values provided to nargs='+' argument"


def test_optional(mock_file_path: Path) -> None:
    """Test that optional arguments can be successfully omitted."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--some_arg",
            "required_value",
            # some_arg2 is omitted
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, (
        f"Expected successful exit code when optional argument is omitted. stderr: {result.stderr}"
    )


def test_required(mock_file_path: Path) -> None:
    """Test that required arguments cause failure when omitted."""
    result = subprocess.run(
        [
            sys.executable,
            str(mock_file_path),
            "--some_arg2",
            "42",
            # some_arg is omitted but is required
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0, "Expected non-zero exit code when required argument is omitted"
    assert "required: --some_arg" in result.stderr.lower()
