"""Unit tests for the cla.CommandLineArgumentType module."""

import subprocess
import sys
import unittest
from pathlib import Path


class TestCLAType(unittest.TestCase):
    """Test CommandLineArgumentType enum behavior with required, optional, and unused arguments."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_file_path = Path(__file__).parent / "mock_files" / "required_optional_unused.py"

    def test_standard_use(self) -> None:
        """Test that providing all required and optional arguments works successfully."""
        result = subprocess.run(
            [
                sys.executable,
                str(self.mock_file_path),
                "--secret_locations",
                '{"secret": "gs://test-bucket/secrets"}',
                "--input_bucket_names",
                "input-bucket",
                "--input_paths",
                "input/path",
                "--input_filenames",
                "test.csv",
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, (
            f"Expected successful exit code when providing required and optional args. stderr: {result.stderr}"
        )

    def test_required_missing(self) -> None:
        """Test that required arguments cause failure when omitted."""
        result = subprocess.run(
            [
                sys.executable,
                str(self.mock_file_path),
                "--input_bucket_names",
                "input-bucket",
                "--input_paths",
                "input/path",
                "--input_filenames",
                "test.csv",
                # secret_locations is omitted but it's required
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode != 0, "Expected non-zero exit code when required argument is omitted"
        assert "required" in result.stderr.lower()
        assert "secret_locations" in result.stderr.lower()

    def test_optional_missing(self) -> None:
        """Test that optional arguments can be successfully omitted."""
        result = subprocess.run(
            [
                sys.executable,
                str(self.mock_file_path),
                "--secret_locations",
                '{"secret": "gs://test-bucket/secrets"}',
                # input_files arguments are omitted but they're optional
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, (
            f"Expected successful exit code when optional arguments are omitted. stderr: {result.stderr}"
        )

    def test_unused_arguments_error(self) -> None:
        """Test that unused arguments cause error when attempted to be used."""
        result = subprocess.run(
            [
                sys.executable,
                str(self.mock_file_path),
                "--secret_locations",
                '{"secret": "gs://test-bucket/secrets"}',
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

        assert result.returncode != 0, "Expected non-zero exit code when using unused arguments"
        assert "unrecognized arguments" in result.stderr.lower()

    def test_unused_partial_arguments_error(self) -> None:
        """Test that even partial unused arguments cause error."""
        result = subprocess.run(
            [
                sys.executable,
                str(self.mock_file_path),
                "--secret_locations",
                '{"secret": "gs://test-bucket/secrets"}',
                "--output_bucket_names",
                "output-bucket",
                # Only partial output arguments provided
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode != 0, "Expected non-zero exit code when using any unused arguments"
        assert "unrecognized arguments" in result.stderr.lower()

    def test_minimal_required_only(self) -> None:
        """Test that providing only required arguments works successfully."""
        result = subprocess.run(
            [
                sys.executable,
                str(self.mock_file_path),
                "--secret_locations",
                '{"secret": "gs://test-bucket/secrets"}',
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, (
            f"Expected successful exit code when providing only required arguments. stderr: {result.stderr}"
        )


if __name__ == "__main__":
    unittest.main()
