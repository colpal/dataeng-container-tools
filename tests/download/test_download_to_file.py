"""Tests for the download module."""

import tempfile
from collections.abc import Generator
from concurrent.futures import Future
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import requests
import requests_mock

from dataeng_container_tools import Download


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_urls_and_files(temp_dir: Path) -> dict[str, Path]:
    """Create mock URLs and file paths for testing."""
    return {
        "https://example.com/file1.txt": temp_dir / "file1.txt",
        "https://example.com/file2.txt": temp_dir / "file2.txt",
        "https://example.com/file3.txt": temp_dir / "file3.txt",
    }


@pytest.fixture
def mock_file_content() -> dict[str, str]:
    """Mock file content for downloads."""
    return {
        "https://example.com/file1.txt": "This is the content of file 1",
        "https://example.com/file2.txt": "This is the content of file 2",
        "https://example.com/file3.txt": "This is the content of file 3",
    }


def test_download_init() -> None:
    """Test Download module initialization."""
    download = Download()
    assert download.MODULE_NAME == "DL"
    assert isinstance(download.DEFAULT_SECRET_PATHS, dict)


def test_get_to_file_success(temp_dir: Path) -> None:
    """Test successful file download with _get_to_file method."""
    test_url = "https://example.com/test.txt"
    test_content = "Test file content"
    test_file = temp_dir / "test.txt"

    with requests_mock.Mocker() as m:
        m.get(test_url, text=test_content)

        url, file_path = Download._get_to_file(
            url=test_url,
            local_file_path=test_file,
            headers={},
        )

        assert url == test_url
        assert file_path == test_file
        assert file_path.exists()
        assert file_path.read_text() == test_content


def test_get_to_file_with_headers(temp_dir: Path) -> None:
    """Test file download with custom headers."""
    test_url = "https://example.com/test.txt"
    test_content = "Test file content"
    test_file = temp_dir / "test.txt"
    test_headers = {"Authorization": "Bearer token123", "User-Agent": "TestAgent/1.0"}

    with requests_mock.Mocker() as m:

        def check_headers(request: requests.Request, context: Any) -> str:  # noqa: ANN401
            assert request.headers.get("Authorization") == "Bearer token123"
            assert request.headers.get("User-Agent") == "TestAgent/1.0"
            return test_content

        m.get(test_url, text=check_headers)

        url, file_path = Download._get_to_file(
            url=test_url,
            local_file_path=test_file,
            headers=test_headers,
        )

        assert url == test_url
        assert file_path == test_file
        assert file_path.read_text() == test_content


def test_get_to_file_http_error(temp_dir: Path) -> None:
    """Test _get_to_file with HTTP error."""
    test_url = "https://example.com/nonexistent.txt"
    test_file = temp_dir / "test.txt"

    with requests_mock.Mocker() as m:
        m.get(test_url, status_code=404)

        with pytest.raises(requests.exceptions.HTTPError):
            Download._get_to_file(
                url=test_url,
                local_file_path=test_file,
                headers={},
            )


def test_download_complete_mode(
    mock_urls_and_files: dict[str, Path],
    mock_file_content: dict[str, str],
) -> None:
    """Test download with 'complete' output mode."""
    with requests_mock.Mocker() as m:
        for url, content in mock_file_content.items():
            m.get(url, text=content)

        result = Download.download(mock_urls_and_files, output="complete")

        assert result is None

        # Verify all files were downloaded
        for _url, file_path in mock_urls_and_files.items():
            assert file_path.exists(), f"File {file_path} was not created"
            assert file_path.read_text() == mock_file_content[_url], f"File content mismatch for {file_path}"


def test_download_generator_mode(
    mock_urls_and_files: dict[str, Path],
    mock_file_content: dict[str, str],
) -> None:
    """Test download with 'generator' output mode."""
    with requests_mock.Mocker() as m:
        for url, content in mock_file_content.items():
            m.get(url, text=content)

        results = list(Download.download(mock_urls_and_files, output="generator"))

        assert len(results) == len(mock_urls_and_files)

        # Check that we got all expected URL-path pairs
        result_urls = {url for url, _ in results}
        expected_urls = set(mock_urls_and_files.keys())
        assert result_urls == expected_urls, f"Expected URLs {expected_urls}, got {result_urls}"

        # Verify files exist and have correct content
        for url, file_path in results:
            assert file_path.exists(), f"File {file_path} was not created"
            assert file_path.read_text() == mock_file_content[url], f"File content mismatch for {file_path}"


def test_download_futures_mode(
    mock_urls_and_files: dict[str, Path],
    mock_file_content: dict[str, str],
) -> None:
    """Test download with 'futures' output mode."""
    with requests_mock.Mocker() as m:
        for url, content in mock_file_content.items():
            m.get(url, text=content)

        with Download.download(mock_urls_and_files, output="futures") as futures:
            assert len(futures) == len(mock_urls_and_files)

            # Check that all futures are Future objects
            for future in futures:
                assert isinstance(future, Future), f"Expected Future object, got {type(future)}"

            # Get results from futures
            results = [future.result() for future in futures]
            result_urls = {url for url, _ in results}
            expected_urls = set(mock_urls_and_files.keys())
            assert result_urls == expected_urls, f"Expected URLs {expected_urls}, got {result_urls}"


def test_download_default_output_mode(
    mock_urls_and_files: dict[str, Path],
    mock_file_content: dict[str, str],
) -> None:
    """Test download with default output mode (complete)."""
    with requests_mock.Mocker() as m:
        for url, content in mock_file_content.items():
            m.get(url, text=content)

        # Test without specifying output parameter - should default to "complete"
        result = Download.download(mock_urls_and_files)

        assert result is None

        # Verify all files were downloaded
        for url, file_path in mock_urls_and_files.items():
            assert file_path.exists(), f"File {file_path} was not created"
            assert file_path.read_text() == mock_file_content[url], f"File content mismatch for {file_path}"


def test_download_with_custom_parameters(
    mock_urls_and_files: dict[str, Path],
    mock_file_content: dict[str, str],
) -> None:
    """Test download with custom parameters."""
    custom_headers = {"Custom-Header": "test-value"}

    with requests_mock.Mocker() as m:

        def check_headers(request: requests.Request, context: Any) -> str:  # noqa: ANN401
            assert request.headers.get("Custom-Header") == "test-value"
            url = request.url
            return mock_file_content[url]

        for url in mock_file_content:
            m.get(url, text=check_headers)

        Download.download(
            mock_urls_and_files,
            headers=custom_headers,
            max_workers=2,
            chunk_size=1024,
            timeout=30,
            decode_content=False,
            mode="thread",
            output="complete",
        )

        # Verify all files were downloaded
        for file_path in mock_urls_and_files.values():
            assert file_path.exists(), f"File {file_path} was not created"


def test_download_process_mode(mock_urls_and_files: dict[str, Path], mock_file_content: dict[str, str]) -> None:
    """Test download with process mode."""
    with requests_mock.Mocker() as m:
        for url, content in mock_file_content.items():
            m.get(url, text=content)

        Download.download(mock_urls_and_files, mode="process", output="complete")

        # Verify all files were downloaded
        for url, file_path in mock_urls_and_files.items():
            assert file_path.exists(), f"File {file_path} was not created"
            assert file_path.read_text() == mock_file_content[url], f"File content mismatch for {file_path}"


def test_download_with_error_handling(temp_dir: Path) -> None:
    """Test download error handling in complete mode."""
    urls_and_files = {
        "https://example.com/valid.txt": temp_dir / "valid.txt",
        "https://example.com/invalid.txt": temp_dir / "invalid.txt",
    }

    with requests_mock.Mocker() as m:
        m.get("https://example.com/valid.txt", text="Valid content")
        m.get("https://example.com/invalid.txt", status_code=404)

        # Should not raise an exception, but log the error
        with patch("dataeng_container_tools.modules.download.download.logger") as mock_logger:
            Download.download(urls_and_files, output="complete")

            # Check that error was logged
            mock_logger.exception.assert_called_once()

        # Valid file should still be downloaded
        valid_file = temp_dir / "valid.txt"
        assert valid_file.exists()
        assert valid_file.read_text() == "Valid content"

        # Invalid file should not exist
        invalid_file = temp_dir / "invalid.txt"
        assert not invalid_file.exists()


def test_download_generator_with_error_handling(temp_dir: Path) -> None:
    """Test download error handling in generator mode."""
    urls_and_files = {
        "https://example.com/valid.txt": temp_dir / "valid.txt",
        "https://example.com/invalid.txt": temp_dir / "invalid.txt",
    }

    with requests_mock.Mocker() as m:
        m.get("https://example.com/valid.txt", text="Valid content")
        m.get("https://example.com/invalid.txt", status_code=404)

        with patch("dataeng_container_tools.modules.download.download.logger") as mock_logger:
            results = list(Download.download(urls_and_files, output="generator"))

            # Should only get results for successful downloads
            assert len(results) == 1
            url, file_path = results[0]
            assert url == "https://example.com/valid.txt"
            assert file_path == temp_dir / "valid.txt"

            # Check that error was logged
            mock_logger.exception.assert_called_once()


def test_download_futures_with_error_handling(temp_dir: Path) -> None:
    """Test download error handling in futures mode."""
    urls_and_files = {
        "https://example.com/valid.txt": temp_dir / "valid.txt",
        "https://example.com/invalid.txt": temp_dir / "invalid.txt",
    }

    with requests_mock.Mocker() as m:
        m.get("https://example.com/valid.txt", text="Valid content")
        m.get("https://example.com/invalid.txt", status_code=404)

        with Download.download(urls_and_files, output="futures") as futures:
            results = []
            errors = []

            for future in futures:
                if future.exception():
                    errors.append(future.exception())
                else:
                    results.append(future.result())

            # Should have one successful result and one error
            assert len(results) == 1
            assert len(errors) == 1

            url, file_path = results[0]
            assert url == "https://example.com/valid.txt"
            assert file_path == temp_dir / "valid.txt"
            assert isinstance(errors[0], requests.exceptions.HTTPError)


def test_download_to_file_invalid_output(temp_dir: Path) -> None:
    """Test download_to_file with invalid output parameter."""
    urls_and_files = {"https://example.com/test.txt": str(temp_dir / "test.txt")}

    with pytest.raises(NotImplementedError, match="Output specified 'invalid' has not been implemented"):
        Download.download_to_file(urls_and_files, output="invalid")  # type: ignore[arg-type]


def test_download_with_string_paths(temp_dir: Path) -> None:
    """Test download with string file paths instead of Path objects."""
    urls_and_files = {
        "https://example.com/file1.txt": str(temp_dir / "file1.txt"),
        "https://example.com/file2.txt": str(temp_dir / "file2.txt"),
    }

    with requests_mock.Mocker() as m:
        m.get("https://example.com/file1.txt", text="Content 1")
        m.get("https://example.com/file2.txt", text="Content 2")

        Download.download(urls_and_files, output="complete")

        # Verify files were created
        file1 = temp_dir / "file1.txt"
        file2 = temp_dir / "file2.txt"
        assert file1.exists()
        assert file2.exists()
        assert file1.read_text() == "Content 1"
        assert file2.read_text() == "Content 2"


def test_download_empty_mapping() -> None:
    """Test download with empty URLs mapping."""
    result = Download.download({}, output="complete")
    assert result is None

    results = list(Download.download({}, output="generator"))
    assert len(results) == 0

    with Download.download({}, output="futures") as futures:
        assert len(futures) == 0


def test_download_large_chunk_size(temp_dir: Path) -> None:
    """Test download with large chunk size."""
    test_url = "https://example.com/large.txt"
    test_content = "x" * (1024 * 1024)  # 1MB content
    test_file = temp_dir / "large.txt"

    with requests_mock.Mocker() as m:
        m.get(test_url, text=test_content)

        Download.download(
            {test_url: test_file},
            chunk_size=64 * 1024 * 1024,  # 64MB chunk
            output="complete",
        )

        assert test_file.exists()
        assert test_file.read_text() == test_content


def test_download_content_bigger_than_chunk(temp_dir: Path) -> None:
    """Test download where content is bigger than chunk size."""
    test_url = "https://example.com/bigfile.txt"
    test_content = "A" * (1024 * 1024)  # 1MB content
    test_file = temp_dir / "bigfile.txt"

    with requests_mock.Mocker() as m:
        m.get(test_url, text=test_content)

        Download.download(
            {test_url: test_file},
            chunk_size=10 * 1024,  # 10KB chunk
            output="complete",
        )

        assert test_file.exists()
        assert test_file.read_text() == test_content
        assert len(test_file.read_text()) == 1024 * 1024


def test_download_custom_timeout(temp_dir: Path) -> None:
    """Test download with custom timeout."""
    test_url = "https://example.com/test.txt"
    test_file = temp_dir / "test.txt"

    with requests_mock.Mocker() as m:
        m.get(test_url, text="content")

        Download.download(
            {test_url: test_file},
            timeout=120,  # 2 minutes
            output="complete",
        )

        assert test_file.exists()
        assert test_file.read_text() == "content"
