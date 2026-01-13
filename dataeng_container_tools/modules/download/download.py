"""Download module for generic HTTP requests.

This module wraps around requests and pool executors to provide multithread/multiprocessing
for downloading files. It offers flexible output options, including waiting for all
downloads to complete, yielding file paths as they complete, or yielding futures
for more fine-grained control.
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import Executor, Future, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Final, Literal, ParamSpec, overload

import requests

from dataeng_container_tools.modules import BaseModule

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping
    from types import TracebackType

logger = logging.getLogger("Container Tools")


class _ExecutorContext:
    """Context manager for executor and futures that ensures proper cleanup."""

    def __init__(self, executor: Executor, futures: set[Future[tuple[str, Path]]]) -> None:
        self.executor = executor
        self.futures = futures

    def __enter__(self) -> set[Future[tuple[str, Path]]]:
        return self.futures

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.executor.shutdown(wait=True)


class Download(BaseModule):
    """A module for downloading files over HTTP/HTTPS.

    Provides functionality to download multiple files concurrently using
    either threads or processes. It allows customization of request headers,
    timeouts, chunk sizes for streaming, and the number of concurrent workers.
    """

    MODULE_NAME: ClassVar[str] = "DL"
    DEFAULT_SECRET_PATHS: ClassVar[dict[str, str]] = {}

    DEFAULT_TIMEOUT: Final = 5 * 60  # 5 minutes
    DEFAULT_CHUNK_SIZE: Final = 32 * 1024 * 1024  # 32 MB
    DEFAULT_MAX_WORKERS: Final = 5

    def __init__(self) -> None:
        """Initializes the DL module."""
        super().__init__()

    @staticmethod
    def _get_to_file(
        url: str,
        local_file_path: Path,
        headers: dict[str, str],
        timeout: int = DEFAULT_TIMEOUT,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        *,
        decode_content: bool = True,
    ) -> tuple[str, Path]:
        """Downloads a single file from a URL to a local path.

        This is an internal helper method used by the main download functions.
        It handles the actual HTTP GET request and streams the response content
        to a local file.

        Args:
            url: The URL of the file to download.
            local_file_path: The local path where the file will be saved.
            headers: HTTP headers to include in the request.
            timeout: Request timeout in seconds. Defaults to DEFAULT_TIMEOUT.
            chunk_size: Size of download chunks in bytes for streaming.
                Defaults to DEFAULT_CHUNK_SIZE.
            decode_content: Whether to decode content based on response headers.
                Defaults to True.

        Returns:
            A tuple containing the original URL and the local Path
            object of the downloaded file.

        Raises:
            requests.exceptions.HTTPError: If the HTTP request returned an unsuccessful
                status code.
            requests.exceptions.RequestException: For other request-related errors
                (e.g., connection issues, timeouts).
        """
        with requests.Session() as session, session.get(url, headers=headers, timeout=timeout, stream=True) as response:
            response.raise_for_status()
            with local_file_path.open("wb") as f:
                for chunk in response.raw.stream(amt=chunk_size, decode_content=decode_content):
                    if chunk:
                        f.write(chunk)
                        f.flush()
                        os.fsync(f.fileno())
        return url, local_file_path

    @staticmethod
    @overload
    def download(
        urls_to_files: Mapping[str, str | os.PathLike[str]],
        *,
        headers: dict[str, str] | None = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        timeout: int = DEFAULT_TIMEOUT,
        decode_content: bool = True,
        mode: Literal["thread", "process"] = "thread",
        output: Literal["complete"],
    ) -> None: ...

    @staticmethod
    @overload
    def download(
        urls_to_files: Mapping[str, str | os.PathLike[str]],
        *,
        headers: dict[str, str] | None = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        timeout: int = DEFAULT_TIMEOUT,
        decode_content: bool = True,
        mode: Literal["thread", "process"] = "thread",
        output: Literal["generator"],
    ) -> Generator[tuple[str, Path]]: ...

    @staticmethod
    @overload
    def download(
        urls_to_files: Mapping[str, str | os.PathLike[str]],
        *,
        headers: dict[str, str] | None = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        timeout: int = DEFAULT_TIMEOUT,
        decode_content: bool = True,
        mode: Literal["thread", "process"] = "thread",
        output: Literal["futures"],
    ) -> _ExecutorContext: ...

    # Default overload when output is not specified
    @staticmethod
    @overload
    def download(
        urls_to_files: Mapping[str, str | os.PathLike[str]],
        *,
        headers: dict[str, str] | None = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        timeout: int = DEFAULT_TIMEOUT,
        decode_content: bool = True,
        mode: Literal["thread", "process"] = "thread",
        output: Literal["complete"] = "complete",
    ) -> None: ...

    @staticmethod
    def download(
        urls_to_files: Mapping[str, str | os.PathLike[str]],
        **kwargs: ParamSpec,
    ) -> None | Generator | _ExecutorContext:
        r"""Downloads files from a mapping of URLs to local file paths.

        This is the primary user-facing download function. It offers multiple modes of
        operation via the `output` parameter, allowing users to:
        - Wait for all downloads to complete (`output="complete"`).
        - Receive a generator yielding (URL, Path) tuples as files are downloaded (`output="generator"`).
        - Receive a context manager containing Future objects for each download task (`output="futures"`).

        Args:
            urls_to_files: A mapping where keys are URLs (str)
                and values are local file paths (str or Path) where the content will be saved.
            **kwargs: Additional keyword arguments that are passed to `download_to_file`.
                See `download_to_file` for more details on available options such as:
                - `headers`: HTTP headers.
                - `max_workers`: Maximum number of workers.
                - `chunk_size`: Chunk size for streaming.
                - `timeout`: Request timeout.
                - `decode_content`: Whether to decode content.
                - mode: Specifies the type of executor to use.
                    "thread" for `ThreadPoolExecutor`, "process" for `ProcessPoolExecutor`.
                    Defaults to "thread".
                - output: Determines the return behavior. Defaults to "complete".
                    "complete": Waits for all downloads to finish and returns None.
                    "generator": Returns a generator that yields `(url, Path)` tuples
                    as each download completes.
                    "futures": Returns a context manager that yields a set of `Future` objects
                    for each submitted download task and automatically handles executor cleanup.

        Returns:
            The return type depends on the `output` keyword argument:
                - `None`: If `output` is "complete" (default).
                - `Generator[tuple[str, Path]]`: If `output` is "generator".
                - `_ExecutorContext`: If `output` is "futures". A context manager that yields
                  a set of Future objects when entered.

        Raises:
            NotImplementedError: If an invalid `output` mode is specified.
            Exceptions from `requests` (e.g., `requests.exceptions.HTTPError`) can be
            raised during the download process, especially when `output="futures"`
            and `future.result()` is called. For "complete" and "generator" modes,
            exceptions are caught and logged.

        Examples:
            Download a single file and wait for completion:
                >>> urls = {"http://example.com/file1.txt": "local_file1.txt"}
                >>> Download.download(urls) # Default output="complete"

            Download multiple files and get paths as they complete:
                >>> urls_map = {
                ...     "http://example.com/image.jpg": "image.jpg",
                ...     "http://example.com/data.csv": "data/data.csv"
                ... }
                >>> for url, path in Download.download(urls_map, output="generator"):
                ...     print(f"Downloaded {url} to {path}")

            Download files using multiple processes and get futures:
                >>> from concurrent.futures import as_completed
                >>> urls_to_download = {"http://example.com/archive.zip": "archive.zip"}
                >>> with Download.download(
                ...     urls_to_download,
                ...     mode="process",
                ...     output="futures",
                ...     max_workers=2
                ... ) as futures:
                ...     for future in as_completed(futures):
                ...         try:
                ...             url, file_path = future.result()
                ...             print(f"Successfully downloaded {url} to {file_path}")
                ...         except Exception as e:
                ...             print(f"Failed to download a file: {e}")
        """
        return Download.download_to_file(urls_to_files, **kwargs)

    @staticmethod
    def download_to_file(
        urls_to_files: Mapping[str, str | os.PathLike[str]],
        *,
        headers: dict[str, str] | None = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        timeout: int = DEFAULT_TIMEOUT,
        decode_content: bool = True,
        mode: Literal["thread", "process"] = "thread",
        output: Literal["complete", "generator", "futures"] = "complete",
    ) -> (
        None  # Complete
        | Generator[tuple[str, Path]]  # File paths generator
        | _ExecutorContext  # Context manager for futures
    ):
        r"""Core implementation for downloading content from URLs using an executor.

        This method handles the setup of the executor (ThreadPoolExecutor or
        ProcessPoolExecutor) and submits download tasks using `_get_to_file`.
        It provides different output modes based on the `output` parameter.

        Args:
            urls_to_files: A mapping where keys are URLs (str)
                and values are local file paths (str or Path object) for saving content.
            headers: HTTP headers to use for requests.
                Defaults to None (an empty dictionary will be used).
            max_workers: The maximum number of worker threads or processes to use
                for concurrent downloads. Defaults to DEFAULT_MAX_WORKERS.
            chunk_size: The size (in bytes) of chunks to use when streaming
                download content. Defaults to DEFAULT_CHUNK_SIZE.
            timeout: The timeout (in seconds) for HTTP requests.
                Defaults to DEFAULT_TIMEOUT.
            decode_content: If True, decodes the response content based on
                response headers (e.g., for gzipped content). Defaults to True.
            mode: Specifies the type of executor to use.
                "thread" for `ThreadPoolExecutor`, "process" for `ProcessPoolExecutor`.
                Defaults to "thread".
            output: Determines the return behavior. Defaults to "complete".
                - "complete": Waits for all downloads to finish and returns None.
                - "generator": Returns a generator that yields `(url, Path)` tuples
                  as each download completes.
                - "futures": Returns a context manager that yields a set of `Future` objects
                  for each submitted download task and automatically handles executor cleanup.

        Returns:
            The return type depends on the `output` argument:
                - `None`: If `output` is "complete".
                - `Generator[tuple[str, Path]]`: If `output` is "generator".
                - `_ExecutorContext`: If `output` is "futures".

        Raises:
            NotImplementedError: If an unsupported `output` mode is provided.
            Exceptions from `requests` (e.g., `requests.exceptions.HTTPError`) can be
            propagated, especially when `output="futures"` and `future.result()` is called.
            For "complete" and "generator" modes, exceptions during individual downloads
            are caught and logged, allowing other downloads to proceed.
        """
        if headers is None:
            headers = {}

        executor_class = ThreadPoolExecutor if mode == "thread" else ProcessPoolExecutor

        if output in ["complete", "generator"]:
            with executor_class(max_workers=max_workers) as executor:
                futures_urls = {
                    executor.submit(
                        Download._get_to_file,
                        url,
                        Path(file_path),
                        headers,
                        timeout,
                        chunk_size,
                        decode_content=decode_content,
                    ): url
                    for url, file_path in urls_to_files.items()
                }

                if output == "complete":
                    for future in as_completed(futures_urls):
                        url = futures_urls[future]
                        try:
                            future.result()
                        except Exception:
                            logger.exception("Error downloading %s", url)
                    return None

                if output == "generator":

                    def file_path_generator() -> Generator:
                        for future in as_completed(futures_urls):
                            url = futures_urls[future]
                            try:
                                url, file_path = future.result()
                                yield url, file_path
                            except Exception:
                                logger.exception("Error downloading %s", url)

                    return file_path_generator()

        if output == "futures":
            executor = executor_class(max_workers=max_workers)
            futures = {
                executor.submit(
                    Download._get_to_file,
                    url,
                    Path(file_path),
                    headers,
                    timeout,
                    chunk_size,
                    decode_content=decode_content,
                )
                for url, file_path in urls_to_files.items()
            }
            return _ExecutorContext(executor, futures)

        msg = f"Output specified '{output}' has not been implemented"
        raise NotImplementedError(msg)
