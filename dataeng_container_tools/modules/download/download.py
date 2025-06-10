"""Download module for generic HTTP requests.

This module wraps around requests and pool executors to provide multithread/multiprocessing
for downloading files. It offers flexible output options, including waiting for all
downloads to complete, yielding file paths as they complete, or yielding futures
for more fine-grained control.
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, overload

import requests

from dataeng_container_tools.modules import BaseModule

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping

logger = logging.getLogger("Container Tools")


class Download(BaseModule):
    """A module for downloading files over HTTP/HTTPS.

    Provides functionality to download multiple files concurrently using
    either threads or processes, with options for customizing request headers,
    timeouts, chunk sizes, and the number of workers.
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
        urls_to_files: Mapping[str, str | Path],
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
        urls_to_files: Mapping[str, str | Path],
        *,
        headers: dict[str, str] | None = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        timeout: int = DEFAULT_TIMEOUT,
        decode_content: bool = True,
        mode: Literal["thread", "process"] = "thread",
        output: Literal["file_path"],
    ) -> Generator[tuple[str, Path]]: ...

    @staticmethod
    @overload
    def download(
        urls_to_files: Mapping[str, str | Path],
        *,
        headers: dict[str, str] | None = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        timeout: int = DEFAULT_TIMEOUT,
        decode_content: bool = True,
        mode: Literal["thread", "process"] = "thread",
        output: Literal["future"],
    ) -> Generator[Future[tuple[str, Path]]]: ...

    # Default overload when output is not specified
    @staticmethod
    @overload
    def download(
        urls_to_files: Mapping[str, str | Path],
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
    def download(  # noqa: D417
        urls_to_files: Mapping[str, str | Path],
        **kwargs: Any,
    ) -> None | Generator:
        r"""Downloads files from a mapping of URLs to local file paths.

        This is the primary user-facing download function. It offers multiple modes of
        operation via the `output` parameter, allowing users to:
        - Wait for all downloads to complete (`output="complete"`).
        - Receive a generator yielding (URL, Path) tuples as files are downloaded (`output="file_path"`).
        - Receive a generator yielding Future objects for each download task (`output="future"`).
        - Receive a tuple containing a list of Future objects and the executor instance (`output="executor"`).

        Args:
            urls_to_files (Mapping[str, str | Path]): A mapping where keys are URLs (str)
                and values are local file paths (str or Path) where the content will be saved.
            headers (dict[str, str] | None): HTTP headers to include in the request.
                Defaults to None.
            max_workers (int): Maximum number of worker threads or processes.
                Defaults to DEFAULT_MAX_WORKERS.
            chunk_size (int): Size of download chunks in bytes.
                Defaults to DEFAULT_CHUNK_SIZE.
            timeout (int): Request timeout in seconds.
                Defaults to DEFAULT_TIMEOUT.
            decode_content (bool): Whether to decode content based on response headers.
                Defaults to True.
            mode (Literal["thread", "process"]): Executor type, "thread" or "process".
                Defaults to "thread".
            output (Literal["complete", "file_path", "future", "executor"]):
                Specifies the return type. Defaults to "complete".
                - "complete": Waits for all downloads and returns None.
                - "file_path": Yields (URL, Path) tuples as downloads complete.
                - "future": Yields Future objects for each download.

        Returns:
            None | Generator[tuple[str, Path]] | Generator[Future[tuple[str, Path]]] | \\
            tuple[list[Future[tuple[str, Path]]], Executor]:
            - None if `output` is "complete".
            - A generator of `(str, Path)` tuples if `output` is "file_path".
            - A generator of `Future[tuple[str, Path]]` objects if `output` is "future".
            - A tuple `(list[Future[tuple[str, Path]]], Executor)` if `output` is "executor".

        Raises:
            Catches and logs exceptions during download when `output` is "complete" or "file_path".
            For "future", exceptions are raised when `future.result()` is called.
            NotImplementedError if an output is not valid
        """
        return Download.download_to_file(urls_to_files, **kwargs)

    @staticmethod
    def download_to_file(
        urls_to_files: Mapping[str, str | Path],
        *,
        headers: dict[str, str] | None = None,
        max_workers: int = DEFAULT_MAX_WORKERS,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        timeout: int = DEFAULT_TIMEOUT,
        decode_content: bool = True,
        mode: Literal["thread", "process"] = "thread",
        output: Literal["complete", "file_path", "future"] = "complete",
    ) -> (
        None  # Complete
        | Generator[tuple[str, Path]]  # File paths
        | Generator[Future[tuple[str, Path]]]  # Future
    ):
        r"""Core implementation for downloading content from URLs using an executor.

        This method handles the setup of the executor and submission of download tasks.
        It's generally recommended to use the `download` method instead, as it provides
        more user-friendly output options.

        Args:
            urls_to_files (Mapping[str, str | Path]): Mapping of URLs to local file paths.
            headers (dict[str, str] | None): HTTP headers. Defaults to None.
            max_workers (int): Maximum number of workers.
                Defaults to DEFAULT_MAX_WORKERS.
            chunk_size (int): Chunk size for streaming.
                Defaults to DEFAULT_CHUNK_SIZE.
            timeout (int): Request timeout. Defaults to DEFAULT_TIMEOUT.
            decode_content (bool): Whether to decode content based on response headers.
                Defaults to True.
            mode (Literal["thread", "process"]): "thread" or "process" for executor type.
                Defaults to "thread".
            output (Literal["complete", "file_path", "future", "executor"]):
                Specifies return type. Defaults to "complete".

        Returns:
            None | Generator[tuple[str, Path]] | Generator[Future[tuple[str, Path]]] | \\
            tuple[list[Future[tuple[str, Path]]], Executor]:
            Dependent on the `output` parameter:
            - None for "complete".
            - Generator of (URL, Path) for "file_path".
            - Generator of Futures for "future".
            - Tuple of (list of Futures, Executor) for "executor".
        """
        if headers is None:
            headers = {}

        executor_class = ThreadPoolExecutor if mode == "thread" else ProcessPoolExecutor

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

            if output == "file_path":
                def file_path_generator() -> Generator:
                    for future in as_completed(futures_urls):
                        url = futures_urls[future]
                        try:
                            url, file_path = future.result()
                            yield url, file_path
                        except Exception:
                            logger.exception("Error downloading %s", url)
                return file_path_generator()

            if output == "future":
                return (future for future in as_completed(futures_urls))

            msg = f"Output specified '{output}' has not been implemented"
            raise NotImplementedError(msg)
