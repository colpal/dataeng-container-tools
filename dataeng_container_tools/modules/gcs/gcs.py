"""Tools for working with GCP.

Deals with receiving downloading and uploading files from/to GCP. Has one
class: `gcs_file_io`.

Typical usage example:

    file_io = gcs_file_io(gcs_secret_location = secret_locations[0])
    pqt_obj = file_io.download_file_to_object(input_uris[0])
    #
    # Edit the object in some way here.
    #
    result = file_io.upload_file_from_object(gcs_uri=output_uris[0], object_to_upload=pqt_obj)
"""

from __future__ import annotations

import io
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast, overload

from dataeng_container_tools.modules import BaseModule, BaseModuleUtilities

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    import pandas as pd
    from google.cloud import storage
    from google.cloud.storage.blob import Blob


class GCSUriUtils:
    """Utility class for handling GCS URIs.

    Provides static methods to resolve and parse GCS URIs.
    """

    PREFIX: Final = "gs://"

    @staticmethod
    def normalize_uri(gcs_uri: str) -> str:
        """Normalizes a GCS URI by removing redundant slashes and resolving relative segments.

        Removes the "gs://" prefix, normalizes the path, and re-adds the prefix.

        Args:
            gcs_uri (str): The GCS URI string to normalize.

        Returns:
            str: The normalized GCS URI string.
        """
        import posixpath

        gcs_uri = gcs_uri.removeprefix(GCSUriUtils.PREFIX)
        return GCSUriUtils.PREFIX + posixpath.normpath(gcs_uri)

    @staticmethod
    def get_components(gcs_uri: str) -> tuple[str, str]:
        """Extracts the bucket name and file path from a GCS URI.

        Args:
            gcs_uri (str): The GCS URI string.

        Returns:
            tuple[str, str]: A tuple containing the bucket name and the file path.

        Raises:
            ValueError: If the URI does not start with the GCS prefix 'gs://'.
        """
        if not gcs_uri.startswith(GCSUriUtils.PREFIX):
            msg = f"Invalid GCS URI: '{gcs_uri}'. URI must start with '{GCSUriUtils.PREFIX}'"
            raise ValueError(msg)

        gcs_uri = gcs_uri.removeprefix(GCSUriUtils.PREFIX)
        bucket = gcs_uri[: gcs_uri.find("/")]
        file_path = gcs_uri[gcs_uri.find("/") + 1 :]
        return bucket, file_path


class GCSFileIO(BaseModule):
    """Uploads and downloads files to/from Google Cloud Storage (GCS).

    This class handles the boilerplate code for interacting with GCS,
    allowing for downloading files to objects or local files, and uploading
    objects or local files to GCS. It also includes helper functions for
    common GCS operations.

    Attributes:
        client (storage.Client): The Google Cloud Storage client instance.
        local (bool): A boolean indicating if the module is in local-only mode.
               If True, no actual GCS operations are performed.
    """

    MODULE_NAME: ClassVar[str] = "GCS"
    DEFAULT_SECRET_PATHS: ClassVar[dict[str, str]] = {"GCS": "/vault/secrets/gcp-sa-storage.json"}

    KNOWN_EXTENSIONS: Final = {".parquet", ".csv", ".xlsx", ".json"}

    def __init__(
        self,
        gcs_secret_location: str | Path | None = None,
        *,
        local: bool = False,
        use_cla_fallback: bool = True,
        use_file_fallback: bool = True,
    ) -> None:
        """Initializes GCSFileIO with desired configuration.

        Args:
            gcs_secret_location (str | Path | None): Path to the GCS service account JSON key file.
            local (bool): If True, operates in local mode without GCS interaction. Should be used
                with a GCS local emulator.
            use_cla_fallback (bool): If True, attempts to use command-line arguments
                as a fallback for secret location if `gcs_secret_location` is not found.
            use_file_fallback (bool): If True, attempts to use the default secret file path
                as a fallback if other sources fail.

        Raises:
            FileNotFoundError: If GCS credentials are not found and not in local mode.
        """
        from google.cloud import storage

        self.local = local

        if not self.local:
            gcs_sa = BaseModuleUtilities.parse_secret_with_fallback(
                gcs_secret_location,
                self.MODULE_NAME if use_cla_fallback else None,
                self.DEFAULT_SECRET_PATHS[self.MODULE_NAME] if use_file_fallback else None,
            )

            if not gcs_sa:
                msg = "GCS credentials not found"
                raise FileNotFoundError(msg)

            self.client: storage.Client = storage.Client.from_service_account_info(gcs_sa)
        else:
            from google.auth.credentials import AnonymousCredentials

            self.client: storage.Client = storage.Client(credentials=AnonymousCredentials())

    def uri_to_blobs(self, gcs_uri: str) -> Iterator[Blob]:
        """Converts a GCS URI to an iterator of Blob objects.

        Supports glob patterns in the GCS URI for matching multiple files.
        See `https://cloud.google.com/storage/docs/json_api/v1/objects/list#list-objects-and-prefixes-using-glob`
        for more information on glob matching.

        Args:
            gcs_uri (str): The GCS URI, which can include glob patterns.

        Returns:
            Iterator[Blob]: An iterator yielding `google.cloud.storage.blob.Blob` objects
            matching the URI.
        """
        bucket_name, file_path = GCSUriUtils.get_components(gcs_uri)
        bucket = self.client.bucket(bucket_name)
        return bucket.list_blobs(match_glob=file_path)

    @overload
    def download(
        self,
        src_dst: Sequence[tuple[str, str | Path]],
    ) -> None: ...

    @overload
    def download(
        self,
        src_dst: str | list[str],
        *,
        dtype: dict | None = None,
        **kwargs: Any,  # Use ParamSpec in future  # noqa: ANN401
    ) -> dict[
        str,
        Any,
    ]: ...  # TODO: Returning type dict[str, pd.DataFrame | io.BytesIO] might be too ambiguous for user typehinting use.

    def download(
        self,
        src_dst: str | list[str] | Sequence[tuple[str, str | Path]],
        **kwargs: Any,  # Use ParamSpec in future
    ) -> ...:
        """Downloads files from GCS to local file paths or Python objects.

        This method dispatches to `download_to_file` if `src_dst` is a sequence of tuples,
        or to `download_to_object` otherwise.

        When downloading to objects:
            - Supports various file types like Parquet, CSV, XLSX, and JSON.
            - If the file extension is not recognized, it returns an `io.BytesIO` object.
            - For CSV files, keyword arguments like `header`, `delimiter`, `encoding` can be passed via `**kwargs`.
            - For XLSX files, keyword arguments like `header` can be passed via `**kwargs`.

        Args:
            src_dst (Sequence[tuple[str, str | Path]] | str | list[str]):
                Sequence of tuples of (GCS URI, local file path) for file download,
                or a single GCS URI or list of GCS URIs for object download.
            dtype (dict | None, optional): Optional dictionary specifying data types for columns,
                primarily for Pandas DataFrames when downloading to objects (e.g., when reading CSV or Parquet).
                If provided (via `**kwargs`). Defaults to None.
            **kwargs (Any): Additional keyword arguments. These are passed to the underlying file
                reading functions (e.g., `pd.read_parquet`, `pd.read_csv`) when downloading to objects.

        Returns:
            None | dict[str, pd.DataFrame | io.BytesIO] | pd.DataFrame | io.BytesIO:
                - `None` if downloading to file (i.e., using tuples).
                - If downloading to objects:
                    - A dictionary mapping blob names to downloaded objects if multiple URIs result in multiple objects.
                    - The type of object depends on the file extension.

        Raises:
            TypeError: If `src_dst` is not a supported type.
            FileNotFoundError: If a file in a bucket does not exist.
            Other exceptions may be raised by GCS client or Pandas during file operations.
        """
        # File download (sequence of tuples)
        if isinstance(src_dst, (list, zip)):
            first_item = next(iter(src_dst), None)
            if first_item and isinstance(first_item, tuple):
                src_dst = cast("Sequence[tuple[str, str | Path]]", src_dst)
                return self.download_to_file(src_dst, **kwargs)

        # Object download (str/list[str])
        if isinstance(src_dst, (str, list)):
            src_dst = cast("str | list", src_dst)
            return self.download_to_object(src_dst, **kwargs)

        msg = "src_dst must be a sequence of tuples, string, or list of strings"
        raise TypeError(msg)

    def download_to_file(
        self,
        src_dst: Sequence[tuple[str, str | Path]],
    ) -> None:
        """Downloads files from GCS to local file paths.

        Args:
            src_dst (Sequence[tuple[str, str | Path]]):
                Sequence of tuples of (GCS URI, local file path) where the files will be downloaded.

        Raises:
            FileNotFoundError: If a file in a bucket does not exist.
            ValueError: If a GCS URI contains wildcards, which are not supported for direct file downloads.
        """
        for gcs_uri, local_file_path in src_dst:
            # Check for wildcards which are not supported for direct file downloads
            # TODO: Support wildcards only if a bool/Path to download the files as is (according to GCS) is passed
            if any(wildcard in gcs_uri for wildcard in ["*", "?", "[", "]", "{", "}"]):
                msg = (
                    f"Wildcards are not supported for direct file downloads. "
                    f"URI '{gcs_uri}' contains wildcards. "
                    f"Use download_to_object() instead for glob pattern matching."
                )
                raise ValueError(msg)

            bucket_name, file_path = GCSUriUtils.get_components(gcs_uri)
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            if blob.exists():
                blob.download_to_filename(str(local_file_path))
            else:
                msg = f"Blob {file_path} does not exist in bucket {bucket_name}"
                # In the future also raise 'google.cloud.exceptions.NotFound' in an ExceptionGroup (Python 3.11)
                raise FileNotFoundError(msg)

    def download_to_object(
        self,
        gcs_uris: str | list[str],
        dtype: dict | None = None,
        **kwargs: Any,  # Use ParamSpec in future  # noqa: ANN401
    ) -> dict[str, pd.DataFrame | io.BytesIO]:
        """Downloads file(s) from GCS into Python objects.

        Supports various file types like Parquet, CSV, XLSX, and JSON.
        If the file extension is not recognized, it returns an `io.BytesIO` object.

        For CSV files, keyword arguments like `header`, `delimiter`, `encoding` can be passed.
        For XLSX files, keyword arguments like `header` can be passed.

        Args:
            gcs_uris (str | list[str]): A single GCS URI or a list of GCS URIs to download.
            dtype (dict | None): Optional dictionary specifying data types for columns, primarily for
                Pandas DataFrames (e.g., when reading CSV or Parquet).
            **kwargs (Any): Additional keyword arguments passed to the underlying file reading
                functions (e.g., `pd.read_parquet`, `pd.read_csv`).

        Returns:
            dict[pd.DataFrame | io.BytesIO]: A dictionary mapping blob names to the downloaded objects.
                The type of object depends on the file extension.

        Raises:
            FileNotFoundError: If a file in a bucket does not exist.
        """
        import pandas as pd

        if not isinstance(gcs_uris, list):
            gcs_uris = [gcs_uris]

        data_dict = {}
        for blob in (blob for uri in gcs_uris for blob in self.uri_to_blobs(uri)):
            if not blob.exists():  # Likely won't happen due to uri_to_blobs handling
                msg = f"Blob {blob.name} does not exist in bucket {blob.bucket.name}"
                # In the future also raise 'google.cloud.exceptions.NotFound' in an ExceptionGroup (Python 3.11)
                raise FileNotFoundError(msg)

            data = io.BytesIO(blob.download_as_bytes())

            file_name = cast("str", blob.name)
            file_extension = next((ext.lstrip(".") for ext in self.KNOWN_EXTENSIONS if file_name.endswith(ext)), None)

            if file_extension == "parquet":
                file_obj = pd.read_parquet(data, **kwargs)
                if dtype:
                    file_obj = file_obj.astype(dtype)

            elif file_extension == "csv":
                csv_kwargs = kwargs.copy()
                csv_kwargs.setdefault("encoding", "utf-8")
                file_obj = pd.read_csv(data, dtype=dtype, **csv_kwargs) if dtype else pd.read_csv(data, **csv_kwargs)

            elif file_extension == "xlsx":
                xlsx_kwargs = kwargs.copy()
                xlsx_kwargs.setdefault("engine", "openpyxl")
                file_obj = (
                    pd.read_excel(data, dtype=dtype, **xlsx_kwargs) if dtype else pd.read_excel(data, **xlsx_kwargs)
                )

            elif file_extension == "json":
                json_kwargs = kwargs.copy()
                file_obj = pd.read_json(data, **json_kwargs)

            else:
                file_obj = data

            # If no recognized format, return the file object itself
            data_dict[blob.name] = file_obj

        return data_dict

    @overload
    def upload(
        self,
        src_dst: Sequence[tuple[str | Path, str]],
        metadata: dict | None = None,
        **kwargs: Any,  # Use ParamSpec in future  # noqa: ANN401
    ) -> None: ...

    @overload
    def upload(
        self,
        src_dst: Sequence[tuple[object, str]],
        metadata: dict | None = None,
        **kwargs: Any,  # Use ParamSpec in future  # noqa: ANN401
    ) -> None: ...

    def upload(
        self,
        src_dst: Sequence[tuple[str | Path, str]] | Sequence[tuple[object, str]],
        metadata: dict | None = None,
        **kwargs: Any,  # Use ParamSpec in future
    ) -> None:
        """Uploads local files or in-memory Python objects to GCS.

        This method serves as a dispatcher for uploading either files from the
        local filesystem or Python objects directly to GCS. You must provide
        a sequence of (source, GCS URI) tuples.

        Metadata can be provided for the uploaded objects. Environment variables
        like `DAG_ID`, `RUN_ID`, `NAMESPACE`, `POD_NAME`, `GITHUB_SHA` are
        automatically added to the metadata if present.

        For object uploads, the method attempts to infer the file type from the
        GCS URI's extension (e.g., .parquet, .csv, .xlsx, .json) and uses
        appropriate serialization methods (e.g., `to_parquet` for Pandas DataFrames).

        Args:
            src_dst (Sequence[tuple[str | Path, str]] | Sequence[tuple[object, str]]):
                Sequence of tuples where each tuple contains (source, GCS URI).
                Source can be local file paths (str or Path) or Python objects.
                Supported object types depend on the file extension of the `gcs_uri`
                (e.g., `pd.DataFrame` for .parquet, .csv, .xlsx; `str` for .json).
            metadata (dict | None): Optional dictionary of metadata to associate with the uploaded GCS object(s).
            **kwargs (Any): Additional keyword arguments passed to the underlying upload or serialization functions
                (e.g., `pd.DataFrame.to_parquet`, `pd.DataFrame.to_csv`).

        Raises:
            TypeError: If `src_dst` is not a supported type.
            ValueError: If uploading an object and no compatible file extension is found in the `gcs_uri`.
        """
        if not src_dst:
            return  # Empty input

        # Separate file and object uploads for mixed-type support
        file_uploads = []
        object_uploads = []

        for source, gcs_uri in src_dst:
            if isinstance(source, (str, Path)):
                file_uploads.append((source, gcs_uri))
            else:
                object_uploads.append((source, gcs_uri))

        # Upload files if any
        if file_uploads:
            self.upload_file(src_dst=file_uploads, metadata=metadata)

        # Upload objects if any
        if object_uploads:
            self.upload_object(src_dst=object_uploads, metadata=metadata, **kwargs)

    def upload_file(
        self,
        src_dst: Sequence[tuple[str | Path, str]],
        metadata: dict | None = None,
    ) -> None:
        """Uploads local file(s) to GCS.

        Metadata can be provided, and common environment variables are
        automatically included.

        Args:
            src_dst (Sequence[tuple[str | Path, str]]):
                Sequence of tuples containing (local file path, GCS URI) pairs to upload from the local filesystem.
            metadata (dict | None): Optional dictionary of metadata for the GCS object(s).
        """
        metadata = metadata or {}

        # Add environment variables to metadata
        env_vars = ["DAG_ID", "RUN_ID", "NAMESPACE", "POD_NAME", "GITHUB_SHA"]
        for var in env_vars:
            if var in os.environ:
                metadata.setdefault(var, os.environ[var])

        for file, gcs_uri in src_dst:
            bucket_name, file_path = GCSUriUtils.get_components(gcs_uri)
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            blob.metadata = metadata
            blob.upload_from_filename(str(file))

    def upload_object(
        self,
        src_dst: Sequence[tuple[object, str]],
        metadata: dict | None = None,
        **kwargs: Any,  # Use ParamSpec in future  # noqa: ANN401
    ) -> None:
        """Uploads Python object(s) to GCS.

        The method attempts to serialize objects based on the GCS URI's file
        extension (e.g., .parquet, .csv, .xlsx, .json).
        Metadata can be provided, and common environment variables are
        automatically included.

        Args:
            src_dst (Sequence[tuple[object, str]]):
                Sequence of tuples containing (Python object, GCS URI) pairs to upload. Supported types include
                `pd.DataFrame` (for .parquet, .csv, .xlsx) and `str` (for .json).
            metadata (dict | None): Optional dictionary of metadata for the GCS object(s).
            **kwargs (Any): Additional keyword arguments passed to the serialization
                functions (e.g., `to_parquet`, `to_csv`).

        Raises:
            ValueError: If no compatible file extension is found in the `gcs_uri`
                for serializing the object.
        """
        import pandas as pd

        metadata = metadata or {}

        # Add environment variables to metadata
        env_vars = ["DAG_ID", "RUN_ID", "NAMESPACE", "POD_NAME", "GITHUB_SHA"]

        for var in env_vars:
            if var in os.environ:
                metadata.setdefault(var, os.environ[var])

        for object_to_upload, gcs_uri in src_dst:
            bucket_name, file_path = GCSUriUtils.get_components(gcs_uri)
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            blob.metadata = metadata

            # Determine file type
            file_extension = next((ext.lstrip(".") for ext in self.KNOWN_EXTENSIONS if file_path.endswith(ext)), None)

            # Handle based on file type
            if file_extension == "parquet" and isinstance(object_to_upload, pd.DataFrame):
                parquet_kwargs = kwargs.copy()
                file_obj = io.BytesIO()
                object_to_upload.to_parquet(file_obj, **parquet_kwargs)
                file_obj.seek(0)
                blob.upload_from_file(file_obj)

            elif file_extension == "csv" and isinstance(object_to_upload, pd.DataFrame):
                csv_kwargs = kwargs.copy()
                csv_kwargs.setdefault("encoding", "utf-8")
                csv_kwargs.setdefault("index", False)
                csv_string = object_to_upload.to_csv(**csv_kwargs)
                blob.upload_from_string(csv_string)

            elif file_extension == "xlsx" and isinstance(object_to_upload, pd.DataFrame):
                xlsx_kwargs = kwargs.copy()
                xlsx_kwargs.setdefault("index", False)
                file_obj = io.BytesIO()
                object_to_upload.to_excel(file_obj, **xlsx_kwargs)
                file_obj.seek(0)
                blob.upload_from_file(file_obj)

            elif file_extension == "json" and isinstance(object_to_upload, str):
                json_string = json.dumps(object_to_upload)
                blob.upload_from_string(json_string)

            else:
                msg = (
                    f"No compatible file extension '{file_extension}' found for object type "
                    f"'{type(object_to_upload)!s}'."
                )
                raise ValueError(msg)
