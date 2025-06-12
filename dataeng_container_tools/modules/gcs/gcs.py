"""Tools for working with Google Cloud Storage (GCS).

Deals with receiving downloading and uploading files from/to GCP.
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
            gcs_uri: The GCS URI string to normalize.

        Returns:
            The normalized GCS URI string.
        """
        import posixpath

        gcs_uri = gcs_uri.removeprefix(GCSUriUtils.PREFIX)
        return GCSUriUtils.PREFIX + posixpath.normpath(gcs_uri)

    @staticmethod
    def get_components(gcs_uri: str) -> tuple[str, str]:
        """Extracts the bucket name and file path from a GCS URI.

        Args:
            gcs_uri: The GCS URI string.

        Returns:
            A tuple containing the bucket name and the file path.

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
        client (google.cloud.storage.Client): The Google Cloud Storage client instance.
        local (bool): A boolean indicating if the module is in local-only mode.
            If True, no actual GCS operations are performed.

    Examples:
        Initialize GCSFileIO and download a file to a local path:
            >>> gcs_io = GCSFileIO(gcs_secret_location="/path/to/your/gcp-sa-storage.json")
            >>> gcs_io.download(src_dst=[("gs://my-bucket/remote_file.txt", "local_file.txt")])

        Download a Parquet file to a Pandas DataFrame:
            >>> gcs_io = GCSFileIO(local=True) # Example for local mode
            >>> dataframes = gcs_io.download(src_dst="gs://my-bucket/data.parquet")
            >>> df = dataframes["data.parquet"]

        Upload a local file to GCS:
            >>> gcs_io.upload(src_dst=[("local_file_to_upload.txt", "gs://my-bucket/uploaded_file.txt")])

        Upload a Pandas DataFrame to GCS as a CSV:
            >>> import pandas as pd
            >>> df_to_upload = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
            >>> gcs_io.upload(src_dst=[(df_to_upload, "gs://my-bucket/uploaded_dataframe.csv")])
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
            gcs_secret_location: Path to the GCS service account JSON key file.
            local: If True, operates in local mode without GCS interaction. Should be used
                with a GCS local emulator. Defaults to False.
            use_cla_fallback: If True, attempts to use command-line arguments
                as a fallback for secret location if `gcs_secret_location` is not found.
                Defaults to True.
            use_file_fallback: If True, attempts to use the default secret file path
                as a fallback if other sources fail. Defaults to True.

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
            gcs_uri: The GCS URI, which can include glob patterns.

        Returns:
            An iterator yielding `google.cloud.storage.blob.Blob`
            objects matching the URI.
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
        Any,  # dict[str, pd.DataFrame | io.BytesIO]
    ]: ...

    def download(
        self,
        src_dst: str | list[str] | Sequence[tuple[str, str | Path]],
        **kwargs: Any,  # Use ParamSpec in future
    ) -> ...:
        """Downloads files from GCS to local file paths or Python objects.

        This method dispatches to `download_to_file` if `src_dst` is a sequence of tuples
        (for downloading to local files), or to `download_to_object` if `src_dst` is
        a string or list of strings (for downloading to Python objects).

        When downloading to objects:
            - Supports various file types like Parquet, CSV, XLSX, and JSON.
            - If the file extension is not recognized, it returns an `io.BytesIO` object.
            - For CSV files, keyword arguments like `header`, `delimiter`, `encoding` can be passed via `**kwargs`.
            - For XLSX files, keyword arguments like `header` can be passed via `**kwargs`.

        Args:
            src_dst:
                - For downloading to local files: A sequence of tuples, where each tuple is
                  (GCS URI, local file path). Example: `[("gs://bucket/file.txt", "local.txt")]`
                - For downloading to Python objects: A single GCS URI (str) or a list of GCS URIs (list[str]).
                  Example: `"gs://bucket/data.csv"` or `["gs://bucket/data1.parquet", "gs://bucket/data2.json"]`
            **kwargs:
                - `dtype`: Passed to `download_to_object`. Dictionary specifying
                  data types for columns, primarily for Pandas DataFrames. Defaults to None.
                - Other keyword arguments are passed to the underlying file reading functions
                  (e.g., `pandas.read_parquet`, `pandas.read_csv`) when downloading to objects.

        Returns:
            - `None` if downloading to local files (i.e., when `src_dst` is a sequence of tuples).
            - If downloading to Python objects (i.e., when `src_dst` is a string or list of strings):
              A dictionary mapping blob names to downloaded objects. The type of object
              depends on the file extension (e.g., `pd.DataFrame` for .parquet, .csv;
              `io.BytesIO` for unrecognized types).

        Raises:
            TypeError: If `src_dst` is not a supported type (neither a sequence of tuples,
                nor a string, nor a list of strings).
            FileNotFoundError: If a GCS blob specified in `src_dst` does not exist.
            ValueError: If a GCS URI for `download_to_file` contains wildcards.
            Other exceptions may be raised by the GCS client or Pandas during file operations.

        Examples:
            Download a single file to a local path:
                >>> gcs_io.download(src_dst=[("gs://my-bucket/config.json", "my_config.json")])

            Download multiple files to local paths:
                >>> files_to_download = [
                ...     ("gs://my-bucket/data.csv", "data/my_data.csv"),
                ...     ("gs://my-bucket/image.png", "images/my_image.png")
                ... ]
                >>> gcs_io.download(src_dst=files_to_download)

            Download a CSV file into a Pandas DataFrame:
                >>> data_objects = gcs_io.download(src_dst="gs://my-bucket/report.csv", delimiter=";")
                >>> report_df = data_objects["report.csv"]

            Download multiple files (Parquet and JSON) into objects:
                >>> object_dict = gcs_io.download(
                ...     src_dst=["gs://my-bucket/dataset.parquet", "gs://my-bucket/metadata.json"]
                ... )
                >>> parquet_df = object_dict["dataset.parquet"]
                >>> metadata_obj = object_dict["metadata.json"] # Likely an io.BytesIO object
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
            src_dst: Sequence of tuples, where each tuple is
                (GCS URI, local file path) indicating where the files will be downloaded.

        Raises:
            FileNotFoundError: If a GCS blob specified in `src_dst` does not exist.
            ValueError: If a GCS URI contains wildcards, which are not supported for direct file downloads.
                Use `download_to_object()` for glob pattern matching.
        """
        for gcs_uri, local_file_path in src_dst:
            # Check for wildcards which are not supported for direct file downloads
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

        For CSV files, keyword arguments like `header`, `delimiter`, `encoding` can be passed via `**kwargs`.
        For XLSX files, keyword arguments like `header` can be passed via `**kwargs`.

        Args:
            gcs_uris: A single GCS URI or a list of GCS URIs to download.
                Can include glob patterns for matching multiple files.
            dtype: Dictionary specifying data types for columns, primarily for
                Pandas DataFrames (e.g., when reading CSV or Parquet). Defaults to None.
            **kwargs: Additional keyword arguments passed to the underlying file reading
                functions (e.g., `pandas.read_parquet`, `pandas.read_csv`).

        Returns:
            A dictionary mapping blob names to the downloaded objects.
            The type of object depends on the file extension (e.g., `pd.DataFrame` for .parquet,
            .csv; `io.BytesIO` for unrecognized types or if the file is not a table format).

        Raises:
            FileNotFoundError: If a GCS blob specified by `gcs_uris` does not exist
                (though `uri_to_blobs` usually handles this by returning an empty iterator).
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

        This method dispatches to `upload_file` for local file uploads and
        `upload_object` for Python object uploads. You must provide a sequence
        of (source, GCS URI) tuples.

        Metadata can be provided for the uploaded objects. Environment variables
        like `DAG_ID`, `RUN_ID`, `NAMESPACE`, `POD_NAME`, `GITHUB_SHA` are
        automatically added to the metadata if present and not already specified.

        For object uploads, the method attempts to infer the file type from the
        GCS URI's extension (e.g., .parquet, .csv, .xlsx, .json) and uses
        appropriate serialization methods (e.g., `to_parquet` for Pandas DataFrames).

        Args:
            src_dst:
                A sequence of tuples, where each tuple contains (source, GCS URI).
                - For file uploads: `source` is a local file path (str or Path).
                  Example: `[("local_data.csv", "gs://bucket/remote_data.csv")]`
                - For object uploads: `source` is a Python object.
                  Supported object types depend on the file extension of the `gcs_uri`
                  (e.g., `pd.DataFrame` for .parquet, .csv, .xlsx; `str` for .json,
                  which will be `json.dumps`ed).
                  Example: `[(my_dataframe, "gs://bucket/df.parquet")]`
            metadata: A dictionary of metadata to associate with the
                uploaded GCS object(s). Defaults to None (an empty dictionary will be used).
            **kwargs: Additional keyword arguments passed to the underlying
                upload or serialization functions (e.g., `pandas.DataFrame.to_parquet`,
                `pandas.DataFrame.to_csv`).

        Raises:
            ValueError: If uploading an object and no compatible file extension is found
                in the `gcs_uri`, or if the object type is not supported for the extension.
            Other exceptions may be raised by the GCS client or Pandas during file operations.

        Examples:
            Upload a single local file:
                >>> gcs_io.upload(src_dst=[("path/to/my_report.pdf", "gs://my-bucket/reports/report.pdf")])

            Upload multiple local files with custom metadata:
                >>> files_to_upload = [
                ...     ("data.csv", "gs://my-bucket/data/current_data.csv"),
                ...     ("archive.zip", "gs://my-bucket/archives/backup.zip")
                ... ]
                >>> gcs_io.upload(src_dst=files_to_upload, metadata={"version": "1.2", "processed_by": "script_A"})

            Upload a Pandas DataFrame as a Parquet file:
                >>> import pandas as pd
                >>> df = pd.DataFrame({'colA': [1, 2], 'colB': ['x', 'y']})
                >>> gcs_io.upload(src_dst=[(df, "gs://my-bucket/dataframes/my_df.parquet")])

            Upload a string as a JSON file (will be json.dumps'd):
                >>> my_config_str = '{"key": "value", "settings": [1, 2, 3]}'
                >>> gcs_io.upload(src_dst=[(my_config_str, "gs://my-bucket/configs/app_config.json")])
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

        Metadata can be provided. Common environment variables (e.g., `DAG_ID`,
        `RUN_ID`) are automatically included in the metadata if present and not
        already specified.

        Args:
            src_dst: Sequence of tuples, where each tuple
                contains (local file path, GCS URI) pairs for files to upload from the local filesystem.
            metadata: A dictionary of metadata to associate with the
                GCS object(s). Defaults to None (an empty dictionary will be used).
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
        Metadata can be provided. Common environment variables (e.g., `DAG_ID`,
        `RUN_ID`) are automatically included in the metadata if present and not
        already specified.

        Args:
            src_dst: Sequence of tuples, where each tuple
                contains (Python object, GCS URI) pairs to upload. Supported object types include:
                - `pandas.DataFrame` (for .parquet, .csv, .xlsx extensions in GCS URI)
                - `str` (for .json extension in GCS URI; the string will be `json.dumps`'d)
            metadata: A dictionary of metadata to associate with the
                GCS object(s). Defaults to None (an empty dictionary will be used).
            **kwargs: Additional keyword arguments passed to the serialization
                functions (e.g., `pandas.DataFrame.to_parquet`, `pandas.DataFrame.to_csv`).

        Raises:
            ValueError: If no compatible file extension is found in the `gcs_uri`
                for serializing the object, or if the object type is not supported for that extension.
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
