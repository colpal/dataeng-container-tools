GCS Operations
==============

The Google Cloud Storage (GCS) module provides tools for interacting with Google Cloud Storage, allowing you to upload and download files in various formats.

Credentials
-----------

Credentials are checked in 3 locations by default. The first of which is the positional argument ``gcs_secret_location`` which takes a ``str`` or ``Path`` to any particular file.

.. code-block:: python

    gcs = GCSFileIO(gcs_secret_location="/path/to/gcs-credentials.json")

There are two additional boolean positional parameters, ``use_cla_fallback`` and ``use_file_fallback`` both defaulted to ``True``.

If no ``gcs_secret_location`` is provided, the ``SecretLocations`` file path for ``GCS`` will be checked instead. See :ref:`command-line-secret-locations` and :doc:`secrets-handling` for more information.

Finally if ``SecretLocations`` does not provide any secret, then ``GCSFileIO.DEFAULT_SECRET_PATHS`` or ``/vault/secrets/gcp-sa-storage.json`` will be used instead.

Note that invalid files may cause exceptions, the only time the next location is checked is when the file is empty or does not exist.

Downloading and Uploading Files
-------------------------------

The ``GCSFileIO`` class can be summarized with 2 main operations:
    | ``download``
    |   ``download_to_file``
    |   ``download_to_object``
    | ``upload``
    |   ``upload_file``
    |   ``upload_object``

Note that ``download`` and ``upload`` wraps around more specific operations such as ``download_to_file``.

As their name suggests, each operation performs a download or upload using disk files or objects. They take a GCS URI which is in the general format ``gs://bucket/path/file.extension``. See :ref:`command-line-input-output` which help builds these URIs.

With ``download`` to object, the output is a ``dict[str, pd.DataFrame | BytesIO]`` where the key is the path of the file. If you only expect one output then use unpacking on the dict' values. With more consider loops or iterators.

Here is what these operations might look like:

.. code-block:: python

    from dataeng_container_tools import GCSFileIO
    import pandas as pd

    # Initialize the GCS client with your credentials
    gcs = GCSFileIO()

    # Download a file from GCS to a DataFrame
    # 'df, = ...' unpacks the dict values, or use `[df] = ...` if you prefer that syntax
    df, = gcs.download("gs://my-bucket/path/to/data.csv").values()

    # Process the data
    df['new_column'] = df['existing_column'] * 2

    # Upload the modified DataFrame back to GCS
    gcs.upload("gs://my-bucket/path/to/processed_data.csv", objects_to_upload=df)

With file operations instead of object operations it looks like:
.. code-block:: python

    from dataeng_container_tools import GCSFileIO
    import pandas as pd

    # Initialize the GCS client with your credentials
    gcs = GCSFileIO()

    # Download a file from GCS to a local file (notice no return unlike with to object)
    gcs.download({"gs://my-bucket/path/to/data.csv": "./local-folder/local-file.csv"})

    # Upload a local file to GCS
    gcs.upload({"./local-folder/local-file.csv": "gs://my-bucket/other-path/to/data.csv"})

Working with Different File Formats
-----------------------------------

The GCS module supports various file formats including ``parquet``, ``csv``, ``xlsx``, and ``json``. If an unrecognized file type is downloaded, it will be a ``BytesIO`` object (similar to ``open`` files). However, for upload the operation will fail.

.. margin::

    .. note::
        Since ``v1.0``, ``pickle``/``pkl`` has been removed from support due to security concerns.

.. code-block:: python

    from dataeng_container_tools import GCSFileIO
    import pandas as pd

    gcs = GCSFileIO()

    # Download files in different formats
    parquet_df, = gcs.download("gs://my-bucket/data.parquet").values()
    csv_df, = gcs.download("gs://my-bucket/data.csv").values()
    excel_df, = gcs.download("gs://my-bucket/data.xlsx").values()
    json_df, = gcs.download("gs://my-bucket/data.json").values()

    # Process data
    result_df = pd.concat([parquet_df, csv_df])

    # Upload in different formats
    gcs.upload("gs://my-bucket/output.parquet", object_to_upload=result_df)

    gcs.upload(
        gcs_uris="gs://my-bucket/output.csv",
        objects_to_upload=result_df,
        header=True,
        index=False,
    )

Batch Operations
----------------

All operations support either a single GCS URI or a list. By inputting a list it will perform in batches.

.. code-block:: python

    from dataeng_container_tools import GCSFileIO

    gcs = GCSFileIO()

    # Download multiple files
    files = gcs.download([
        "gs://my-bucket/file1.csv",
        "gs://my-bucket/file2.csv",
        "gs://my-bucket/file3.csv",
    ])

    # Process the files
    processed_files = []
    for df in files:
        # Perform operations on each DataFrame
        df['processed'] = True
        processed_files.append(df)
    
    upload_files = [
        "gs://my-bucket/processed/file1.csv",
        "gs://my-bucket/processed/file2.csv",
        "gs://my-bucket/processed/file3.csv",
    ],

    # Upload the processed files
    gcs.upload(
        gcs_uris=dict(zip(upload_files, processed_files)),
        headers=0,
    )

And just like with single processing, file operations are supported:

.. code-block:: python

    from dataeng_container_tools import GCSFileIO

    gcs = GCSFileIO()

    # Download multiple files
    files = gcs.download({
        "gs://my-bucket/file1.csv": "./file1.csv",
        "gs://my-bucket/file2.csv": "./file2.csv",
        "gs://my-bucket/file3.csv": "./file3.csv",
    })

    # Upload the processed files
    gcs.upload(
        gcs_uris={
            "./file1.csv": "gs://my-bucket/other/file1.csv",
            "./file2.csv": "gs://my-bucket/other/file2.csv",
            "./file3.csv": "gs://my-bucket/other/file3.csv",
        },
        headers=0,
    )

    # Alternate syntax, many ways to do this. Just use any collections.abc.Mapping compatible input.
    local_files = ["./file1.csv", "./file2.csv", "./file3.csv"]
    upload_uris = ["gs://my-bucket/other/file1.csv", "gs://my-bucket/other/file2.csv", "gs://my-bucket/other/file3.csv"]
    gcs.upload(
        gcs_uris=dict(zip(local_files, upload_uris)),
        headers=0,
    )

Globs and Wildcards
-------------------

As specified with GCS, this library wrapper supports globs allowing the user to use patterns such as wildcards. This is only supported for downloading to objects since downloading to files requires a mapping and uploading has no need for it.

For more details on GCS glob patterns, see the `GCS documentation <https://cloud.google.com/storage/docs/json_api/v1/objects/list#list-objects-and-prefixes-using-glob>`_.

.. code-block:: python

    from dataeng_container_tools import GCSFileIO

    # Initialize the GCS client
    gcs = GCSFileIO()

    # Download all files from a directory in GCS
    # For example, if the bucket 'my-bucket' has:
    # - data/subdir/file1.csv
    # - data/subdir/file2.csv
    # - data/subdir/subsubdir/file3.csv
    # - data/other/file4.txt
    # - data/other/file5.csv
    # Files file1.csv and file2.csv will be downloaded
    downloaded_files1 = gcs.download("gs://my-bucket/data/subdir/*")

    # downloaded_files1 will be a dict:
    # {
    #   "data/subdir/file1.csv": df_file1,
    #   "data/subdir/file2.csv": df_file2,
    # }

    downloaded_files2 = gcs.download("gs://my-bucket/data/subdir/**")

    # downloaded_files2 will be a dict:
    # {
    #   "data/subdir/file1.csv": df_file1,
    #   "data/subdir/file2.csv": df_file2,
    #   "data/subdir/subsubdir/file3.csv": df_file3,
    # }

    downloaded_files3 = gcs.download("gs://my-bucket/**/*.csv")

    # downloaded_files3 will be a dict:
    # {
    #   "data/subdir/file1.csv": df_file1,
    #   "data/subdir/file2.csv": df_file2,
    #   "data/subdir/subsubdir/file3.csv": df_file3,
    #   "data/other/file5.csv": df_file5,
    # }

    # Accessing file data:
    for file_path, df_object in downloaded_files1.items():
        print(f"Processing file: {file_path}")
        print(df_object.head())

Working with Local Files
------------------------

You can use the GCS module to work with local files by passing ``local=True`` into the class. Note to make this work, an emulator will be needed.

Some emulators:

- `oittaa/gcp-storage-emulator <https://github.com/oittaa/gcp-storage-emulator>`_
- `fsouza/fake-gcs-server <https://github.com/fsouza/fake-gcs-server>`_

However, these emulators are a bit outdated and do not support globs at the time of writing.

.. margin::

    .. note::
        Before ``v1.0``, ``local=True`` would operate on the local file system instead. This was removed; use `Pathlib <https://docs.python.org/3/library/pathlib.html>`_ instead.

.. code-block:: python

    from dataeng_container_tools import GCSFileIO

    # Initialize in local mode
    gcs = GCSFileIO(local=True)

    # Download a file to local disk
    gcs.download({"/emulated-gcs-bucket/file.csv": "./file.csv"})

    # Upload a local file
    gcs.upload({"/emulated-gcs-bucket/file.csv", files="./file.csv"})
