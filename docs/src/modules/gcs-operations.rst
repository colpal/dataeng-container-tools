GCS Operations
==============

The Google Cloud Storage (GCS) module provides tools for interacting with Google Cloud Storage, allowing you to upload and download files in various formats.


Credentials
-----------

Credentials are checked in 3 locations by default. The first is the positional argument ``gcs_secret_location``, which takes a ``str`` or ``Path`` to any particular file.

.. code-block:: python

    gcs = GCSFileIO(gcs_secret_location="/path/to/gcs-credentials.json")

There are two additional boolean positional parameters, ``use_cla_fallback`` and ``use_file_fallback``, both defaulted to ``True``.

If no ``gcs_secret_location`` is provided, the ``SecretLocations`` file path for ``GCS`` will be checked instead. See :ref:`command-line-secret-locations` and :doc:`secrets-handling` for more information.

Finally, if ``SecretLocations`` does not provide any secret, then ``GCSFileIO.DEFAULT_SECRET_PATHS`` or ``/vault/secrets/gcp-sa-storage.json`` will be used instead.

Note that invalid files may cause exceptions; the only time the next location is checked is when the file is empty or does not exist.


Downloading and Uploading Files
-------------------------------

The ``GCSFileIO`` class can be summarized with 2 main operations:
    | ``download``
    |   ``download_to_file``
    |   ``download_to_object``
    | ``upload``
    |   ``upload_file``
    |   ``upload_object``

Both ``download`` and ``upload`` dispatch to more specialized operations such as ``download_to_file``.

As their names suggest, each operation performs a download or upload using disk files or objects. They take a GCS URI ``str`` which is in the general format ``gs://bucket/additional-paths/file.extension``. See :ref:`command-line-input-output` which helps build these URIs.

They will also attempt their best at parsing file types based on extensions within the URI. The currently supported file extensions are:

.. autoattribute:: dataeng_container_tools.modules.gcs.GCSFileIO.KNOWN_EXTENSIONS

The following sections :ref:`download-section` and :ref:`upload-section` will explain basic syntax. The other sections will show more niche use cases.


.. _download-section:

Download
________

.. py:function:: GCSFileIO.download(src_to_dst, **kwargs)

The first input is ``src_dst``. This handles the type of operation, file type, and destination type. It can be a string ``"gcs_uri"`` or a tuple in the format ``(source_gcs_uri, destination_file)``.

.. code-block:: python

    # If provided a string, will download to an object
    data = gcs.download(src_dst="gs://my-bucket/data.csv")

    # If provided a 2-sized tuple, will send the data to a local file instead
    gcs.download(src_dst=("gs://my-bucket/data.csv", "./data.csv"))


    # Alternate Syntax: we can ommit the src_dst keyword. The follow is equivalent to above
    gcs.download(("gs://my-bucket/data.csv", "./data.csv"))

Additionally, if it is provided a list instead of a ``str`` or ``tuple``, download will act in batches and perform multiple operations.

.. code-block:: python

    # If provided a list, will parse through all items in a batch operation
    data = gcs.download([
        "gs://my-bucket/data.csv",
        ("gs://my-bucket/data.csv", "./data.csv")
    ])

If provided ``kwargs`` (or additional parameters), ``download`` will pass them to relevant object based download operations.

.. code-block:: python

    # Passes nrows and on_bad_lines args to pd.read_csv
    # Only grab the first 10 rows of the csv
    # Skip bad lines
    data = gcs.download("gs://my-bucket/data1.csv", nrows=10, on_bad_lines="skip")

    # Unfortunately when dealing with batches, kwargs will apply to all items
    # Try not to mix file types together when using kwargs, keep them separated
    data = gcs.download([
        "gs://my-bucket/other-data1.json",
        "gs://my-bucket/other-data2.json",
    ], lines=True)  # Applies lines=True to both other-data.json and other-data2.json


The return of ``download`` is a ``dict[str, pd.DataFrame | BytesIO]`` where the key is the path of the file. This applies only to any URI downloaded as an object.

As of now, all data will attempt to be returned as ``pd.DataFrame``. If unrecognized, it will be a ``BytesIO`` object. Alternative returned formats may be supported in the future.

.. code-block:: python
    
    # Download a file as an object
    data = gcs.download("gs://my-bucket/data1.csv")

    # data will be a dict with the file path as the key
    # {
    #   "my-bucket/data.csv": df_object,
    # }

    # Accessing the DataFrame
    df = data["my-bucket/data1.csv"]

    data2 = gcs.download([
        "gs://my-bucket/data1.csv",  # Will appear in dict output
        ("gs://my-bucket/data2.csv", "./data2.csv"),  # Will not appear in dict output
    ])

    data3 = gcs.download(("gs://my-bucket/data3.csv", "./data3.csv"))  # data3 will be an empty dict


    # Alternative Syntax: We can use python unpacking to avoid having to index. This is good if there is only one or few downloads
    # The comma specifies unpacking the .values() output of the dictionary
    df, = gcs.download("gs://my-bucket/data1.csv").values()


.. _upload-section:

Upload
______

.. py:function:: GCSFileIO.upload(src_to_dst, **kwargs)

The syntax for uploading is similar to downloading, but now ``src_to_dst`` only takes in a tuple or list of tuples in the format ``(source_file, destination_gcs_uri)`` or ``(source_object, destination_gcs_uri)``.

.. code-block:: python

    # Say we have some_df DataFrame and ./data1.csv file
    some_df = ...

    # Upload file to GCS
    gcs.upload(("./data1.csv", "gs://my-bucket/data1.csv"))

    # Upload object to GCS
    gcs.upload((some_df, "gs://my-bucket/data2.csv"))

    # Upload both to GCS
    gcs.upload([
        ("./data1.csv", "gs://my-bucket/data1.csv"),
        (some_df, "gs://my-bucket/data2.csv")
    ])

Any ``kwargs`` passed will go through relevant operations when uploading objects.

.. code-block:: python

    some_df = ...

    gcs.upload((some_df, "gs://my-bucket/data.csv"), mode="a")

Since upload always returns ``None`` there is no need to do anything further.


Generic Use Case
----------------

This is what a full usage of the GCS module may look like from start to finish.

.. code-block:: python

    from dataeng_container_tools import GCSFileIO
    import pandas as pd

    # Initialize the GCS client with your credentials
    gcs = GCSFileIO()

    # Download a file from GCS to a DataFrame
    df, = gcs.download("gs://my-bucket/path/to/data.csv").values()

    # Process the data
    df['new_column'] = df['existing_column'] * 2

    # Upload the modified DataFrame back to GCS
    gcs.upload((df, "gs://my-bucket/path/to/processed_data.csv"))

With file operations instead of object operations it looks like:

.. code-block:: python

    from dataeng_container_tools import GCSFileIO
    import pandas as pd

    # Initialize the GCS client with your credentials
    gcs = GCSFileIO()

    # Download a file from GCS to a local file (notice no return unlike with to object)
    gcs.download(("gs://my-bucket/path/to/data.csv", "./local-folder/local-file.csv"))

    # Upload a local file to GCS (can also choose to omit the src_to_dst keyword arg)
    gcs.upload(("./local-folder/local-file.csv", "gs://my-bucket/other-path/to/data.csv"))


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
    gcs.upload((result_df, "gs://my-bucket/output.parquet"))

    gcs.upload(
        src_to_dst=(result_df, "gs://my-bucket/output.csv"),
        header=True,
        index=False,
    )


Batch Operations
----------------

Batch operations was shown before when providing a single list of tuples.

If you prefer to use two independent lists to manage the src and dst, Python's `zip <https://docs.python.org/3.3/library/functions.html#zip>`_ is also accepted.

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
        df["processed"] = True
        processed_files.append(df)
    
    upload_files = [
        "gs://my-bucket/processed/file1.csv",
        "gs://my-bucket/processed/file2.csv",
        "gs://my-bucket/processed/file3.csv",
    ],

    # Upload the processed files
    gcs.upload(zip(processed_files, upload_files))


Download: Globs and Wildcards
-----------------------------

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


Upload: Metadata
----------------

The upload function has a custom ``metadata: dict`` input parameter.

As of now, the following environment variables are detected and inserted directly into 
the uploaded GCS blobs: ``DAG_ID, RUN_ID, NAMESPACE, POD_NAME, GITHUB_SHA``.

.. code-block:: python

    from dataeng_container_tools import GCSFileIO
    import pandas as pd
    from datetime import datetime

    gcs = GCSFileIO()

    metadata = {
        "Content-Type": "text/csv",
        "Custom-Time": "2024-01-15T10:30:00Z",
        "Cache-Control": "no-cache, max-age=3600",
        "Content-Language": "en-US",
        "Project": "data-analysis",  # Custom metadata
        "Dataset": "quarterly-sales"  # Custom metadata
    }

    # Upload with custom metadata
    gcs.upload(("./data.csv", "gs://my-bucket/data.csv"), metadata=metadata)


See `GCS metadata documentation <https://cloud.google.com/storage/docs/metadata>`_ for use cases.


Opting Out of the Module
------------------------

The ``GCSFileIO`` exposes the ``.client`` attribute for use. This is useful since the user 
still gains simple credential and authentication without needing to opt into APIs. There are several
reasons why this may be useful, for example if there's a functionality the module cannot provide or if
the user feels more comfortable using the Python GCS library directly.

.. code-block:: python

    from dataeng_container_tools import GCSFileIO

    gcs_client = GCSFileIO().client

    bucket = gcs_client.bucket("my_bucket")
    blob = bucket.blob("my_file.txt")

    if blob.exists():
        blob.download_to_filename("./my_file.txt")


Working with Local Files
------------------------

You can use the GCS module to work with local files by passing ``local=True`` into the class. Note that to make this work, an emulator will be needed.

Some emulators:

- `oittaa/gcp-storage-emulator <https://github.com/oittaa/gcp-storage-emulator>`_
- `fsouza/fake-gcs-server <https://github.com/fsouza/fake-gcs-server>`_
- `googleapis/storage-testbench <https://github.com/googleapis/storage-testbench/tree/main>`_

However, these emulators have a few issues. The first two do not support globs and will generally fail most operations. The third one only works in memory so the user cannot access internal files.

.. margin::

    .. note::
        Before ``v1.0``, ``local=True`` would operate on the local file system instead. This was removed; use `Pathlib <https://docs.python.org/3/library/pathlib.html>`_ instead.

.. code-block:: python

    from dataeng_container_tools import GCSFileIO

    # Initialize in local mode
    gcs = GCSFileIO(local=True)

    # Download a file to local disk
    gcs.download([("gs://emulated-gcs-bucket/file.csv", "./file.csv")])

    # Upload a local file
    gcs.upload([("./file.csv", "gs://emulated-gcs-bucket/file.csv")])
