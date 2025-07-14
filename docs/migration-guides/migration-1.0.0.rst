Migration Guide 1.0.0
=====================

.. warning::
   This documentation is currently under construction (TBD).

This contains information on important changes that would be important to know 
when migrating a container using DataEng Container Tools ``v0.6.4`` to ``v1.0.0``

Rename
------

The following files, classes, functions have been renamed. References in this page will use the new names:

- ``cla.py``
   - Class: ``custom_command_line_argument -> CustomCommandLineArgument``
   - Class: ``command_line_argument_type -> CommandLineArgumentType``
   - Class: ``command_line_secret -> CommandLineSecret``
   - Class: ``command_line_arguments -> CommandLineArguments``
      - Function: ``cla.CommandLineArguments.get_secret_locations -> secrets_manager.SecretLocations`` (file change, function to class)
      - Function: ``cla.CommandLineArguments.get_secrets -> secrets_manager.SecretManager.get_secrets`` (file change)

- ``gcs.py -> modules/gcs/gcs.py`` (relocation, imports does not change)
   - Class: ``gcs_file_io -> GCSFileIO``
      - Function: ``GCSFileIO.__get_parts (instancemethod) -> GCSUriUtils.get_components (staticmethod)``

- ``db.py -> modules/datastore/datastore.py``
   - Class: ``Db -> Datastore``

- ``safe_stdout.py -> safe_textio.py``
   - Class: ``safe_stdout -> SafeTextIO``
      - Function: ``sys.stdout.add_words (instancemethod) -> SafeTextIO.add_words (classmethod)``
   - Function: ``setup_default_stdout -> setup_default_stdio``
   - Function: ``safe_stdout.setup_stdout -> secrets_manager.process_secret_folder`` (file change)


Removed
-------

- Python 3.7 and 3.8 support has been dropped
- Removed ``BQ`` (BigQuery) module (and ``bq.py``)
- Removed ``simple_setup.py``
- Removed all ``exceptions`` (and ``exceptions.py``)

- ``cla.py``
   - Removed unused ``CommandLineArguments.get_pandas_kwargs``
   - Removed unused ``CommandLineArguments.check_args``
   - Removed unused ``CommandLineArguments.__input_pandas_kwargs`` (and its args ``--input_pandas_kwargs``)
   - Removed unused ``CommandLineArguments.__output_pandas_kwargs`` (and its args ``--output_pandas_kwargs``)

- ``gcs.py``
   - The old ``local=True`` system in GCS has been removed (in place for a different local system)

Breaking
--------

- CommandLineArguments will now use ``parse_known_args=True`` by default
- ``gcs.py``
   - Functions ``download_file_to_object``, ``download_files_to_objects``, ``download_file_to_disk``, ``download_files_to_disk`` were consolidated to ``download``
   - Functions ``upload_file_from_disk``, ``upload_files_from_disk``, ``upload_file_from_object``, ``upload_files_from_objects`` were consolidated to ``upload``
