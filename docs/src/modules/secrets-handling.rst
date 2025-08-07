Secrets Handling
================

The DE Container Tools package provides robust tools for handling secrets securely with the `SecretManager` and `SafeTextIO` modules.

Basic Secret Management (SecretManager)
---------------------------------------

Here's a basic example of using the ``SecretManager`` class:

.. code-block:: python

    from dataeng_container_tools import SecretManager, SecretLocations

    def use_secret(secret: str) -> ...:
        pass

    # Access secrets
    custom_secret_path: str = (SecretManager.DEFAULT_SECRET_FOLDER / "custom_secret.json").as_posix()
    use_secret(SecretManager.secrets[custom_secret_path])

    # Get default paths from registered modules
    default_paths = SecretManager.get_module_secret_paths()
    print(f"Default secret paths: {default_paths}")

Secret Locations (SecretLocations)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SecretLocations class is a singleton that gives quick access to secret string paths. By default it is initialized with some basic paths. When ``--secret_locations`` arg is passed and CommandLineArguments is initialized, SecretLocations will update with new attributes and override any existing.  See :ref:`command-line-secret-locations` for more details.

.. code-block:: python

    from dataeng_container_tools import SecretLocations, GCSFileIO

    # Get common secret paths
    gcs_secret_path = SecretLocations().GCS
    sf_secret_path = SecretLocations().SF
    db_secret_path = SecretLocations().DB

    # Use GCS path
    gcs_file_io = GCSFileIO(gcs_secret_path)

    # If custom secret CUSTOM is provided via CLA (type hinting not available)
    custom_secret_path: str = SecretLocations().CUSTOM

    # Can also access via indexing
    gcs_secret_path = SecretLocations()["GCS"]
    custom_secret_path: str = SecretLocations()["CUSTOM"]


Safe Handling of Output with Secrets (SafeTextIO)
-------------------------------------------------

SafeTextIO is a subclass of TextIO that replaces secrets with ``*``. By default, this automatically initializes and overrides ``sys.stdout`` and ``sys.sterr`` which covers ``print`` and ``logging``. It is possible to also add your own TextIO output such as a file.

.. note::

    When secrets are parsed by SecretManager, their secrets will automatically be added to all instances of SafeTextIO. Rarely will you need to add your own.

.. code-block:: python

    from dataeng_container_tools import SafeTextIO
    from pathlib import Path

    # Define additional sensitive values to censor
    SafeTextIO.add_words(["secret_api_key", "my_password"])

    # Now when you print sensitive information, it will be censored
    api_key = "secret_api_key"
    password = "my_password"

    print(f"API Key: {api_key} Something")
    print(f"Password: {password}Something")

    # Create a custom SafeTextIO for a specific file
    log_file_path = Path("log.txt")
    with log_file_path.open("w") as log_file:
        safe_logger = SafeTextIO(textio=log_file)
        safe_logger.write(f"Sensitive info: {api_key}")

Output:

.. code-block:: text

    API Key: ************** Something
    Password: ***********Something

log.txt

.. code-block:: text

    Sensitive info: **************

Parsing Secret Files
--------------------

You can parse secret files using the ``SecretManager``. Secrets parsed this way are automatically hidden by SafeTextIO.

.. code-block:: python

    from dataeng_container_tools import SecretManager
    from pathlib import Path

    # Parse a specific secret file
    gcs_secret = SecretManager.parse_secret("/path/to/gcs-credentials.json")

    # If the secret is a JSON file, you can access its contents as a dictionary
    if isinstance(gcs_secret, dict):
        project_id = gcs_secret["project_id"]
        print(f"Project ID: {project_id}")  # Hidden by SafeTextIO (*** censored)
    else:  # Secret is a string
        print(f"Some secret: {gcs_secret}")  # Hidden by SafeTextIO (*** censored)

    # Alternatively process all secret files in a directory
    SecretManager.process_secret_folder(Path("/custom/secrets/path"))

    # Access all parsed secrets
    all_secrets = SecretManager.secrets

    # Access a particular secret via its path
    some_secret = all_secrets["/custom/secrets/path/secret.json"]
