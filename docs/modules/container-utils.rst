Container Utilities
===================

The Container Utilities module provides tools for detecting the current runtime environment, specifically whether the code is running inside a Docker container or in a local environment.

Basic Usage
-----------

The most common use case is to import the ``IS_LOCAL`` constant to conditionally execute code based on the environment:

.. code-block:: python

    from dataeng_container_tools import IS_LOCAL

    # Example 1: Conditional configuration
    if IS_LOCAL:
        # Use local development settings
        database_host = "localhost"
        debug_mode = True
        log_level = "DEBUG"
    else:
        # Use production/container settings
        database_host = "prod-db-server"
        debug_mode = False
        log_level = "INFO"

    # Example 2: Conditional file paths
    if IS_LOCAL:
        data_path = "./local_data/"
        config_file = "dev_config.json"
    else:
        data_path = "/app/data/"
        config_file = "/app/config/prod_config.json"

    # Example 3: Conditional service endpoints
    if IS_LOCAL:
        api_endpoint = "http://localhost:8080"
        use_ssl = False
    else:
        api_endpoint = "https://api.production.com"
        use_ssl = True
   
   # Example 4: Another syntax
   value = 1 if IS_LOCAL else 2

Advanced Usage
--------------

For more granular control, you can also import the underlying functions directly, although it is not reccomended and is likely not required:

.. code-block:: python

    from dataeng_container_tools.container_utils import is_docker, is_local

    # Check if running in Docker specifically
    if is_docker():
        print("Running inside a Docker container")
        # Container-specific logic here
    
    # Check if running locally (not in any container in general)
    if is_local():
        print("Running in local development environment")
        # Local development logic here

    # You can also use these in conditional expressions
    log_file = "app.log" if is_local() else "/var/log/app.log"
    port = 8000 if is_local() else 80

Environment Detection
---------------------

The module uses two methods to detect if the code is running in a Docker container:

1. Checks for the existence of ``/.dockerenv`` file (created by Docker)
2. Examines ``/proc/self/cgroup`` for Docker-specific control groups

The ``IS_LOCAL`` constant (and ``is_local()``) is the inverse of ``is_docker()`` - it returns ``True`` when not running in any container environment.
