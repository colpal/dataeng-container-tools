Creating Modules
================

.. warning::
   This documentation is currently under construction (TBD).

Module Integration for Secrets
------------------------------

The ``BaseModule`` class automatically registers with ``SecretManager`` to centralize secret handling:

.. code-block:: python

    from dataeng_container_tools.modules.base_module import BaseModule

    # Create a custom module that automatically registers with SecretManager
    class MyCustomModule(BaseModule):
        MODULE_NAME = "CUSTOM"
        DEFAULT_SECRET_PATHS = {
            "api_key": "/path/to/api_key.json",
            "config": "/path/to/config.json"
        }

        def __init__(self):
            super().__init__()
            # Module-specific initialization

    # Testing
    # The module's secret paths are now registered with SecretManager
    my_module = MyCustomModule()

    # Access all registered secret paths
    all_paths = SecretManager.get_module_secret_paths()
    print(f"All registered paths: {all_paths}")
    # Should include the CUSTOM module paths

Additionally you may also add an attribute to the SecretLocations class in secrets_manager.py
