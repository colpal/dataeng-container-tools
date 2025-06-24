Creating Modules
================

Module Integration for Secrets
------------------------------

The ``BaseModule`` class automatically registers with ``SecretManager`` to centralize secret handling:

.. code-block:: python

    from dataeng_container_tools.modules.base_module import BaseModule

    # Create a custom module that automatically registers with SecretManager
    class MyCustomModule(BaseModule):
        MODULE_NAME = "CUSTOM"
        DEFAULT_SECRET_PATHS = {
            "custom_api_key": "/path/to/api_key.json",
            "custom_config": "/path/to/config.json"
        }

        def __init__(self):
            # Module-specific initialization
            print("Hello world!")

    # Testing
    # The module's secret paths are now registered with SecretManager
    my_module = MyCustomModule()

    # Access all registered secret paths
    all_paths = SecretManager.get_module_secret_paths()
    print(f"All registered paths: {all_paths}")
    # Should include the CUSTOM module paths

Additionally, you may add an attribute to the SecretLocations class in secrets_manager.py:

.. code-block:: python

    @final
    class SecretLocations(dict[str, str]):

        # Add type hints for common attributes
        GCS: str
        SF: str
        DS: str
        custom_api_key: str  # Add here
        custom_config: str   # Add here

When using large libraries that take a long time to load, do not import them at the top of the file, 
as this will cause long boot times when loading the library (even if the module is not used). 
Instead, import at three locations:

- In a ``TYPE_CHECKING`` block to satisfy linting requirements
- As needed in code where they are used
- In the ``__init__`` method to ensure they are already loaded during actual use

.. code-block:: python

    from dataeng_container_tools.modules.base_module import BaseModule

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        import pandas as pd  # Avoid large libraries at top level except for type checking

    # Create a custom module that automatically registers with SecretManager
    class MyCustomModule(BaseModule):
        MODULE_NAME = "CUSTOM"
        DEFAULT_SECRET_PATHS = {
            "custom_api_key": "/path/to/api_key.json",
            "custom_config": "/path/to/config.json"
        }

        def __init__(self):
            import pandas as pd  # Load large libraries in __init__ even if not used directly now

            # Module-specific initialization
            print("Hello world!")
        
        def some_function(self):
            import pandas as pd  # Import in any function that requires it

            some_var = pd.DataFrame()
