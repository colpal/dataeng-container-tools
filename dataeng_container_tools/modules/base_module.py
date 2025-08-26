"""Base module for data engineering container tools.

This module provides a base class for all specialized modules such as GCS, DB, etc.
It offers common functionality and a consistent interface that all module implementations
should follow, ensuring a uniform API across the library.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar

from dataeng_container_tools.secrets_manager import SecretLocations, SecretManager

if TYPE_CHECKING:
    import os

logger = logging.getLogger("Container Tools")


class ModuleRegistryMeta(type):
    """Metaclass that automatically registers BaseModule subclasses with SecretManager.

    This metaclass intercepts the creation of subclasses of BaseModule.
    If a subclass defines `MODULE_NAME` and `DEFAULT_SECRET_PATHS` attributes,
    it automatically registers that module with the `SecretLocations` manager.
    This allows for centralized management of default secret paths for different
    modules.
    """

    def __init__(cls, name: str, bases: tuple[type, ...], namespace: dict[str, Any]) -> None:
        """Initializes the class and registers it with SecretManager if applicable.

        Args:
            name: The name of the class being created.
            bases: A tuple of the base classes of the class being created.
            namespace: A dictionary containing the attributes and methods of the
                class being created.
        """
        super().__init__(name, bases, namespace)
        # Only register subclasses of BaseModule, not BaseModule itself
        if name != "BaseModule" and hasattr(cls, "MODULE_NAME") and hasattr(cls, "DEFAULT_SECRET_PATHS"):
            SecretLocations.register_module(cls)
            logger.debug("Auto-registered module %s with SecretManager", getattr(cls, "MODULE_NAME", "Unknown"))


class BaseModule(metaclass=ModuleRegistryMeta):
    """Base class for all specialized modules.

    This abstract class defines the common interface and functionality that
    all module implementations should follow. It provides methods for handling
    secrets, initialization, and common utilities.

    Subclasses are automatically registered with the `SecretManager` if they
    define `MODULE_NAME` and `DEFAULT_SECRET_PATHS` class attributes, thanks to
    the `ModuleRegistryMeta` metaclass.

    Attributes:
        MODULE_NAME: Identifies the module type for logging and display.
            Should be overridden by subclasses.
        DEFAULT_SECRET_PATHS: Default secret file paths for this module.
            Keys are unique descriptive names for secrets (e.g., "GCS_API", "API_PEM"),
            and values are their corresponding file paths. Should be overridden by subclasses.
        client: Client instance used to interact with external services. This is
            typically initialized in the subclass's `__init__` method.

    Examples:
        Creating a specialized module inheriting from BaseModule:

        >>> class APIClient(BaseModule):
        ...     MODULE_NAME = "API"
        ...     DEFAULT_SECRET_PATHS = {
        ...         "API_CONFIG": "/vault/secrets/api-config.json"
        ...     }
        ...
        ...     def __init__(self, **kwargs):
        ...         super().__init__()
        ...         # API-specific initialization
        ...         print(f"{self.MODULE_NAME} module initialized.")
        >>> api = APIClient()
        API module initialized.
        >>> print(api.MODULE_NAME)
        API
        >>> print(BaseModule.get_default_secret_paths()) # Default for BaseModule itself
        {}
        >>> print(API.get_default_secret_paths())
        {"API_CONFIG": PosixPath("/vault/secrets/api-config.json")}
    """

    # Class attributes to identify the module type and its default secret paths
    MODULE_NAME: ClassVar[str] = "BASE"
    DEFAULT_SECRET_PATHS: ClassVar[dict[str, str]] = {}

    def __init__(self) -> None:
        """Initializes the base module.

        Currently, this base initializer only sets up a placeholder for the client.
        Subclasses should call `super().__init__()` and then perform their
        specific client initialization and other setup tasks.
        """
        self.client: Any = ...  # Placeholder for the client object

    def to_dict(self) -> dict[str, Any]:
        """Converts module configuration to a dictionary.

        This method is intended to provide a serializable representation of the
        module's current state or configuration. Subclasses should override this
        to include relevant attributes.

        Returns:
            A dictionary representation of the module's configuration.
        """
        return {
            "module_name": self.MODULE_NAME,
        }

    def __str__(self) -> str:
        """Returns a string representation of the module.

        By default, this returns the string representation of the dictionary
        obtained from `to_dict()`.

        Returns:
            A string representation of the module's configuration.
        """
        return str(self.to_dict())

    @classmethod
    def get_default_secret_paths(cls) -> dict[str, Path]:
        """Gets the default secret paths for this module as Path objects.

        Returns:
            A dictionary where keys are secret names and values
            are `pathlib.Path` objects corresponding to the default secret file locations.
        """
        return {k: Path(v) for k, v in cls.DEFAULT_SECRET_PATHS.items()}


class BaseModuleUtilities:
    """Utility class providing helper methods for BaseModule and its subclasses.

    This class contains static utility methods that assist with common operations
    across different module implementations, such as secret management with fallback
    mechanisms. It is not intended to be instantiated.
    """

    @staticmethod
    def parse_secret_with_fallback(
        *secret_locations: str | os.PathLike[str] | None,
    ) -> str | dict | None:
        """Attempts to parse a secret with multiple fallback options.

        This method tries to parse a secret from each provided location in order
        until one succeeds or all options are exhausted.

        Args:
            *secret_locations: Variable number of potential secret locations. Can be
                file paths (str or PathLike) or None values (which are skipped).

        Returns:
            The parsed secret content (str or dict) if found through any method,
            otherwise None.
        """
        for location in secret_locations:
            if location is None:
                continue

            secret_content = SecretManager.parse_secret(location, verbose=False)
            if secret_content:
                return secret_content

        return None
