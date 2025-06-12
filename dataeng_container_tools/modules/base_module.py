"""Base module for data engineering container tools.

This module provides a base class for all specialized modules such as GCS, DB, etc.
It offers common functionality and a consistent interface that all module implementations
should follow, ensuring a uniform API across the library.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, ClassVar

from dataeng_container_tools.secrets_manager import SecretLocations, SecretManager

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
        self.client: Any = ... # Placeholder for the client object

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
        secret_location: str | Path | None = None,
        fallback_secret_key: str | None = None,
        fallback_secret_file: str | Path | None = None,
    ) -> str | dict | None:
        """Attempts to parse a secret with multiple fallback options.

        This method tries to parse a secret from the `secret_location` first.
        If that fails or `secret_location` is not provided, it attempts to use
        `fallback_secret_key` to look up the secret path from `SecretLocations`.
        If that also fails or is not provided, it tries `fallback_secret_file`.

        Args:
            secret_location: The primary file path of the secret.
            fallback_secret_key: A key to look up a secret path in `SecretLocations`
                as a secondary option. For example, "GCS" or "SF_USER".
            fallback_secret_file: A direct file path to use as a tertiary fallback.

        Returns:
            The parsed secret content (str or dict) if found through any method,
            otherwise None.
        """
        secret_content = None

        # Main location
        if secret_location:
            secret_content = SecretManager.parse_secret(secret_location)

        # CLA fallback
        if not secret_content and fallback_secret_key:
            secret_content = SecretManager.parse_secret(SecretLocations()[fallback_secret_key])

        # File fallback
        if not secret_content and fallback_secret_file:
            secret_content = SecretManager.parse_secret(fallback_secret_file)

        return secret_content
