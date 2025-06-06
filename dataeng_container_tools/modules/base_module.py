"""Base module for data engineering container tools.

This module provides a base class for all specialized modules such as GCS, DB, etc.
It offers common functionality and a consistent interface that all module implementations
should follow, ensuring a uniform API across the library.

Typical usage example:

    class GCSModule(BaseModule):
        DEFAULT_MODULE_TYPE = "GCS"
        DEFAULT_SECRET_PATHS = {
            "service_account": "/vault/secrets/gcp-sa-storage.json",
            "config": "/vault/secrets/gcs-config.json"
        }

        def __init__(self, override_secret_paths=None, **kwargs):
            super().__init__(override_secret_paths, **kwargs)
            # GCS-specific initialization
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, ClassVar

from dataeng_container_tools.secrets_manager import SecretLocations, SecretManager

logger = logging.getLogger("Container Tools")


class ModuleRegistryMeta(type):
    """Metaclass that automatically registers BaseModule subclasses with SecretManager."""

    def __init__(cls, name, bases, namespace) -> None:  # noqa: ANN001
        """Initialize the class and register it with SecretManager if applicable.

        Args:
            cls: The class being created
            name: Name of the class
            bases: Tuple of base classes
            namespace: Dictionary of class attributes
        """
        super().__init__(name, bases, namespace)
        # Only register subclasses
        if name != "BaseModule" and hasattr(cls, "MODULE_NAME") and hasattr(cls, "DEFAULT_SECRET_PATHS"):
            SecretLocations.register_module(cls)
            logger.debug("Auto-registered module %s with SecretManager", getattr(cls, "MODULE_NAME", "Unknown"))


class BaseModule(metaclass=ModuleRegistryMeta):
    """Base class for all specialized modules.

    This abstract class defines the common interface and functionality that
    all module implementations should follow. It provides methods for handling
    secrets, initialization, and common utilities.

    Attributes:
        MODULE_NAME (ClassVar[str]): Identifies the module type for logging and display.
        DEFAULT_SECRET_PATHS (ClassVar[dict[str, str]]): Default secret file paths for this module.
            Keys are predefined secret types, values are file paths.
        local (bool): Indicates if the module is operating in local mode (no external services).
        secret_paths (dict[str, Path]): Dictionary of secret file paths used by the module.
        client (Any): Client instance used to interact with external services.

    """

    # Class attributes to identify the module type and its default secret paths
    MODULE_NAME: ClassVar[str] = "BASE"
    DEFAULT_SECRET_PATHS: ClassVar[dict[str, str]] = {}

    def __init__(self) -> None:
        """Initialize the base module."""
        self.client = ...

    def to_dict(self) -> dict[str, Any]:
        """Convert module configuration to a dictionary.

        Returns:
            A dictionary representation of the module's configuration.

        """
        return {
            "module_name": self.MODULE_NAME,
        }

    def __str__(self) -> str:
        """Return a string representation of the module.

        Returns:
            A string representation of the module's configuration.

        """
        return str(self.to_dict())

    @classmethod
    def get_default_secret_paths(cls) -> dict[str, Path]:
        """Get the default secret paths for this module.

        Returns:
            A dictionary of default secret keys and their file paths.

        """
        return {k: Path(v) for k, v in cls.DEFAULT_SECRET_PATHS.items()}


class BaseModuleUtilities:
    """Utility class providing helper methods for BaseModule and its subclasses.

    This class contains static utility methods that assist with common operations
    across different module implementations, such as secret management with fallback
    mechanisms.
    """

    @staticmethod
    def parse_secret_with_fallback(
        secret_location: str | Path | None = None,
        fallback_secret_key: str | None = None,
        fallback_secret_file: str | Path | None = None,
    ) -> str | dict | None:
        """Attempts to parse a secret with multiple fallback options if the primary source fails.

        Tries to parse a secret from the provided location. If that fails, it will
        attempt to use fallback mechanisms in the following order:
        1. Command-line argument lookup
        2. File-based lookup

        Args:
            secret_location: Primary location to look for the secret (file path or key)
            fallback_secret_key: Key to use when looking up the secret in SecretLocations
                when the primary lookup fails
            fallback_secret_file: File path to use as final fallback if other methods fail

        Returns:
            The secret content if found through any method, otherwise None.
            Content may be a string or dictionary depending on the secret format.
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
