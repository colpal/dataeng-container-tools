"""Collection of various constants and default values."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, final

from .safe_textio import SafeTextIO

if TYPE_CHECKING:
    import os

logger = logging.getLogger("Container Tools")


class SecretManager:
    """Manages secret files and their contents.

    This class provides functionality to parse individual secret files or process
    entire directories of secrets. It automatically handles JSON parsing and
    maintains a registry of all loaded secrets.

    Attributes:
        DEFAULT_SECRET_FOLDER: Default directory path for secret files.
        files: List of all processed secret file paths.
        secrets: Dictionary mapping file paths to their parsed content.
    """

    DEFAULT_SECRET_FOLDER: Final = Path("/vault/secrets/")

    files: ClassVar[list[Path]] = []
    secrets: ClassVar[dict[str, str | dict]] = {}

    @classmethod
    def parse_secret(cls, file: str | os.PathLike[str], *, update_bad_words: bool = True) -> str | dict | None:
        """Parses the content of a secret file and returns it as a string or dictionary.

        This method reads the content of the file specified by the given file path.
        If the content is a valid JSON object, it is parsed and returned as a dictionary.
        Otherwise, the content is returned as a stripped string.

        Args:
            file: The path to the secret file to be parsed.
            update_bad_words: Whether to also update the list of bad words for SafeTextIO.
                Defaults to True.

        Returns:
            The content of the file as a dictionary if it is a valid JSON object,
            otherwise as a stripped string. None if the file path is invalid.

        Raises:
            json.JSONDecodeError: If the content is not a properly formatted JSON object
                and an attempt to parse it as JSON is made.

        Examples:
            Parse a simple text secret:
                >>> SecretManager.parse_secret("/vault/secrets/api_key")
                "some_api_key_value"

            Parse a JSON secret:
                >>> SecretManager.parse_secret("/vault/secrets/database_json")
                {"host": "db.example.com", "port": 1234, "username": "user"}
        """
        file_path = Path(file)
        if not file_path.exists():
            return None

        # Parse secret as str text
        try:
            content: str = file_path.read_text().strip()
        except OSError:
            logger.exception("File %s cannot be read.", file_path.as_posix())
            return None

        # Try optimistically parsing as JSON
        try:
            if content.startswith("{") and content.endswith("}"):
                content = json.loads(content)  # JSON objects are always considered dicts according to JSONDecoder class
        except json.JSONDecodeError:
            logger.exception("File %s is not a properly formatted JSON file.", file_path.as_posix())

        # Add secrets to variables and bad words
        cls.secrets[file_path.as_posix()] = content
        cls.files.append(file_path)

        if update_bad_words:
            cls.update_bad_words()

        return content

    @classmethod
    def process_secret_folder(cls, folder: str | os.PathLike[str] = DEFAULT_SECRET_FOLDER) -> None:
        """Process all secret files in the given folder.

        This method recursively processes all files in the specified folder,
        treating each file as a potential secret. All secrets are parsed
        and added to the internal registry.

        Args:
            folder: The path to the folder containing secret files.
                Defaults to DEFAULT_SECRET_FOLDER (/vault/secrets/).

        Examples:
            Process the default secret folder:
                >>> SecretManager.process_secret_folder()

            Process a custom folder:
                >>> SecretManager.process_secret_folder("/path/to/my/secrets")
        """
        folder_path = Path(folder)
        if not folder_path.exists():
            logger.info(
                "No secret files found in default directory. This is normal when running locally.",
            )
            return

        files = [file_path for file_path in folder_path.glob("**/*") if file_path.is_file()]
        logger.info("Found these secret files: %s", [file.as_posix() for file in files])
        for file in files:
            cls.parse_secret(file, update_bad_words=False)
        cls.update_bad_words()

    @classmethod
    def update_bad_words(cls) -> None:
        """Update the bad words list for SafeTextIO with current secrets.

        This method extracts all secret values from the secrets registry and adds
        them to SafeTextIO's bad words list to prevent accidental logging or
        exposure of sensitive information.
        """
        bad_words = set()
        for secret in SecretManager.secrets.values():
            these_bad_words = set(secret.values()) if isinstance(secret, dict) else {secret}
            bad_words.update(these_bad_words)
        SafeTextIO.add_words(bad_words)


@final
class SecretLocations(dict[str, str]):
    """Dictionary of SecretLocations which can be accessed like a dict or with common attributes.

    A singleton class that manages secret file locations. This class extends dict functionality
    with attribute access for common secret types like GCS, SF (Snowflake), and DS (Datastore).

    Examples:
        Basic usage:
            >>> SecretLocations().update({"api_key": "/vault/secrets/api_key"})
            >>> foo_bar(SecretLocations()["api_key"])

        Alternate syntax:
            >>> locations = SecretLocations()
            >>> locations.update({"api_key": "/vault/secrets/api_key"})
            >>> foo_bar(locations["api_key"])

        Alternate usage for some preset attributes:
            >>> SecretLocations().GCS  # Access GCS secret location
            >>> SecretLocations().SF   # Access Snowflake secret location
    """

    _instance: ClassVar[SecretLocations | None] = None

    def __new__(cls, *_args: ..., **_kwargs: ...) -> SecretLocations:
        """Create a new instance of SecretLocations or return the existing one.

        Implements the singleton pattern to ensure only one instance exists.

        Returns:
            The singleton instance
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        """Initialize the instance attributes.

        Attributes:
            GCS (str): Default location for GCP storage secret.
            SF (str): Default location for Snowflake secret.
            DS (str): Default location for Datastore secret.
        """
        self.GCS: str = ""
        self.SF: str = ""
        self.DS: str = ""

    def update(self, new_secret_locations: dict[str, str], *, set_attr: bool = False) -> None:  # type: ignore  # noqa: PGH003
        """Updates the secret locations with new values and optionally sets them as attributes.

        Args:
            new_secret_locations: A dictionary containing new secret locations
                where the keys are the secret names and the values are their corresponding locations.
            set_attr: If True, sets the new secret locations as attributes
                of the instance. Defaults to False.
        """
        super().update(new_secret_locations)

        if set_attr:
            for key, value in new_secret_locations.items():
                setattr(self, key, value)

    @classmethod
    def register_module(cls, module_class: type[Any]) -> None:
        """Register a module class to collect its secret paths.

        Args:
            module_class: A BaseModule subclass with MODULE_NAME and DEFAULT_SECRET_PATHS.
        """
        if hasattr(module_class, "MODULE_NAME") and hasattr(module_class, "DEFAULT_SECRET_PATHS"):
            instance = cls()

            # If the user also lazy or defers the loading of a module, it can override user CLA SecretLocations
            # Ensure the key is not already initialized
            new_paths = {k: v for k, v in module_class.DEFAULT_SECRET_PATHS.items() if k not in instance}

            instance.update(new_secret_locations=new_paths, set_attr=True)

            logger.debug("Registered module %s for secret management", module_class.MODULE_NAME)
