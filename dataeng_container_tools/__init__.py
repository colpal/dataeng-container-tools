"""Data Engineering Container Tools."""

from importlib.metadata import version

__version__ = version("dataeng-container-tools")

from .cla import (
    CommandLineArguments,
    CommandLineArgumentType,
    CustomCommandLineArgument,
)
from .container_utils import IS_LOCAL
from .log_utils import configure_logger
from .modules import Datastore, Download, GCSFileIO, Snowflake
from .safe_textio import SafeTextIO
from .secrets_manager import SecretLocations, SecretManager

__all__ = [
    "IS_LOCAL",
    "CommandLineArgumentType",
    "CommandLineArguments",
    "CustomCommandLineArgument",
    "Datastore",
    "Download",
    "GCSFileIO",
    "SafeTextIO",
    "SecretLocations",
    "SecretManager",
    "Snowflake",
]

# Set up the logger
logger = configure_logger("Container Tools")

# Initialize secrets
SecretManager.process_secret_folder()
