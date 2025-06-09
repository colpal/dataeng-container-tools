"""Data Engineering Container Tools."""

from importlib.metadata import version

__version__ = version("dataeng-container-tools")

from .cla import (
    CommandLineArguments,
    CommandLineArgumentType,
    CustomCommandLineArgument,
)
from .log_utils import configure_logger
from .modules import DS, Download, GCSFileIO, Snowflake
from .safe_textio import SafeTextIO, setup_default_stdio
from .secrets_manager import SecretLocations, SecretManager

__all__ = [
    "DS",
    "CommandLineArgumentType",
    "CommandLineArguments",
    "CustomCommandLineArgument",
    "Download",
    "GCSFileIO",
    "SafeTextIO",
    "SecretLocations",
    "SecretManager",
    "Snowflake",
    "configure_logger",
    "setup_default_stdio",
]

# Set up the logger
logger = configure_logger("Container Tools")

# Initialize secrets and stdout/stderr bad words output
setup_default_stdio()
SecretManager.process_secret_folder()
