"""Container utilities and tools for container environments."""

from pathlib import Path
from typing import Final


def is_docker() -> bool:
    """Returns whether the current environment is in a Docker container.

    Source: https://stackoverflow.com/a/73564246

    Returns:
        True if is in a Docker container. False otherwise.
    """
    cgroup = Path("/proc/self/cgroup")
    return Path("/.dockerenv").is_file() or (cgroup.is_file() and "docker" in cgroup.read_text())


def is_local() -> bool:
    """Detects whether the current environment is local and not in any type of container.

    Returns:
        True if not in any container. False otherwise.
    """
    return not is_docker()


IS_LOCAL: Final = is_local()
