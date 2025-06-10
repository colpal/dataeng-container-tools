"""Setup script for the data engineering container tools.

Ensures that only pinned release tags are used for installation.
"""

import subprocess
import sys

import setuptools


def get_current_branch() -> str:
    """Get the current git branch name."""
    try:
        cmd = ["git", "rev-parse", "--abbrev-ref", "HEAD"]
        output = subprocess.check_output(cmd).strip()  # noqa: S603
        return output.decode("utf-8")
    except subprocess.SubprocessError:  # No git or git repo
        return ""


def check_branch() -> None:
    """Disallow master, develop, or feature branches."""
    branch_name = get_current_branch()
    if branch_name not in ["HEAD"] and not branch_name.startswith("test/"):
        sys.exit(f"Installation from {branch_name} branch is not allowed. Please use a pinned release tag instead.")


check_branch()

if __name__ == "__main__":
    setuptools.setup()
