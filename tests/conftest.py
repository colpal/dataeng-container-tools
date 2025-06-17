"""Pytest configuration for args."""

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add custom command line options."""
    parser.addoption(
        "--gcs-port",
        action="store",
        default="9000",
        help="Port number for GCS emulator (default: 9000)",
    )
