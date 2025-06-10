"""Pytest configuration for GCS tests."""

import os

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add custom command line options."""
    parser.addoption(
        "--gcs-port",
        action="store",
        default="9000",
        help="Port number for GCS emulator (default: 9000)",
    )


@pytest.fixture(scope="session", autouse=True)
def setup_gcs_emulator(request: pytest.FixtureRequest) -> None:
    """Set up GCS emulator environment variable."""
    gcs_port = request.config.getoption("--gcs-port")
    os.environ["STORAGE_EMULATOR_HOST"] = f"http://localhost:{gcs_port}"
