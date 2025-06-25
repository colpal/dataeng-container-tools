"""Pytest configuration for Snowflake tests."""

import json
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest


@pytest.fixture
def temp_credentials(fakesnow_server: dict) -> Generator[str, None, None]:
    """Create temporary credentials file for testing using fakesnow server credentials."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        credentials = {
            "username": fakesnow_server["user"],
            "password": fakesnow_server["password"],
        }
        json.dump(credentials, f)
        temp_path = f.name

    yield temp_path

    # Cleanup
    Path(temp_path).unlink(missing_ok=True)
