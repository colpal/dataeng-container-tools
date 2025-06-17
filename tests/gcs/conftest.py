"""Pytest configuration for GCS tests."""

import os
import signal
import subprocess
import sys
from collections.abc import Generator
from pathlib import Path

import pytest


@pytest.fixture(scope="session", autouse=True)
def setup_gcs_testbench(request: pytest.FixtureRequest) -> Generator[None, None, None]:
    """Set up GCS emulator."""
    gcs_port = request.config.getoption("--gcs-port")
    os.environ["STORAGE_EMULATOR_HOST"] = f"http://localhost:{gcs_port}"

    runner = Path(__file__).parent / "_helper" / "testbench_run.py"
    cmd = [sys.executable, runner.as_posix(), "localhost", gcs_port, "10"]
    process = subprocess.Popen(cmd)

    yield None

    process.send_signal(signal.SIGINT)  # Try to terminate gracefully
    process.wait(timeout=15)

    if process.poll() is None:  # If the emulator does not end itself, terminate manually
        process.terminate()
