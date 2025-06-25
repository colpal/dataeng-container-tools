"""Pytest configuration for GCS tests."""

import os
import signal
import subprocess
import sys
import time
from collections.abc import Generator
from pathlib import Path

import pytest
import requests


def _is_gcs_ready(port: str) -> bool:
    """Check if GCS testbench is ready by making a health check request."""
    try:
        response = requests.get(f"http://localhost:{port}/", timeout=5)
    except (requests.RequestException, ConnectionError):
        return False
    return response.status_code == 200


@pytest.fixture(scope="session", autouse=True)
def setup_gcs_testbench(request: pytest.FixtureRequest) -> Generator[None, None, None]:
    """Set up GCS emulator."""
    gcs_port = request.config.getoption("--gcs-port")
    if not isinstance(gcs_port, str):
        gcs_port = "9000"

    os.environ["STORAGE_EMULATOR_HOST"] = f"http://localhost:{gcs_port}"

    runner = Path(__file__).parent / "_helper" / "testbench_run.py"
    cmd = [sys.executable, runner.as_posix(), "localhost", gcs_port, "10"]
    process = subprocess.Popen(cmd)

    # Wait for GCS testbench to be ready (max 5 minutes)
    timeout = 300  # 5 minutes
    start_time = time.time()

    while time.time() - start_time < timeout:
        if _is_gcs_ready(gcs_port):
            break
        time.sleep(3)
    else:
        process.terminate()
        msg = f"GCS testbench failed to start within {timeout} seconds"
        raise RuntimeError(msg)

    yield None

    process.send_signal(signal.SIGINT)  # Try to terminate gracefully
    process.wait(timeout=15)

    if process.poll() is None:  # If the emulator does not end itself, terminate manually
        process.terminate()
