# Basic Example Container

The following is a basic container that uses this library.

## pyproject.toml

```toml
[project]
name = "some-container"
version = "0.1.0"
description = "Add your description here"
readme = "README.md"
requires-python = ">=3.13,<3.14"
dependencies = [
    "dataeng-container-tools",
]

[tool.uv.sources]
dataeng-container-tools = { git = "https://github.com/colpal/dataeng-container-tools.git", rev = "v1.0.0-alpha.7" }
```

## Dockerfile

```dockerfile
FROM us-east4-docker.pkg.dev/cp-artifact-registry/mirror-dockerhub/astral/uv:0.10.7-python3.13-trixie-slim AS builder

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git=1:2.* && \
    rm -rf /var/lib/apt/lists/*

COPY .python-version pyproject.toml uv.lock ./

ENV UV_PYTHON_INSTALL_DIR="/app/python"

uv sync --frozen --no-dev --no-cache --compile-bytecode

FROM debian:trixie-slim

WORKDIR /app

COPY --from=builder /app/.venv .venv
COPY --from=builder /app/python python
COPY ./app ./app

ENTRYPOINT ["/app/.venv/bin/python", "app/entrypoint.py"]
```

## app/entrypoint.py

```python
"""Basic example Python script."""
import sys
from pathlib import Path

from dataeng_container_tools import CommandLineArguments, CommandLineArgumentType, GCSFileIO


def main() -> int:
    """Basic example function."""
    CommandLineArguments(secret_locations=CommandLineArgumentType.REQUIRED)

    Path("./local.txt").write_text("Hello world")

    gcs_file_io = GCSFileIO()
    gcs_file_io.upload(src_dst=("./local.txt", "./upload.txt"))

    return 0


if __name__ == "__main__":
    sys.exit(main())
```
