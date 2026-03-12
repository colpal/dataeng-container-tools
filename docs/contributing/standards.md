# Standards

We welcome contributions to the DE Container Tools project! This page describes how to set up your development environment and submit changes.

## Getting the Source Code

1. Fork the repository on GitHub
2. Clone your fork locally:

```bash
git clone https://github.com/your-username/dataeng-container-tools.git
cd dataeng-container-tools
```

## Development Environment Setup

=== "UV (Recommended)"

    UV is a fast Python package installer and resolver. Learn more at https://docs.astral.sh/uv/

    Install and use UV for environment setup:

    ```bash
    # Create and activate virtual environment
    uv sync -p 3.10

    # Mac/Linux
    source .venv/bin/activate
    # Windows
    .venv\Scripts\activate
    ```

=== "Python"

    Create and activate a virtual environment:

    ```bash
    # Create and activate virtual environment (should be Python 3.10)
    python3 -m venv .venv

    # Mac/Linux
    source .venv/bin/activate
    # On Windows
    .venv\Scripts\activate

    pip install -e ".[dev]"
    ```

You will also need to install the [Ruff extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)

## Code Style Guidelines

This project uses Ruff for linting and code quality enforcement:

1. Before submitting code, run ruff:

    ```bash
    ruff check .
    ```

2. Follow these coding standards:

   - Use [Google-style docstrings](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)
   - Follow [Ruff rules](https://docs.astral.sh/ruff/rules/)
   - Keep line length to 120 characters
   - Use type hints for all function signatures
   - Write comprehensive unit tests for new functionality

## Making Changes

1. Make your changes and ensure all tests pass
2. Update documentation if needed
3. Follow the [Conventional Commit Messages](https://gist.github.com/qoomon/5dfcdf8eec66a051ecd85625518cfd13)

## Submitting Changes

1. Push your changes to your branch:

    ```bash
    git push origin your-branch
    ```

2. Open a Pull Request merging from your fork to the `main` repository
3. Wait for code review

## Building Documentation

To build the documentation locally:

```bash
mkdocs serve
```

The documentation will be available at `http://127.0.0.1:8000/`. The server will automatically reload when you make changes to the documentation files.
