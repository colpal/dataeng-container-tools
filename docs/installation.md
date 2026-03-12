# Installation

## Requirements

DE Container Tools requires Python 3.10 or higher.

## Installing from PyPI

=== "UV (Recommended)"

    UV is a fast Python package installer and resolver. Learn more at https://docs.astral.sh/uv/

    ```bash
    uv pip install git+https://github.com/colpal/dataeng-container-tools.git@v1.0.0-alpha.7
    ```

=== "pip"

    ```bash
    pip install git+https://github.com/colpal/dataeng-container-tools.git@v1.0.0-alpha.7
    ```

## Install Optionals

The following optional dependencies are available:

- `snowflake`
- `polars`

To install additional optional dependencies:

=== "UV (Recommended)"

    ```bash
    uv pip install dataeng-container-tools[snowflake]==1.0.0-alpha.7
    ```

=== "pip"

    ```bash
    pip install dataeng-container-tools[snowflake]==1.0.0-alpha.7
    ```

## Verification

To verify that the package has been installed correctly, you can run:

    ```bash
    python -c "import dataeng_container_tools; print(dataeng_container_tools.__version__)"
    ```

This should display the version number of the installed package.
