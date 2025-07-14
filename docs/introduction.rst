Introduction
============

Overview
--------

The Data Engineering Container Tools package provides a comprehensive set of utilities designed to streamline container operations for data engineering tasks. This package simplifies common operations when working with containerized applications, particularly in cloud environments.

Core Components
---------------

The package is organized into several core modules:

Command Line Arguments (CLA)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The CLA module provides tools for retrieving and processing command line inputs in a standardized way. It includes three main classes:

* ``CommandLineArguments`` - Creates, parses, and retrieves command line inputs commonly used in data engineering containers
* ``CommandLineArgumentType`` - An enumeration for defining argument requirements (REQUIRED, OPTIONAL, UNUSED)
* ``CustomCommandLineArgument`` - A wrapper for creating custom command line arguments

Google Cloud Storage (GCS)
~~~~~~~~~~~~~~~~~~~~~~~~~~

The GCS module handles interactions with Google Cloud Storage, providing methods for downloading and uploading files with various formats, including:

* Pandas DataFrames
* CSV files
* Parquet files
* Excel files
* JSON files
* Pickle files

Database Operations (DB)
~~~~~~~~~~~~~~~~~~~~~~~~

The DB module provides utilities for working with Google Cloud Datastore. It handles operations such as:

* Creating and retrieving task entries
* Managing task execution state
* Handling metadata for tasks

Safe Text I/O
~~~~~~~~~~~~~

The SafeTextIO module ensures secrets are not accidentally printed to stdout or stderr. It provides:

* Automatic censoring of sensitive information
* Secret file handling
* Secure logging capabilities

Secret Management
~~~~~~~~~~~~~~~~~

The SecretManager component manages secrets for all modules, providing:

* Centralized secret handling
* Automatic registration of module secret needs
* Path resolution and fallbacks for secret locations

Getting Started
---------------

See the :doc:`installation` section for installation instructions and see the :doc:`example-demos/index` and :doc:`/modules/index` sections for detailed usage examples.