Standards
=========

We welcome contributions to the DE Container Tools project! This page describes how to set up your development environment and submit changes.

Getting the Source Code
-----------------------------

1. Fork the repository on GitHub
2. Clone your fork locally:

   .. code-block:: bash

       git clone https://github.com/your-username/dataeng-container-tools.git
       cd dataeng-container-tools

Development Environment Setup
-----------------------------

.. tabs::

   .. tab:: Python

      Create and activate a virtual environment:

      .. code-block:: bash

         # Confirm Python 3.9
         python --version

         python -m venv .venv

         # Mac/Linux
         source .venv/bin/activate
         # On Windows
         .venv\Scripts\activate

         pip install -e ".[dev]"

   .. tab:: UV (Recommended)

      UV is a fast Python package installer and resolver. Learn more at https://docs.astral.sh/uv/

      Install and use UV for environment setup:

      .. code-block:: bash

         # Install UV if you don't have it
         pip install uv
         
         # Create and activate virtual environment
         uv venv --python 3.9

         # Mac/Linux
         source .venv/bin/activate
         # Windows
         .venv\Scripts\activate
         
         # Install development dependencies
         uv pip install -e ".[dev]"

You will also need to install the `Ruff extension <https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff>`_

For writing RST Documentation, the following extensions may help:

- `reStructuredText <https://marketplace.visualstudio.com/items?itemName=lextudio.restructuredtext>`_
- `reStructuredText Syntax highlighting <https://marketplace.visualstudio.com/items?itemName=trond-snekvik.simple-rst>`_
- `Esbonio <https://marketplace.visualstudio.com/items?itemName=swyddfa.esbonio>`_

Code Style Guidelines
---------------------

This project uses Ruff for linting and code quality enforcement:

1. Before submitting code, run ruff:

   .. code-block:: bash

       ruff check .

2. Follow these coding standards:

   - Use `Google-style docstrings <https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html>`_
   - Follow `Ruff rules <https://docs.astral.sh/ruff/rules/>`_
   - Keep line length to 120 characters
   - Use type hints for all function signatures 
   - Write comprehensive unit tests for new functionality

Making Changes
--------------

1. Make your changes and ensure all tests pass
2. Update documentation if needed
3. Follow the `Conventional Commit Messages <https://gist.github.com/qoomon/5dfcdf8eec66a051ecd85625518cfd13>`_

Submitting Changes
------------------

1. Push your changes to your branch:

   .. code-block:: bash

      git push origin your-branch

2. Open a Pull Request merging from your fork to the `main` repository
3. Wait for code review

Building Documentation
----------------------

To build the documentation locally:

.. code-block:: bash

   cd docs
   python3 make.py --html

The documentation will be available in the `docs/build/html` directory. Simply open `index.html` with a browser.
