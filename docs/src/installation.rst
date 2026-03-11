Installation
============

Requirements
------------

DE Container Tools requires Python 3.10 or higher.

Installing from PyPI
--------------------

.. tabs::

   .. tab:: UV (Recommended)

      UV is a fast Python package installer and resolver. Learn more at https://docs.astral.sh/uv/

      .. code-block:: bash
        :substitutions:

          uv pip install git+https://github.com/colpal/dataeng-container-tools.git@|version|

   .. tab:: pip

      .. code-block:: bash
        :substitutions:

          pip install git+https://github.com/colpal/dataeng-container-tools.git@|version|

Install Optionals
-----------------

The following optional dependencies are available:

- `snowflake`
- `polars`

To install additional optional dependencies:

.. tabs::

   .. tab:: UV (Recommended)

      .. code-block:: bash
        :substitutions:

          uv pip install dataeng-container-tools[snowflake]==|version|

   .. tab:: pip

      .. code-block:: bash
        :substitutions:

          pip install dataeng-container-tools[snowflake]==|version|

Verification
------------

To verify that the package has been installed correctly, you can run:

.. code-block:: bash

    python -c "import dataeng_container_tools; print(dataeng_container_tools.__version__)"

This should display the version number of the installed package.
