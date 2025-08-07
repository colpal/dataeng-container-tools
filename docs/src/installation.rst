Installation
============

Requirements
------------

DE Container Tools requires Python 3.9 or higher. The package has the following dependencies:

* pandas
* google-cloud-storage
* google-cloud-datastore
* openpyxl
* pyarrow
* psutil

Installing from PyPI
--------------------

The recommended way to install DE Container Tools is via pip:

.. code-block:: bash

    pip install dataeng-container-tools

Installing for Development
--------------------------

For development, you can install the package in editable mode with additional dependencies:

.. code-block:: bash

    # Clone the repository
    git clone https://github.com/colpal/dataeng-container-tools.git
    cd dataeng-container-tools
    
    # Install in editable mode with development dependencies
    pip install -e ".[dev,docs]"

Install Specific Version
------------------------

To install a specific version of the package:

.. code-block:: bash

    pip install dataeng-container-tools==1.0.0

Verification
------------

To verify that the package has been installed correctly, you can run:

.. code-block:: bash

    python -c "import dataeng_container_tools; print(dataeng_container_tools.__version__)"

This should display the version number of the installed package.
