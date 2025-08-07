Basic Example Container
=======================

The following is a basic container that uses this library.


Dockerfile
----------

.. code-block:: Docker

   FROM ghcr.io/astral-sh/uv:0.7.15-python3.13-bookworm-slim
   
   WORKDIR /usr/src/app

   COPY scripts/* .
   COPY requirements.txt .

   RUN apt-get update && \
       apt-get install -y --no-install-recommends \
       git=1:2.* && \
       rm -rf /var/lib/apt/lists/*

   ENTRYPOINT ["python", "./entrypoint.py"]


requirements.txt
----------------
.. code-block:: text

   git+https://github.com/colpal/dataEng-container-tools.git@1.0.0


scripts/entrypoint.py
---------------------

.. code-block:: python

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
