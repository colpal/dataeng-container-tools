Download
========

The Download (DL) module wraps around Python's ``requests`` and ``PoolExecutors`` to provide an easy way to download from multiple URLs concurrently.

Currently, the module supports one function, which can be accessed through the static method ``Download.download``.

.. py:function:: Download.download(urls_to_files, *, headers=None, max_workers=5, chunk_size=32 * 1024 * 1024, timeout=300, decode_content=True, mode="thread", output="complete")


Basic Downloading
-----------------

We can pass a mapping dictionary of URLs to file paths to the function to download them:

.. code-block:: python
    
    from dataeng_container_tools import Download

    Download.download({
        "http://example.com/info.txt": "text/info.txt",
        "http://example.com/data.csv": "data/data.csv",
    })

This saves the files to the ``text/info.txt`` and ``data/data.csv`` paths.

However, if you need to include header information such as API keys or content type, the ``headers`` parameter can be passed to apply to all URLs.

.. code-block:: python

    from dataeng_container_tools import Download

    headers = {
        "Authorization": "Bearer your_api_token_here",
        "Content-Type": "application/json",
        "Content-Encoding": "gzip",
        "User-Agent": "MyApp/1.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no",
    }

    Download.download({
        "http://example.com/info.txt": "text/info.txt",
        "http://example.com/data.csv": "data/data.csv",
    }, headers=headers)


Additional Parameters
---------------------

Requests Parameters
___________________

The first set of parameters to note are the ``requests`` library ones.

``chunk_size`` controls how many bytes can be read from an endpoint before being written to the destination 
file. Keeping this value too high can cause high memory usage as all the data is kept in RAM. Keeping this 
value too low can cause reduced throughput as the function has to write to the file many times. The default 
value is a good starting point and should only be modified if needed.

``timeout`` controls how long the connection should wait when the endpoint does not respond. 
Since connections can sometimes stall indefinitely, it is often a good idea to set some value 
to avoid having issues with a container lasting hours should a server not respond. The parameter 
accepts a number of seconds, a tuple of (connect, read), or ``None`` to disable. 
See `timeouts <https://requests.readthedocs.io/en/latest/user/advanced/#timeouts>`_ for more information.

Generally, use a try-except block to handle timeouts:

.. code-block:: python

    from dataeng_container_tools import Download
    from requests.exceptions import Timeout

    try:
        Download.download({
            "http://example.com/info.txt": "text/info.txt",
            "http://example.com/data.csv": "data/data.csv",
        })
    except Timeout as e:
        print(f"Error, timed out: {e!s}")

``decode_content`` determines whether the request should automatically decode the file as it reads it. 
Generally, most server/API endpoints support sending a file in a compressed format such as ``gzip`` 
since this reduces the size and bandwidth required to transfer the file. However, if the user wishes to, 
they can perform file operations on the raw compressed file. By default this is True, and for most use cases 
it should not be set to False. 
See `urllib3 <https://urllib3.readthedocs.io/en/stable/reference/urllib3.response.html>`_ for more information.

.. code-block:: python

    from dataeng_container_tools import Download
    import gzip

    headers = {
        "Content-Type": "application/json",
        "Content-Encoding": "gzip",
        "Accept": "application/json",
        "Accept-Encoding": "gzip",  # Only accept gzip
    }

    # Download compressed file without decoding
    Download.download({
        "http://example.com/data.json": "data.gz",
    }, headers=headers, decode_content=False)

    # Read the compressed file
    with gzip.open("data.gz", "rb") as f:
        content = f.read()
        print(content.decode('utf-8'))

This can help write performant code by processing the file in one operation and reduce the need to 
load the entire file into memory.

Executor Parameters
___________________

``max_workers`` controls how many threads or processes can be used to download the files concurrently. 
Generally, having something like 5 workers means 5 URLs can be downloaded at the same time. However, 
beware that this does not mean having more workers will always result in better throughput or faster downloads. 
In Python, the Global Interpreter Lock (GIL) prevents threads from running in parallel. 
Even if you use ``process`` mode instead, having more max_workers can significantly increase memory usage. 
The default value of 5 is a good starting point and should only be modified if needed. 
See `Python Wiki <https://wiki.python.org/moin/GlobalInterpreterLock>`_ or `Real Python <https://realpython.com/python-gil/>`_ 
for more information on the GIL.

``mode`` controls whether the function uses threads or processes to download the files. In general, 
``thread`` is more efficient with resource allocation and simpler to manage, and is usually more than 
enough for most use cases regarding downloading files. However, if the user wishes to use processes instead, 
they can set the mode to ``process``. As mentioned in the ``max_workers`` section, the difference between 
``thread`` and ``process`` comes down to whether the user wishes to bypass the GIL or not.

.. warning::
    When running in containerized environments, using ``mode="process"`` may cause downloads to stall 
    indefinitely without detection due to CPU allocation limits. Container orchestrators often restrict 
    the number of CPU cores available to a container, which can bottleneck process-based concurrency. 

Output Types
------------

The ``output`` parameter controls the return type and how ``Download.download`` should handle 
its output.

Using ``output="complete"`` will cause the function to wait until all downloads have been processed and 
saved to a file. The return type is ``None`` and is good for basic use cases.

Using ``output="generator"`` will make the function return a generator that yields a 2-tuple (url, file_path). 
Unlike with ``complete``, this will yield results as they come in (out of order). So processing can start 
immediately instead of waiting.

.. code-block:: python
    
    from dataeng_container_tools import Download

    for url, file_path in Download.download({
        "http://example.com/info.txt": "text/info.txt",
        "http://example.com/data.csv": "data/data.csv",
    }, output="generator"):
        print(f"Downloaded {url} to {file_path}")
        # Further processing here

Using ``output="futures"`` grants the most flexibility and complexity by returning a context 
manager that gives a list of ``concurrent.futures.Future`` objects. 
See `Future <https://docs.python.org/3/library/concurrent.futures.html#future-objects>`_ for more information.

.. code-block:: python
    
    from dataeng_container_tools import Download
    from concurrent.futures import as_completed

    with Download.download({
        "http://example.com/info.txt": "text/info.txt",
        "http://example.com/data.csv": "data/data.csv",
    }, output="futures") as futures:
        for future in as_completed(futures):
            if future.exception() is not None:
                print(f"Warning: Future completed with exception: {future.exception()}")
            else:
                print(f"Success: Download completed without errors")
                url, file_path = future.result()
                print(f"Downloaded {url} to {file_path}")
