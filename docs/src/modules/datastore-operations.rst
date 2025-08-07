Datastore Operations
====================

The Datastore (DS) module provides tools for interacting with Google Cloud Datastore, allowing you to store and retrieve task information.

Basic Usage
-----------

Here's a basic example of using the ``Datastore`` class:

.. code-block:: python

    from dataeng_container_tools import Datastore

    # Initialize the Datastore client with your credentials
    ds = Datastore(
        task_kind="MyTask",
        gcp_secret_location="/path/to/datastore-credentials.json"
    )

    # Define task parameters
    task_params = {
        "dag_id": "my_dag",
        "run_id": "run_123",
        "airflow_task_id": "my_task",
        "status": "RUNNING",
        "metadata": {
            "user": "admin",
            "priority": "high"
        }
    }

    # Handle the task (create or update entry)
    ds.handle_task(params=task_params)

Querying Task Entries
---------------------

You can query existing task entries:

.. code-block:: python

    from dataeng_container_tools import Datastore

    # Initialize the Datastore client
    ds = Datastore(
        task_kind="MyTask", 
        gcp_secret_location="/path/to/datastore-credentials.json",
    )

    # Create filter criteria
    filter_map = {
        "dag_id": "my_dag",
        "run_id": "run_123",
    }

    # Query task entries
    task_entries = ds.get_task_entry(
        filter_map=filter_map,
        kind="MyTask",
    )

    # Process the results
    for entry in task_entries:
        print(f"Task ID: {entry['airflow_task_id']}")
        print(f"Status: {entry['status']}")
        print(f"Created: {entry['created_at']}")

Updating Task Status
--------------------

You can update the status of a task:

.. code-block:: python

    from dataeng_container_tools import Datastore

    # Initialize the Datastore client
    ds = Datastore(
        task_kind="MyTask", 
        gcp_secret_location="/path/to/datastore-credentials.json",
    )

    # Define filter to find the task
    filter_map = {
        "dag_id": "my_dag",
        "run_id": "run_123",
        "airflow_task_id": "my_task",
    }

    # Find the existing task entry
    entries = ds.get_task_entry(
        filter_map=filter_map,
        kind="MyTask",
    )

    if entries:
        # Update task parameters
        updated_params = {
            "dag_id": "my_dag",
            "run_id": "run_123",
            "airflow_task_id": "my_task",
            "status": "COMPLETED",
            "end_time": datetime.datetime.now(datetime.timezone.utc),
        }

        # Update the task
        ds.handle_task(params=updated_params)

Ordering Task Entries
---------------------

You can specify ordering when querying task entries:

.. code-block:: python

    from dataeng_container_tools import Datastore

    # Initialize the Datastore client
    ds = Datastore(
        task_kind="MyTask", 
        gcp_secret_location="/path/to/datastore-credentials.json",
    )

    # Define filter
    filter_map = {"dag_id": "my_dag"}

    # Define ordering parameters
    order_params = {
        "order_by_key_list": ["created_at"],
        "descending_order": True,  # Latest first
    }

    # Query task entries with ordering
    task_entries = ds.get_task_entry(
        filter_map=filter_map,
        kind="MyTask",
        order_task_entries_params=order_params,
    )

    # Process the results - now in order by created_at (descending)
    for entry in task_entries:
        print(f"Task: {entry['airflow_task_id']}, Created: {entry['created_at']}")
