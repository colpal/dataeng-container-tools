"""Tools for working with Google Cloud Datastore.

This module handles all Datastore operations for storing and retrieving task entries.
It provides functionality to manage task entries in Datastore with features like
filtering, ordering, and updating task information.
"""

from __future__ import annotations

import datetime
import logging
import os
from operator import itemgetter
from typing import TYPE_CHECKING, Any, ClassVar

from dataeng_container_tools.modules import BaseModule, BaseModuleUtilities

if TYPE_CHECKING:
    from google.cloud import datastore

logger = logging.getLogger("Container Tools")


class Datastore(BaseModule):
    """Handles all Google Cloud Datastore operations.

    This class provides methods for interacting with Google Cloud Datastore,
    including querying, creating, and updating task entries.

    Attributes:
        current_task_kind: The kind of task entries this instance will handle.
        client: The Datastore client instance.

    Examples:
        ds = DataStore("task_kind", ds_secret_location="/path/to/credentials.json")
        client = ds.client
        ds.handle_task(client, {
            "dag_id": "my_dag",
            "run_id": "run_123",
            "airflow_task_id": "my_task"
        })
    """

    MODULE_NAME: ClassVar[str] = "DS"
    DEFAULT_SECRET_PATHS: ClassVar[dict[str, str]] = {"DS": "/vault/secrets/gcp-credentials.json"}

    def __init__(
        self,
        task_kind: str,
        *,
        gcp_secret_location: str | os.PathLike[str] | None = None,
        use_cla_fallback: bool = True,
        use_file_fallback: bool = True,
    ) -> None:
        """Initialize the DS module.

        Args:
            task_kind: The kind of task entries this instance will handle.
            gcp_secret_location: The location of the secret file
                associated with Datastore.
            use_cla_fallback: If True, attempts to use command-line arguments
                as a fallback source for secrets when the primary source fails.
            use_file_fallback: If True, attempts to use the default secret file
                as a fallback source when both primary and command-line sources fail.
        """
        from google.cloud import datastore

        self.current_task_kind = task_kind

        gcp_credentials = BaseModuleUtilities.parse_secret_with_fallback(
            gcp_secret_location,
            self.MODULE_NAME if use_cla_fallback else None,
            self.DEFAULT_SECRET_PATHS[self.MODULE_NAME] if use_file_fallback else None,
        )
        if not gcp_credentials:
            msg = "Datastore credentials not found"
            raise FileNotFoundError(msg)

        self.client: datastore.Client = datastore.Client.from_service_account_info(gcp_credentials)

    def get_task_entry(
        self,
        filter_map: dict[str, Any],
        kind: str,
        order_task_entries_params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Query task entries based on filter criteria.

        Args:
            filter_map: Dictionary of filter criteria (key-value pairs).
            kind: The kind of entities to query.
            order_task_entries_params: Optional parameters for ordering task entries.
                Should contain 'order_by_key_list' (list of keys to order by) and
                'descending_order' (boolean) keys.

        Returns:
            List of task entries matching the filter criteria.

        Raises:
            Exception: If ordering keys are not present in entries or other query errors.
        """
        # Create and execute query with filters
        query = self.client.query(kind=kind)
        for key, value in filter_map.items():
            query.add_filter(key, "=", value)

        entries = list(query.fetch())

        # Apply ordering if specified and multiple entries exist
        if len(entries) > 1 and order_task_entries_params is not None:
            order_keys = order_task_entries_params["order_by_key_list"]

            # Validate that all ordering keys exist in entries
            for key in order_keys:
                for entry in entries:
                    if key not in entry:
                        msg = f"No element for key: {key} present in fetched entry. Cannot order entries by key: {key}"
                        raise ValueError(msg)

            # Sort entries based on specified keys and order
            entries = sorted(
                entries,
                key=itemgetter(*order_keys),
                reverse=order_task_entries_params["descending_order"],
            )

        return entries

    def put_snapshot_task_entry(
        self,
        task_entry: datastore.Entity,
        params: dict[str, Any],
    ) -> None:
        """Store or update a task entry in Datastore.

        Args:
            task_entry: Entity which stores the actual instance of data.
            params: Dictionary containing parameters (key-value pairs) to store.
        """
        # Update task entry with provided parameters
        for key, value in params.items():
            task_entry[key] = value

        # Update modification timestamp
        task_entry["modified_at"] = datetime.datetime.now(datetime.timezone.utc)

        # Store entity in Datastore
        logger.info("Storing task entry: %s", task_entry)
        self.client.put(task_entry)

    def handle_task(
        self,
        params: dict[str, Any],
        order_task_entries_params: dict[str, Any] | None = None,
    ) -> None:
        """Check if a task instance exists and update it or create a new one.

        Args:
            params: Dictionary containing parameters (key-value pairs) to store.
            order_task_entries_params: Optional parameters for ordering task entries
                when retrieving existing entries.
        """
        from google.cloud import datastore

        # Get commit ID from environment if available
        commit_id = os.environ.get("GITHUB_SHA", "")

        # Create filter to find existing entries
        filter_entries = {
            "dag_id": params["dag_id"],
            "run_id": params["run_id"],
            "airflow_task_id": params["airflow_task_id"],
        }

        # Check for existing entries
        existing_entries = self.get_task_entry(
            filter_entries,
            self.current_task_kind,
            order_task_entries_params,
        )

        task_key = self.client.key(self.current_task_kind)
        task_entry = datastore.Entity(key=task_key, exclude_from_indexes=("exception_details",))
        if existing_entries:
            task_entry.update(existing_entries[0])
        else:
            task_entry["commit_id"] = commit_id
            task_entry["created_at"] = datetime.datetime.now(datetime.timezone.utc)

        # Store the task entry
        self.put_snapshot_task_entry(task_entry, params)
