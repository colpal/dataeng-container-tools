"""This module is for working with Snowflake.

This module can connect to a snowflake table and execute a custom query.

Example use case:
"""

from __future__ import annotations

import json
import logging

import snowflake.connector

from dataeng_container_tools.modules import BaseModule

logger = logging.getLogger("Container Tools")

# custom class connects to snowflake and executes custom queries


class Snowflake(BaseModule):
    """Handles Snowflake operations.

    This class creates a connection to a snowflake table and executes custom queries entered.

    Attributes:
    ----------
    sf_secret_path : str
        Path to vault secrets.
    role : str
        snowflake role needed for connection
    database : str
        snowflake database the user wants to connect to
    schema : str
        snowflake schema the user wants to connect to
    warehouse : str
        snowflake warehouse the user wants to connect to
    account : str
        snowflake account used for connection
    query_tag : str
        tag of query performed
    """

    def __init__(
        self,
        role: str,
        database: str,
        schema: str,
        warehouse: str,
        account: str,
        query_tag: str,
        sf_secret_path: str,
    ) -> None:
        """Initialize a snowflake connection."""
        with open(sf_secret_path) as f:
            sf_vault_json = json.load(f)

        self.role = role
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.account = account
        self.query_tag = query_tag
        self.private_key_file = sf_vault_json["rsa_private_key"]
        self.user = sf_vault_json["username"]

        self.ctx = snowflake.connector.connect(
            user=user,
            account=account,
            private_key=private_key,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
        )

    # function that executes the custom query
    def execute(self, query: str) -> None:
        """Executes a query and returns the results."""
        cursor = self.ctx.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
        finally:
            cursor.close()
        return result
