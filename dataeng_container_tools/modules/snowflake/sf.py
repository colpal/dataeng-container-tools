"""This module is for working with Snowflake.

This module can connect to a snowflake table and execute a custom query.

Example use case:
"""

from __future__ import annotations

import logging
from typing import ClassVar

from dataeng_container_tools.modules import BaseModule, BaseModuleUtilities

logger = logging.getLogger("Container Tools")

# custom class connects to snowflake and executes custom queries


class Snowflake(BaseModule):
    """Handles Snowflake operations.

    This class creates a connection to a snowflake table and executes custom queries entered.

    Attributes:
    ----------
    sf_secret_location : str
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

    MODULE_NAME: ClassVar[str] = "Snowflake"
    DEFAULT_SECRET_PATHS: ClassVar[dict[str, str]] = {"Snowflake": "/vault/secrets/sf_creds.json"}

    def __init__(
        self,
        role: str,
        database: str,
        schema: str,
        warehouse: str,
        account: str,
        query_tag: str,
        sf_secret_location: str,
        use_cla_fallback: bool = True,
        use_file_fallback: bool = True,
    ) -> None:
        """Initialize a snowflake connection."""
        import snowflake.connector as sc

        sf_creds: str | dict[str, str] | None = BaseModuleUtilities.parse_secret_with_fallback(
            sf_secret_location,
            self.MODULE_NAME if use_cla_fallback else None,
            self.DEFAULT_SECRET_PATHS[self.MODULE_NAME] if use_file_fallback else None,
        )

        if not sf_creds:
            msg = "Snopwflake credentials not found"
            raise FileNotFoundError(msg)

        self.role = role
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.account = account
        self.query_tag = query_tag
        self.private_key_file = sf_creds["rsa_private_key"]
        self.user = sf_creds["username"]

        self.ctx = sc.connect(
            user=user,
            account=account,
            private_key=private_key_file,
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
