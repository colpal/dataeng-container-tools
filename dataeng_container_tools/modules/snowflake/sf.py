"""This module is for working with Snowflake.

This module can connect to a Snowflake table and execute a custom query.

Example use case:
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar

from dataeng_container_tools.modules import BaseModule, BaseModuleUtilities

if TYPE_CHECKING:
    from snowflake.connector.connection import SnowflakeConnection

logger = logging.getLogger("Container Tools")


class Snowflake(BaseModule):
    """Handles Snowflake operations.

    This class creates a connection to a snowflake table and executes custom queries entered.

    Attributes:
        sf_secret_location (str): Path to vault secrets.
        role (str): snowflake role needed for connection
        database (str): snowflake database the user wants to connect to
        schema (str): snowflake schema the user wants to connect to
        warehouse (str): snowflake warehouse the user wants to connect to
        account (str): snowflake account used for connection
        query_tag (str): tag of query performed
    """

    MODULE_NAME: ClassVar[str] = "SF"
    DEFAULT_SECRET_PATHS: ClassVar[dict[str, str]] = {
        "SF": "/vault/secrets/sf_creds.json"
    }

    def __init__(
        self,
        role: str,
        database: str,
        schema: str,
        warehouse: str,
        account: str,
        query_tag: str,
        sf_secret_location: str,
        *,
        use_cla_fallback: bool = True,
        use_file_fallback: bool = True,
    ) -> None:
        """Initialize a snowflake connection."""
        import snowflake.connector as sc

        sf_creds = BaseModuleUtilities.parse_secret_with_fallback(
            sf_secret_location,
            self.MODULE_NAME if use_cla_fallback else None,
            self.DEFAULT_SECRET_PATHS[self.MODULE_NAME] if use_file_fallback else None,
        )

        if not sf_creds:
            msg = "Snowflake credentials not found"
            raise FileNotFoundError(msg)

        if not isinstance(sf_creds, dict):
            msg = "Snowflake credentials must be JSON"
            raise TypeError(msg)

        self.user = sf_creds["username"]
        self.authenticator = "SNOWFLAKE_JWT"
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.query_tag = query_tag
        try:
            self.password = sf_creds["password"]
            self.ctx: SnowflakeConnection = sc.connect(
                user=self.user,
                password=self.password,
                authenticator=self.authenticator,
                account=account,
                warehouse=warehouse,
                database=database,
                schema=schema,
                role=role,
            )
            logger.info("Connected to Snowflake successfully with username/password!")
        except Exception as e:
            logger.info(f"Username/Password authentication failed: {e}")
            logger.info("Attempting key pair authentication...")
            try:
                self.private_key = sf_creds["rsa_private_key"]
                self.ctx: SnowflakeConnection = sc.connect(
                    user=self.user,
                    private_key=self.private_key,
                    authenticator=self.authenticator,
                    account=account,
                    warehouse=warehouse,
                    database=database,
                    schema=schema,
                    role=role,
                )
                logger.info("Connected to Snowflake successfully with key pair!")
            except Exception as e2:
                logger.info(f"Key pair authentication failed: {e2}")

    def execute(self, query: str) -> list[tuple] | list[dict]:
        """Executes a query and returns the results."""
        cursor = self.ctx.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
        finally:
            cursor.close()
        return result
