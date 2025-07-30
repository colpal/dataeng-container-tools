"""This module is for working with Snowflake.

This module can connect to a Snowflake table and execute a custom query.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from dataeng_container_tools.modules import BaseModule, BaseModuleUtilities

if TYPE_CHECKING:
    import os
    from types import TracebackType

    from snowflake.connector.connection import SnowflakeConnection


class Snowflake(BaseModule):
    """Handles Snowflake operations.

    This class creates a connection to a Snowflake table and executes custom queries entered.

    Attributes:
        account: Snowflake account used for connection
        role: Snowflake role needed for connection
        database: Snowflake database the user wants to connect to
        schema: Snowflake schema the user wants to connect to
        warehouse: sSnowflake warehouse the user wants to connect to
    """

    MODULE_NAME: ClassVar[str] = "SF"
    DEFAULT_SECRET_PATHS: ClassVar[dict[str, str]] = {"SF": "/vault/secrets/sf_creds.json"}

    def __init__(
        self,
        account: str,
        database: str,
        schema: str,
        warehouse: str,
        role: str,
        *,
        sf_secret_location: str | os.PathLike[str] | None = None,
        use_cla_fallback: bool = True,
        use_file_fallback: bool = True,
        **kwargs: Any,  # Use ParamSpec in future  # noqa: ANN401
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
        self.account = account
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role

        # Handle both password and private key authentication
        private_key = sf_creds.get("rsa_private_key")
        private_key_bytes = (
            serialization.load_pem_private_key(
                private_key.encode("utf-8"),
                password=None,
                backend=default_backend(),
            ).private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            if private_key
            else None
        )

        self.ctx: SnowflakeConnection = sc.connect(
            user=self.user,
            password=sf_creds.get("password"),
            private_key=private_key_bytes,
            account=account,
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role,
            **kwargs,
        )

    def execute(self, query: str) -> list[tuple] | list[dict]:
        """Executes a query and returns the results."""
        cursor = self.ctx.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
        finally:
            cursor.close()
        return result

    def __enter__(self) -> Snowflake:  # noqa: PYI034
        """Context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Context manager wrapper for closing Snowflake."""
        self.ctx.__exit__(exc_type, exc_val, exc_tb)
