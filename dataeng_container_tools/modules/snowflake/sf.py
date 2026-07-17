"""This module is for working with Snowflake.

This module can connect to a Snowflake table and execute a custom query.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from dataeng_container_tools.modules import BaseModule, BaseModuleUtilities
from dataeng_container_tools.secrets_manager import SecretLocations

if TYPE_CHECKING:
    import os
    from types import TracebackType

    from snowflake.connector.connection import SnowflakeConnection


class Snowflake(BaseModule):
    """Wrapper around the Snowflake Python connector.

    Opens a connection using credentials resolved from a secret file and exposes
    a simple `execute` helper. The authentication method (key-pair or password)
    is inferred from the credentials: if the secret contains an `rsa_private_key`,
    key-pair auth is used, otherwise the `password` field is used.

    For full control, the underlying connection is available as `ctx`. The class
    can also be used as a context manager to close the connection automatically.

    Requires the `snowflake` extra: `dataeng-container-tools[snowflake]`.

    Attributes:
        account: Snowflake account used for the connection.
        role: Snowflake role used for the connection.
        database: Snowflake database to connect to.
        schema: Snowflake schema to connect to.
        warehouse: Snowflake warehouse to connect to.
        user: Username read from the credentials secret.
        ctx: The underlying `snowflake.connector.SnowflakeConnection`.

    Examples:
        Run a query with the convenience helper:
            >>> sf = Snowflake(
            ...     account="my_account",
            ...     database="MY_DB",
            ...     schema="PUBLIC",
            ...     warehouse="MY_WH",
            ...     role="MY_ROLE",
            ... )
            >>> rows = sf.execute("SELECT * FROM my_table")

        Use as a context manager and access the raw connection:
            >>> with Snowflake("acct", "DB", "PUBLIC", "WH", "ROLE") as sf:
            ...     cursor = sf.ctx.cursor()
            ...     cursor.execute("SELECT 1")
    """

    MODULE_NAME: ClassVar[str] = "SF"
    DEFAULT_SECRET_PATHS: ClassVar[dict[str, str]] = {
        "SF": "/vault/secrets/sf-key-pair.json",
        "SF_ALT": "/vault/secrets/sf_key_pair.json",
        "SF_LEGACY": "/vault/secrets/sf-creds.json",
        "SF_LEGACY_ALT": "/vault/secrets/sf_creds.json",
    }

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
        **kwargs: Any,
    ) -> None:
        """Initialize a Snowflake connection.

        Credentials are resolved from the first available source: `sf_secret_location`,
        then the registered `SecretLocations` entries, then the module default paths.
        The credentials JSON must contain a `username` and either an `rsa_private_key`
        (key-pair auth) or a `password`.

        Args:
            account: Snowflake account identifier.
            database: Database to connect to.
            schema: Schema to connect to.
            warehouse: Warehouse to use.
            role: Role to assume.
            sf_secret_location: Explicit path to the credentials JSON. If omitted,
                the fallbacks described above are used.
            use_cla_fallback: Whether to fall back to `SecretLocations` paths.
            use_file_fallback: Whether to fall back to the module default secret paths.
            **kwargs: Extra keyword arguments forwarded to
                `snowflake.connector.connect` (e.g. `query_tag`, `session_parameters`).

        Raises:
            ImportError: If the `snowflake` extra is not installed.
            FileNotFoundError: If no credentials could be resolved.
            TypeError: If the resolved credentials are not a JSON object.
        """
        try:
            import snowflake.connector as sc
        except ImportError as e:
            msg = (
                "The 'snowflake' extra is required to use the Snowflake class. "
                "Install it with: dataeng-container-tools[snowflake]"
            )
            raise ImportError(msg) from e

        # Build list of secret paths in order of precedence
        secret_paths = [sf_secret_location]
        for key in [self.MODULE_NAME, "SF_ALT", "SF_LEGACY", "SF_LEGACY_ALT"]:
            if use_cla_fallback:
                secret_paths.append(SecretLocations().get(key))
            if use_file_fallback:
                secret_paths.append(self.DEFAULT_SECRET_PATHS.get(key))

        sf_creds = BaseModuleUtilities.parse_secret_with_fallback(*secret_paths)

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
        """Execute a single query and return all rows.

        Opens a cursor, runs the query, fetches every row, and closes the cursor.
        For streaming large result sets or multi-statement execution, use `ctx`
        directly instead.

        Args:
            query: The SQL statement to execute.

        Returns:
            All fetched rows. Each row is a tuple by default.
        """
        cursor = self.ctx.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
        finally:
            cursor.close()
        return result

    def __enter__(self) -> Snowflake:
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
