Snowflake
=========

A simple wrapper for the `Python Snowflake Connector <https://docs.snowflake.com/en/developer-guide/python-connector/python-connector>`_.

.. py:class:: Snowflake(role: str, database: str, schema: str, warehouse: str, account: str, query_tag: str, sf_secret_location: str, *, use_cla_fallback: bool = True, use_file_fallback: bool = True, **kwargs)

From secrets (either by the fallbacks or ``sf_secret_location``), the wrapper will infer the usage of ``password`` or ``private_key`` based login.

.. code-block:: python

   from dataeng_container_tools import Snowflake

   sf = Snowflake(role="TEST_ROLE", database="TEST_DB", schema="TEST_SCHEMA", warehouse="TEST_WH", account="TEST_ACCOUNT")

   results = sf.execute("SELECT * FROM TEST_TABLE")
   print(results)

Alternately the context can be directly accessed for more flexibility.

.. code-block:: python

   from dataeng_container_tools import Snowflake

   # Notice that additional Snowflake args can be passed via keywords such as query_tag
   sf = Snowflake(role="TEST_ROLE", database="TEST_DB", schema="TEST_SCHEMA", warehouse="TEST_WH", account="TEST_ACCOUNT", query_tag="test_tag")

   cur = sf.ctx.cursor()
   cur.execute("SELECT * FROM TEST_TABLE")
   print(cur.fetchall())
