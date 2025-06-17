"""Tests for Snowflake execute functionality."""

from dataeng_container_tools import Snowflake


def test_execute_simple_query(fakesnow_server: dict, temp_credentials: str) -> None:
    """Test executing a simple SELECT query."""
    sf = Snowflake(
        role="test_role",
        database="test_db",
        schema="test_schema",
        warehouse="test_warehouse",
        account=fakesnow_server["account"],
        query_tag="test_query",
        sf_secret_location=temp_credentials,
        host=fakesnow_server["host"],
        port=fakesnow_server["port"],
        protocol=fakesnow_server["protocol"],
        session_parameters=fakesnow_server["session_parameters"],
        network_timeout=fakesnow_server["network_timeout"],
    )

    # Test: Execute simple query
    result = sf.execute("SELECT 1 as test_col")

    # Verify: Result should contain expected data
    assert result is not None
    assert len(result) == 1
    assert result[0] == (1,)


def test_execute_create_table_and_select(fakesnow_server: dict, temp_credentials: str) -> None:
    """Test creating a table and selecting from it."""
    sf = Snowflake(
        role="test_role",
        database="test_db",
        schema="test_schema",
        warehouse="test_warehouse",
        account=fakesnow_server["account"],
        query_tag="test_query",
        sf_secret_location=temp_credentials,
        host=fakesnow_server["host"],
        port=fakesnow_server["port"],
        protocol=fakesnow_server["protocol"],
        session_parameters=fakesnow_server["session_parameters"],
        network_timeout=fakesnow_server["network_timeout"],
    )

    # Test: Create a table
    sf.execute("CREATE TABLE test_table (id INT, name STRING)")

    # Test: Insert data
    sf.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")

    # Test: Select data
    result = sf.execute("SELECT * FROM test_table ORDER BY id")

    # Verify: Check results
    assert len(result) == 2
    assert result[0] == (1, "Alice")
    assert result[1] == (2, "Bob")


def test_execute_aggregation_query(fakesnow_server: dict, temp_credentials: str) -> None:
    """Test executing aggregation queries."""
    sf = Snowflake(
        role="test_role",
        database="test_db",
        schema="test_schema",
        warehouse="test_warehouse",
        account=fakesnow_server["account"],
        query_tag="test_query",
        sf_secret_location=temp_credentials,
        host=fakesnow_server["host"],
        port=fakesnow_server["port"],
        protocol=fakesnow_server["protocol"],
        session_parameters=fakesnow_server["session_parameters"],
        network_timeout=fakesnow_server["network_timeout"],
    )

    # Test: Setup data
    sf.execute("CREATE TABLE sales (product STRING, amount INT)")
    sf.execute("""
        INSERT INTO sales VALUES
        ('Widget A', 100),
        ('Widget B', 200),
        ('Widget A', 150)
    """)

    # Test: Execute aggregation query
    result = sf.execute("""
        SELECT product, SUM(amount) as total_sales
        FROM sales
        GROUP BY product
        ORDER BY product
    """)

    # Verify: Check aggregated results
    assert len(result) == 2
    assert result[0] == ("Widget A", 250)
    assert result[1] == ("Widget B", 200)


def test_execute_empty_result(fakesnow_server: dict, temp_credentials: str) -> None:
    """Test executing a query that returns no results."""
    sf = Snowflake(
        role="test_role",
        database="test_db",
        schema="test_schema",
        warehouse="test_warehouse",
        account=fakesnow_server["account"],
        query_tag="test_query",
        sf_secret_location=temp_credentials,
        host=fakesnow_server["host"],
        port=fakesnow_server["port"],
        protocol=fakesnow_server["protocol"],
        session_parameters=fakesnow_server["session_parameters"],
        network_timeout=fakesnow_server["network_timeout"],
    )

    # Test: Create empty table and query it
    sf.execute("CREATE TABLE empty_table (id INT)")
    result = sf.execute("SELECT * FROM empty_table")

    # Verify: Result should be empty
    assert result == []
