import pytest
import os

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("mssql wrapper not available - make sure mssql.py is importable", allow_module_level=True)

# Test configuration - adjust as needed
TEST_CONNECTION_STRING = os.getenv("FASTMSSQL_TEST_CONNECTION_STRING")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connect_and_disconnect_basic():
    """Ensure that connect() establishes and disconnect() closes the connection."""
    if not TEST_CONNECTION_STRING:
        pytest.fail("No test connection string set")

    conn = Connection(TEST_CONNECTION_STRING)

    # Initially not connected
    assert not await conn.is_connected()

    # Connect
    connected = await conn.connect()
    assert connected is True
    assert await conn.is_connected()

    # Disconnect
    disconnected = await conn.disconnect()
    assert disconnected is True
    assert not await conn.is_connected()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_double_connect_and_disconnect():
    """Ensure multiple connect/disconnect calls behave correctly (idempotent)."""
    if not TEST_CONNECTION_STRING:
        pytest.fail("No test connection string set")

    conn = Connection(TEST_CONNECTION_STRING)

    # Connect twice
    assert await conn.connect() is True
    assert await conn.connect() is True  # should just return True again
    assert await conn.is_connected()

    # Disconnect twice
    assert await conn.disconnect() is True
    assert await conn.disconnect() is False  # already disconnected
    assert not await conn.is_connected()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_requires_connect():
    """Verify that queries fail if connect() is not called first."""
    if not TEST_CONNECTION_STRING:
        pytest.fail("No test connection string set")

    conn = Connection(TEST_CONNECTION_STRING)

    # Without connecting, threadpool should be automatically created
    result = await conn.query("SELECT 1")
    rows = result.rows() if result.has_rows() else []

    # After connecting, query should succeed
    await conn.connect()
    result = await conn.query("SELECT 1 as value")
    rows = result.rows() if result.has_rows() else []
    assert rows[0]["value"] == 1

    # Disconnect should drop the pool
    await conn.disconnect()
    assert not await conn.is_connected()
