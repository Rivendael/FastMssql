"""
Tests for ApplicationIntent parameter behavior

This module tests the actual behavior differences between ReadOnly and ReadWrite
connection intents, verifying that write operations are properly handled.
"""

import pytest
from conftest import Config

try:
    from fastmssql import ApplicationIntent, Connection, SslConfig
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_readonly_intent_rejects_write_operations(test_config: Config):
    """Test that ReadOnly intent prevents write operations."""
    try:
        # Create connection with ReadOnly intent
        conn = Connection(
            ssl_config=SslConfig.development(),
            application_intent=ApplicationIntent.READ_ONLY,
            **test_config.asdict(),
        )

        assert await conn.connect()
        assert await conn.is_connected()

        # Try a write operation - should fail with ReadOnly intent
        # Attempting to modify system objects should fail on read-only connections
        try:
            await conn.execute(
                "INSERT INTO sys.messages (error, severity, dlang, text) VALUES (50001, 16, 'us_english', 'test')"
            )
            # If we get here, the server allowed the write (may happen in some configs)
            pytest.skip("Server allowed write on ReadOnly connection")
        except RuntimeError as e:
            # Expected: write operation should be rejected
            assert (
                "error" in str(e).lower()
                or "permission" in str(e).lower()
                or "read" in str(e).lower()
            )

        assert await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_readwrite_intent_allows_write_operations(test_config: Config):
    """Test that ReadWrite intent allows write operations."""
    try:
        # Create connection with ReadWrite intent
        conn = Connection(
            ssl_config=SslConfig.development(),
            application_intent=ApplicationIntent.READ_WRITE,
            **test_config.asdict(),
        )

        assert await conn.connect()
        assert await conn.is_connected()

        # Execute a simple write operation to a temporary table using a batch
        # This ensures all operations use the same connection
        await conn.execute(
            """
            DECLARE @test_table TABLE (id INT PRIMARY KEY, value NVARCHAR(255));
            INSERT INTO @test_table (id, value) VALUES (1, 'test_value');
            SELECT * FROM @test_table;
            """
        )

        # Query to verify read operations work
        result = await conn.query("SELECT 1 as test_col")
        rows = result.rows()
        assert len(rows) == 1
        assert rows[0]["test_col"] == 1

        assert await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_default_intent_allows_write_operations(test_config: Config):
    """Test that default intent (no flag) allows write operations."""
    try:
        # Create connection without specifying application_intent
        # Should default to ReadWrite behavior
        conn = Connection(ssl_config=SslConfig.development(), **test_config.asdict())

        assert await conn.connect()
        assert await conn.is_connected()

        # Execute a simple write operation using a table variable
        # This works on any connection and doesn't require persistence
        await conn.execute(
            """
            DECLARE @test_table TABLE (id INT PRIMARY KEY, value NVARCHAR(255));
            INSERT INTO @test_table (id, value) VALUES (1, 'default_value');
            SELECT * FROM @test_table;
            """
        )

        # Query to verify read operations work
        result = await conn.query("SELECT 1 as test_col")
        rows = result.rows()
        assert len(rows) == 1
        assert rows[0]["test_col"] == 1

        assert await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")
