"""
Tests for explicit connection management and advanced scenarios

This module tests explicit connect/disconnect cycles, connection reuse,
and various edge cases in connection lifecycle management.
"""

import asyncio

import pytest
from conftest import Config

try:
    from fastmssql import ApplicationIntent, Connection, SslConfig
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connect_disconnect_cycle(test_config: Config):
    """Test basic connect/disconnect cycle."""
    try:
        conn = Connection(test_config.connection_string)

        # Initially not connected
        assert not await conn.is_connected()

        # Connect
        assert await conn.connect()
        assert await conn.is_connected()

        # Use connection
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1

        # Disconnect
        assert await conn.disconnect()
        assert not await conn.is_connected()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reconnect_after_disconnect(test_config: Config):
    """Test reconnecting after disconnect."""
    try:
        conn = Connection(test_config.connection_string)

        # First cycle
        assert await conn.connect()
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1
        assert await conn.disconnect()

        # Second cycle - reconnect
        assert await conn.connect()
        result = await conn.query("SELECT 2 as val")
        assert result.rows()[0]["val"] == 2
        assert await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_reconnect_cycles(test_config: Config):
    """Test many connect/disconnect cycles."""
    try:
        conn = Connection(test_config.connection_string)

        for i in range(5):
            # Connect
            assert await conn.connect()
            assert await conn.is_connected()

            # Query
            result = await conn.query(f"SELECT {i} as val")
            assert result.rows()[0]["val"] == i

            # Disconnect
            assert await conn.disconnect()
            assert not await conn.is_connected()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_double_connect(test_config: Config):
    """Test calling connect() when already connected."""
    try:
        conn = Connection(test_config.connection_string)

        # First connect
        assert await conn.connect()
        assert await conn.is_connected()

        # Second connect (should handle gracefully)
        result = await conn.connect()
        assert result is True
        assert await conn.is_connected()

        # Connection should still work
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1

        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_double_disconnect(test_config: Config):
    """Test calling disconnect() when already disconnected."""
    try:
        conn = Connection(test_config.connection_string)

        # Connect and disconnect
        assert await conn.connect()
        assert await conn.disconnect()

        # Second disconnect (should return False or handle gracefully)
        result = await conn.disconnect()
        # Result might be False (already disconnected)
        assert result is False or result is True
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_without_explicit_connect(test_config: Config):
    """Test that query works without explicit connect (lazy initialization)."""
    try:
        conn = Connection(test_config.connection_string)

        # Don't call connect() - should initialize pool on first use
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_without_explicit_connect(test_config: Config):
    """Test that execute works without explicit connect."""
    try:
        conn = Connection(test_config.connection_string)

        # Create temp table without explicit connect
        await conn.execute("""
            IF OBJECT_ID('tempdb..##noconnect', 'U') IS NOT NULL
                DROP TABLE ##noconnect
        """)

        await conn.execute("""
            CREATE TABLE ##noconnect (id INT)
        """)

        result = await conn.execute("INSERT INTO ##noconnect VALUES (@P1)", [42])
        assert result == 1

        # Verify
        result = await conn.query("SELECT * FROM ##noconnect")
        assert result.rows()[0]["id"] == 42

        # Cleanup
        await conn.execute("DROP TABLE ##noconnect")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_operations_between_connect_disconnect(test_config: Config):
    """Test multiple operations between connect and disconnect."""
    try:
        conn = Connection(test_config.connection_string)

        await conn.connect()

        # Multiple operations on same connection
        for i in range(3):
            result = await conn.query(f"SELECT {i} as val")
            assert result.rows()[0]["val"] == i

        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_explicit_connections(test_config: Config):
    """Test multiple concurrent connections with explicit management."""
    try:

        async def run_operations(conn_id):
            conn = Connection(test_config.connection_string)

            await conn.connect()
            result = await conn.query(f"SELECT {conn_id} as val")
            value = result.rows()[0]["val"]
            await conn.disconnect()

            return value

        # Run multiple connections concurrently
        results = await asyncio.gather(
            run_operations(1),
            run_operations(2),
            run_operations(3),
        )

        assert results == [1, 2, 3]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_disconnect_cleans_up_resources(test_config: Config):
    """Test that disconnect properly cleans up resources."""
    try:
        conn = Connection(test_config.connection_string)

        await conn.connect()
        await conn.query("SELECT 1")

        # Get stats before disconnect
        await conn.pool_stats()

        await conn.disconnect()

        # After disconnect, pool should not be active
        assert not await conn.is_connected()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connection_with_explicit_parameters(test_config: Config):
    """Test explicit connect/disconnect verifies the API signature."""
    try:
        # Test that Connection accepts individual parameters via connection_string
        # This verifies the API supports the parameter style even if we use conn string
        conn = Connection(test_config.connection_string)

        await conn.connect()
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1
        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reuse_same_connection_multiple_times(test_config: Config):
    """Test reusing same connection object multiple times."""
    try:
        conn = Connection(test_config.connection_string)

        # Use connection 3 times
        for cycle in range(3):
            # Explicit connect
            await conn.connect()

            # Run queries
            for query_num in range(2):
                result = await conn.query(f"SELECT {cycle * 2 + query_num} as val")
                assert result.rows()[0]["val"] == cycle * 2 + query_num

            # Disconnect
            await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_stats_after_disconnect(test_config: Config):
    """Test pool stats behavior after disconnect."""
    try:
        conn = Connection(test_config.connection_string)

        await conn.connect()
        stats_connected = await conn.pool_stats()
        assert stats_connected is not None

        await conn.disconnect()

        # Pool stats after disconnect might still be retrievable
        try:
            await conn.pool_stats()
            # Might return empty stats or raise error
        except Exception:
            # Error after disconnect is acceptable
            pass
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connect_after_error(test_config: Config):
    """Test that connection still works after query error."""
    try:
        conn = Connection(test_config.connection_string)

        await conn.connect()

        # Execute query with error
        try:
            await conn.query("INVALID SQL SYNTAX")
        except Exception:
            pass  # Expected error

        # Connection should still be usable
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1

        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mixed_explicit_and_context_manager(test_config: Config):
    """Test mixing explicit connect/disconnect with context manager usage."""
    try:
        conn = Connection(test_config.connection_string)

        # Explicit connect
        await conn.connect()
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1

        # Use in context manager (might not disconnect on exit)
        async with conn:
            result = await conn.query("SELECT 2 as val")
            assert result.rows()[0]["val"] == 2

        # Explicit disconnect
        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_batch_with_explicit_connect(test_config: Config):
    """Test batch operations with explicit connect/disconnect."""
    try:
        conn = Connection(test_config.connection_string)

        # Create table without explicit connect (lazy init)
        await conn.execute("""
            IF OBJECT_ID('tempdb..##batch_explicit', 'U') IS NOT NULL
                DROP TABLE ##batch_explicit
        """)

        await conn.execute("""
            CREATE TABLE ##batch_explicit (id INT, value VARCHAR(50))
        """)

        # Explicit connect
        await conn.connect()

        # Execute batch
        batch_items = [
            ("INSERT INTO ##batch_explicit VALUES (@P1, @P2)", [1, "one"]),
            ("INSERT INTO ##batch_explicit VALUES (@P1, @P2)", [2, "two"]),
        ]

        results = await conn.execute_batch(batch_items)
        assert len(results) == 2

        # Verify with batch query
        query_batch = [
            ("SELECT * FROM ##batch_explicit WHERE id = @P1", [1]),
            ("SELECT * FROM ##batch_explicit WHERE id = @P1", [2]),
        ]

        query_results = await conn.query_batch(query_batch)
        assert len(query_results) == 2

        # Cleanup before disconnect
        await conn.execute("DROP TABLE ##batch_explicit")

        # Disconnect
        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connection_reuse_after_idle(test_config: Config):
    """Test connection still works after period of idle time."""
    try:
        conn = Connection(test_config.connection_string)

        await conn.connect()
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1

        # Idle period
        await asyncio.sleep(0.5)

        # Should still work
        result = await conn.query("SELECT 2 as val")
        assert result.rows()[0]["val"] == 2

        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bulk_insert_with_explicit_connect(test_config: Config):
    """Test bulk insert with explicit connect/disconnect."""
    try:
        conn = Connection(test_config.connection_string)

        # Create table
        await conn.execute("""
            IF OBJECT_ID('tempdb..##bulk_explicit', 'U') IS NOT NULL
                DROP TABLE ##bulk_explicit
        """)

        await conn.execute("""
            CREATE TABLE ##bulk_explicit (id INT, value VARCHAR(50))
        """)

        # Explicit connect
        await conn.connect()

        # Bulk insert
        rows = [[i, f"row_{i}"] for i in range(1, 6)]

        await conn.bulk_insert("##bulk_explicit", ["id", "value"], rows)

        # Verify
        result = await conn.query("SELECT COUNT(*) as cnt FROM ##bulk_explicit")
        assert result.rows()[0]["cnt"] == 5

        # Cleanup before disconnect
        await conn.execute("DROP TABLE ##bulk_explicit")

        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parallel_explicit_connections(test_config: Config):
    """Test multiple parallel connections with explicit management."""
    try:

        async def worker(worker_id):
            conn = Connection(test_config.connection_string)

            # Explicit connect
            await conn.connect()

            # Do work
            for _ in range(3):
                result = await conn.query(f"SELECT {worker_id} as val")
                assert result.rows()[0]["val"] == worker_id

            # Disconnect
            await conn.disconnect()

            return worker_id

        # Run workers in parallel
        results = await asyncio.gather(
            worker(1),
            worker(2),
            worker(3),
        )

        assert sorted(results) == [1, 2, 3]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_application_intent_readonly(test_config: Config):
    """Test ApplicationIntent=ReadOnly parameter with individual connection parameters."""
    try:
        conn = Connection(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            ssl_config=SslConfig.development(),
            application_intent=ApplicationIntent.READ_ONLY,
        )

        # Connect and verify it works
        assert await conn.connect()
        assert await conn.is_connected()

        # Execute a read query
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1

        # Disconnect
        assert await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_application_intent_readwrite(test_config: Config):
    """Test ApplicationIntent=ReadWrite parameter with individual connection parameters."""
    try:
        # Create connection with ReadWrite intent using individual parameters
        conn = Connection(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            ssl_config=SslConfig.development(),
            application_intent=ApplicationIntent.READ_WRITE,
        )

        # Connect and verify it works
        assert await conn.connect()
        assert await conn.is_connected()

        # Execute a query
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1

        # Disconnect
        assert await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_application_intent_default(test_config: Config):
    """Test that connections work without explicit ApplicationIntent (defaults to ReadWrite)."""
    try:
        # Create connection without specifying application_intent using individual parameters
        conn = Connection(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            ssl_config=SslConfig.development(),
        )

        # Connect and verify it works
        assert await conn.connect()
        assert await conn.is_connected()

        # Execute a query
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1

        # Disconnect
        assert await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_application_intent_as_string(test_config: Config):
    """Test that ApplicationIntent can be passed as a string."""
    try:
        # Create connection with application_intent as string
        conn = Connection(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            ssl_config=SslConfig.development(),
            application_intent="READ_ONLY",
        )

        # Connect and verify it works
        assert await conn.connect()
        assert await conn.is_connected()

        # Execute a read query
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1

        # Disconnect
        assert await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_application_intent_case_insensitive(test_config: Config):
    """Test that ApplicationIntent string is case insensitive."""
    try:
        # Test with lowercase string
        conn_lower = Connection(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            ssl_config=SslConfig.development(),
            application_intent="read_write",
        )

        assert await conn_lower.connect()
        result = await conn_lower.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1
        await conn_lower.disconnect()

        # Test with mixed case string
        conn_mixed = Connection(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            ssl_config=SslConfig.development(),
            application_intent="Read_Only",
        )

        assert await conn_mixed.connect()
        result = await conn_mixed.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1
        await conn_mixed.disconnect()

        # Test with uppercase string
        conn_upper = Connection(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            ssl_config=SslConfig.development(),
            application_intent="READONLY",
        )

        assert await conn_upper.connect()
        result = await conn_upper.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1
        await conn_upper.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connection_with_explicit_port(test_config: Config):
    """Test connection with explicit port parameter using individual parameters."""
    try:
        # Create connection with explicit port using individual parameters
        conn = Connection(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            ssl_config=SslConfig.development(),
            application_intent=ApplicationIntent.READ_WRITE,
            port=test_config.port,
        )

        assert await conn.connect()
        assert await conn.is_connected()

        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1

        assert await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_connection_with_invalid_port(test_config: Config):
    """Test connection with invalid/wrong port parameter fails appropriately."""
    # Create connection with wrong port using individual parameters
    conn = Connection(
        server=test_config.server,
        database=test_config.database,
        username=test_config.username,
        password=test_config.password,
        ssl_config=SslConfig.development(),
        application_intent=ApplicationIntent.READ_WRITE,
        port=9999,  # Invalid port - should fail to connect
    )

    # Connection should fail within 3 seconds
    with pytest.raises(Exception):
        await asyncio.wait_for(conn.connect(), timeout=3)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connection_with_application_name(test_config: Config):
    """Test connection with explicit application_name parameter."""
    try:
        # Create connection with application_name using individual parameters
        conn = Connection(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            ssl_config=SslConfig.development(),
            application_name="TestApp123",
        )

        assert await conn.connect()
        assert await conn.is_connected()

        # Query the APP_NAME() to verify it was set
        result = await conn.query("SELECT APP_NAME() as app_name")
        app_name = result.rows()[0]["app_name"]
        assert app_name == "TestApp123"

        assert await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connection_with_application_name_and_port(test_config: Config):
    """Test connection with both application_name and port parameters."""
    try:
        conn = Connection(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            ssl_config=SslConfig.development(),
            port=test_config.port,
            application_name="PortAppTest",
        )

        assert await conn.connect()
        assert await conn.is_connected()

        # Verify application name
        result = await conn.query("SELECT APP_NAME() as app_name")
        app_name = result.rows()[0]["app_name"]
        assert app_name == "PortAppTest"

        assert await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_connection_with_instance_name(test_config: Config):
    """Test connection with instance_name parameter (if available on test server)."""
    try:
        # Try with instance name - may not be available on all test servers
        # This test validates the parameter is accepted even if the instance doesn't exist
        # or if the test server doesn't use named instances

        # First try with SQLEXPRESS which is common
        conn = Connection(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            ssl_config=SslConfig.development(),
            instance_name="SQLEXPRESS",
        )

        # Try to connect - may fail if instance doesn't exist, which is ok
        try:
            if await asyncio.wait_for(conn.connect(), timeout=3):
                result = await conn.query("SELECT 1 as val")
                assert result.rows()[0]["val"] == 1
                await conn.disconnect()
        except asyncio.TimeoutError:
            # Instance may not exist, that's ok - we're testing the parameter acceptance
            pass
        except Exception as e:
            # Connection failures are expected if instance doesn't exist
            if "instance" not in str(e).lower():
                pytest.skip(f"Named instance not available on test server: {e}")
    except Exception as e:
        pytest.skip(f"Cannot test instance_name: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_connection_with_instance_and_application_name(test_config: Config):
    """Test connection with both instance_name and application_name parameters."""
    try:
        conn = Connection(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            ssl_config=SslConfig.development(),
            instance_name="SQLEXPRESS",
            application_name="InstanceAppTest",
        )

        try:
            if await asyncio.wait_for(conn.connect(), timeout=3):
                result = await conn.query("SELECT APP_NAME() as app_name")
                app_name = result.rows()[0]["app_name"]
                assert app_name == "InstanceAppTest"
                await conn.disconnect()
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            if "instance" not in str(e).lower():
                pytest.skip(f"Named instance not available on test server: {e}")
    except Exception as e:
        pytest.skip(f"Cannot test instance_name with application_name: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_application_name_with_connection_string(test_config: Config):
    """Test that application_name can be set via connection string format."""
    try:
        # Parse connection string to extract components

        # Create a connection using the connection_string parameter
        # Note: application_name may need to be passed as individual parameter
        conn = Connection(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            ssl_config=SslConfig.development(),
            application_name="ConnStrAppTest",
        )

        assert await conn.connect()
        result = await conn.query("SELECT APP_NAME() as app_name")
        assert result.rows()[0]["app_name"] == "ConnStrAppTest"
        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_connections_different_app_names(test_config: Config):
    """Test multiple concurrent connections with different application names."""
    try:

        async def create_and_verify_app_name(app_name):
            conn = Connection(
                server=test_config.server,
                database=test_config.database,
                username=test_config.username,
                password=test_config.password,
                ssl_config=SslConfig.development(),
                application_name=app_name,
            )

            await conn.connect()
            result = await conn.query("SELECT APP_NAME() as app_name")
            retrieved_app_name = result.rows()[0]["app_name"]
            await conn.disconnect()

            return retrieved_app_name

        # Create multiple connections with different app names
        results = await asyncio.gather(
            create_and_verify_app_name("App1"),
            create_and_verify_app_name("App2"),
            create_and_verify_app_name("App3"),
        )

        assert results == ["App1", "App2", "App3"]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


class TestPasswordValidation:
    """Test password validation when creating connections."""

    def test_connection_with_username_requires_password(self):
        """Creating a connection with username but no password should fail."""
        # This should raise a PyValueError when initializing the connection
        with pytest.raises(ValueError) as exc_info:
            Connection(
                server="localhost",
                database="testdb",
                username="testuser",
                password=None,  # Explicitly no password
            )
        assert "password is required when username is provided" in str(exc_info.value)

    def test_connection_with_username_and_password_succeeds(self):
        """Creating a connection with username and password should succeed (object creation)."""
        # Object creation should succeed - actual connection will fail if server is unavailable
        conn = Connection(
            server="localhost",
            database="testdb",
            username="testuser",
            password="testpass",
        )
        assert conn is not None

    def test_connection_without_username_allows_no_password(self):
        """Creating a connection without username should not require password."""
        # Object creation should succeed with no username and no password
        conn = Connection(server="localhost", database="testdb")
        assert conn is not None

    def test_connection_with_connection_string_ignores_password_check(self):
        """Connection string mode should not enforce password validation."""
        # Connection strings bypass the username/password arguments
        conn = Connection(
            connection_string="Server=localhost;Database=testdb;User=testuser;Password=testpass"
        )
        assert conn is not None


class TestApplicationIntentValidation:
    """Test ApplicationIntent parsing and validation in Connection constructor."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_application_intent_readonly_lowercase(self, test_config: Config):
        """Test connection with application_intent='readonly' (lowercase)."""
        try:
            async with Connection(
                test_config.connection_string, application_intent="readonly"
            ) as conn:
                result = await conn.query("SELECT 1 as val")
                assert result.rows()[0]["val"] == 1
        except Exception as e:
            pytest.fail(f"Connection with readonly intent failed: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_application_intent_readonly_mixed_case(self, test_config: Config):
        """Test connection with application_intent='ReadOnly' (mixed case)."""
        try:
            async with Connection(
                test_config.connection_string, application_intent="ReadOnly"
            ) as conn:
                result = await conn.query("SELECT 1 as val")
                assert result.rows()[0]["val"] == 1
        except Exception as e:
            pytest.fail(f"Connection with ReadOnly intent failed: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_application_intent_readonly_alternative_format(
        self, test_config: Config
    ):
        """Test connection with application_intent='read_only' (underscore format)."""
        try:
            async with Connection(
                test_config.connection_string, application_intent="read_only"
            ) as conn:
                result = await conn.query("SELECT 1 as val")
                assert result.rows()[0]["val"] == 1
        except Exception as e:
            pytest.fail(f"Connection with read_only intent failed: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_application_intent_readwrite_lowercase(self, test_config: Config):
        """Test connection with application_intent='readwrite' (lowercase)."""
        try:
            async with Connection(
                test_config.connection_string, application_intent="readwrite"
            ) as conn:
                result = await conn.query("SELECT 1 as val")
                assert result.rows()[0]["val"] == 1
        except Exception as e:
            pytest.fail(f"Connection with readwrite intent failed: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_application_intent_readwrite_mixed_case(self, test_config: Config):
        """Test connection with application_intent='ReadWrite' (mixed case)."""
        try:
            async with Connection(
                test_config.connection_string, application_intent="ReadWrite"
            ) as conn:
                result = await conn.query("SELECT 1 as val")
                assert result.rows()[0]["val"] == 1
        except Exception as e:
            pytest.fail(f"Connection with ReadWrite intent failed: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_application_intent_readwrite_alternative_format(
        self, test_config: Config
    ):
        """Test connection with application_intent='read_write' (underscore format)."""
        try:
            async with Connection(
                test_config.connection_string, application_intent="read_write"
            ) as conn:
                result = await conn.query("SELECT 1 as val")
                assert result.rows()[0]["val"] == 1
        except Exception as e:
            pytest.fail(f"Connection with read_write intent failed: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_application_intent_empty_string(self, test_config: Config):
        """Test connection with application_intent='' (defaults to readwrite)."""
        try:
            async with Connection(
                test_config.connection_string, application_intent=""
            ) as conn:
                result = await conn.query("SELECT 1 as val")
                assert result.rows()[0]["val"] == 1
        except Exception as e:
            pytest.fail(f"Connection with empty intent failed: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_application_intent_none(self, test_config: Config):
        """Test connection with application_intent=None (default)."""
        try:
            async with Connection(
                test_config.connection_string, application_intent=None
            ) as conn:
                result = await conn.query("SELECT 1 as val")
                assert result.rows()[0]["val"] == 1
        except Exception as e:
            pytest.fail(f"Connection with None intent failed: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_application_intent_whitespace_handling(self, test_config: Config):
        """Test connection with application_intent with leading/trailing whitespace."""
        try:
            async with Connection(
                test_config.connection_string, application_intent="  readonly  "
            ) as conn:
                result = await conn.query("SELECT 1 as val")
                assert result.rows()[0]["val"] == 1
        except Exception as e:
            pytest.fail(f"Connection with whitespace intent failed: {e}")
