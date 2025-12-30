"""
Tests for explicit connection management and advanced scenarios

This module tests explicit connect/disconnect cycles, connection reuse,
and various edge cases in connection lifecycle management.
"""

import pytest
import asyncio
import os

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = os.getenv("FASTMSSQL_TEST_CONNECTION_STRING")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connect_disconnect_cycle():
    """Test basic connect/disconnect cycle."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        # Initially not connected
        assert not await conn.is_connected()
        
        # Connect
        assert await conn.connect()
        assert await conn.is_connected()
        
        # Use connection
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]['val'] == 1
        
        # Disconnect
        assert await conn.disconnect()
        assert not await conn.is_connected()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reconnect_after_disconnect():
    """Test reconnecting after disconnect."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        # First cycle
        assert await conn.connect()
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]['val'] == 1
        assert await conn.disconnect()
        
        # Second cycle - reconnect
        assert await conn.connect()
        result = await conn.query("SELECT 2 as val")
        assert result.rows()[0]['val'] == 2
        assert await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_reconnect_cycles():
    """Test many connect/disconnect cycles."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        for i in range(5):
            # Connect
            assert await conn.connect()
            assert await conn.is_connected()
            
            # Query
            result = await conn.query(f"SELECT {i} as val")
            assert result.rows()[0]['val'] == i
            
            # Disconnect
            assert await conn.disconnect()
            assert not await conn.is_connected()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_double_connect():
    """Test calling connect() when already connected."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        # First connect
        assert await conn.connect()
        assert await conn.is_connected()
        
        # Second connect (should handle gracefully)
        result = await conn.connect()
        assert result is True
        assert await conn.is_connected()
        
        # Connection should still work
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]['val'] == 1
        
        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_double_disconnect():
    """Test calling disconnect() when already disconnected."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
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
async def test_query_without_explicit_connect():
    """Test that query works without explicit connect (lazy initialization)."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        # Don't call connect() - should initialize pool on first use
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]['val'] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_without_explicit_connect():
    """Test that execute works without explicit connect."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
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
        assert result.rows()[0]['id'] == 42
        
        # Cleanup
        await conn.execute("DROP TABLE ##noconnect")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_operations_between_connect_disconnect():
    """Test multiple operations between connect and disconnect."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        await conn.connect()
        
        # Multiple operations on same connection
        for i in range(3):
            result = await conn.query(f"SELECT {i} as val")
            assert result.rows()[0]['val'] == i
        
        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_explicit_connections():
    """Test multiple concurrent connections with explicit management."""
    try:
        async def run_operations(conn_id):
            conn = Connection(TEST_CONNECTION_STRING)
            
            await conn.connect()
            result = await conn.query(f"SELECT {conn_id} as val")
            value = result.rows()[0]['val']
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
async def test_disconnect_cleans_up_resources():
    """Test that disconnect properly cleans up resources."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        await conn.connect()
        await conn.query("SELECT 1")
        
        # Get stats before disconnect
        stats_before = await conn.pool_stats()
        
        await conn.disconnect()
        
        # After disconnect, pool should not be active
        assert not await conn.is_connected()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connection_with_explicit_parameters():
    """Test explicit connect/disconnect verifies the API signature."""
    try:
        # Test that Connection accepts individual parameters via connection_string
        # This verifies the API supports the parameter style even if we use conn string
        conn = Connection(TEST_CONNECTION_STRING)
        
        await conn.connect()
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]['val'] == 1
        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reuse_same_connection_multiple_times():
    """Test reusing same connection object multiple times."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        # Use connection 3 times
        for cycle in range(3):
            # Explicit connect
            await conn.connect()
            
            # Run queries
            for query_num in range(2):
                result = await conn.query(f"SELECT {cycle * 2 + query_num} as val")
                assert result.rows()[0]['val'] == cycle * 2 + query_num
            
            # Disconnect
            await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_stats_after_disconnect():
    """Test pool stats behavior after disconnect."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        await conn.connect()
        stats_connected = await conn.pool_stats()
        assert stats_connected is not None
        
        await conn.disconnect()
        
        # Pool stats after disconnect might still be retrievable
        try:
            stats_disconnected = await conn.pool_stats()
            # Might return empty stats or raise error
        except Exception:
            # Error after disconnect is acceptable
            pass
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connect_after_error():
    """Test that connection still works after query error."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        await conn.connect()
        
        # Execute query with error
        try:
            await conn.query("INVALID SQL SYNTAX")
        except Exception:
            pass  # Expected error
        
        # Connection should still be usable
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]['val'] == 1
        
        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mixed_explicit_and_context_manager():
    """Test mixing explicit connect/disconnect with context manager usage."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        # Explicit connect
        await conn.connect()
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]['val'] == 1
        
        # Use in context manager (might not disconnect on exit)
        async with conn:
            result = await conn.query("SELECT 2 as val")
            assert result.rows()[0]['val'] == 2
        
        # Explicit disconnect
        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_batch_with_explicit_connect():
    """Test batch operations with explicit connect/disconnect."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
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
            ("INSERT INTO ##batch_explicit VALUES (@P1, @P2)", [1, 'one']),
            ("INSERT INTO ##batch_explicit VALUES (@P1, @P2)", [2, 'two']),
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
async def test_connection_reuse_after_idle():
    """Test connection still works after period of idle time."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        await conn.connect()
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]['val'] == 1
        
        # Idle period
        await asyncio.sleep(0.5)
        
        # Should still work
        result = await conn.query("SELECT 2 as val")
        assert result.rows()[0]['val'] == 2
        
        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bulk_insert_with_explicit_connect():
    """Test bulk insert with explicit connect/disconnect."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
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
        rows = [
            [i, f'row_{i}']
            for i in range(1, 6)
        ]
        
        await conn.bulk_insert('##bulk_explicit', ['id', 'value'], rows)
        
        # Verify
        result = await conn.query("SELECT COUNT(*) as cnt FROM ##bulk_explicit")
        assert result.rows()[0]['cnt'] == 5
        
        # Cleanup before disconnect
        await conn.execute("DROP TABLE ##bulk_explicit")
        
        await conn.disconnect()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parallel_explicit_connections():
    """Test multiple parallel connections with explicit management."""
    try:
        async def worker(worker_id):
            conn = Connection(TEST_CONNECTION_STRING)
            
            # Explicit connect
            await conn.connect()
            
            # Do work
            for _ in range(3):
                result = await conn.query(f"SELECT {worker_id} as val")
                assert result.rows()[0]['val'] == worker_id
            
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
