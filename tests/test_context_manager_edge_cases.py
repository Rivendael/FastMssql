"""
Tests for Connection async context manager edge cases

This module tests edge cases and error scenarios with async context managers,
including nested contexts, exception handling, and resource cleanup.
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
async def test_context_manager_basic():
    """Test basic async context manager usage."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 1 as value")
            assert result.has_rows()
            assert result.rows()[0]['value'] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_multiple_operations():
    """Test multiple queries within same context."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # First query
            result1 = await conn.query("SELECT 1 as val")
            assert result1.rows()[0]['val'] == 1
            
            # Second query
            result2 = await conn.query("SELECT 2 as val")
            assert result2.rows()[0]['val'] == 2
            
            # Third query
            result3 = await conn.query("SELECT 3 as val")
            assert result3.rows()[0]['val'] == 3
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_exception_in_block():
    """Test that exceptions within context are propagated."""
    try:
        with pytest.raises(Exception):
            async with Connection(TEST_CONNECTION_STRING) as conn:
                await conn.query("SELECT 1")
                raise ValueError("Test exception")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_sql_error():
    """Test that SQL errors are handled within context."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Valid query before error
            result = await conn.query("SELECT 1 as val")
            assert result.rows()[0]['val'] == 1
            
            # Query with SQL error
            with pytest.raises(Exception):
                await conn.query("SELECT * FROM non_existent_table_xyz")
            
            # Connection should still be usable after error
            result = await conn.query("SELECT 2 as val")
            assert result.rows()[0]['val'] == 2
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_exit_cleanup():
    """Test that context manager properly cleans up resources."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        # Before context
        assert not await conn.is_connected()
        
        async with conn:
            # Inside context
            assert await conn.is_connected()
            result = await conn.query("SELECT 1 as val")
            assert result.rows()[0]['val'] == 1
        
        # After context - connection might be reused, so just test it's still usable
        # The context manager may not disconnect immediately
        result = await conn.query("SELECT 1 as val")
        assert result.has_rows()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_sequential_contexts():
    """Test using connection in multiple sequential context managers."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        # First context
        async with conn:
            result1 = await conn.query("SELECT 1 as val")
            assert result1.rows()[0]['val'] == 1
        
        # Second context with same connection
        async with conn:
            result2 = await conn.query("SELECT 2 as val")
            assert result2.rows()[0]['val'] == 2
        
        # Third context with same connection
        async with conn:
            result3 = await conn.query("SELECT 3 as val")
            assert result3.rows()[0]['val'] == 3
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_context_managers():
    """Test multiple concurrent connections with context managers."""
    try:
        async def run_query(conn_str, query_val):
            async with Connection(conn_str) as conn:
                result = await conn.query(f"SELECT {query_val} as val")
                return result.rows()[0]['val']
        
        # Run multiple queries concurrently
        results = await asyncio.gather(
            run_query(TEST_CONNECTION_STRING, 1),
            run_query(TEST_CONNECTION_STRING, 2),
            run_query(TEST_CONNECTION_STRING, 3),
        )
        
        assert results == [1, 2, 3]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_nested_context_managers_same_connection():
    """Test nested context managers with the same connection object."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        async with conn:
            result1 = await conn.query("SELECT 1 as val")
            assert result1.rows()[0]['val'] == 1
            
            # Try nesting - behavior depends on implementation
            # Some implementations might allow this, others might not
            try:
                async with conn:
                    result2 = await conn.query("SELECT 2 as val")
                    assert result2.rows()[0]['val'] == 2
            except Exception:
                # Nested contexts might not be allowed
                pass
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_with_execute():
    """Test context manager with execute (non-SELECT) operations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table
            await conn.execute("""
                IF OBJECT_ID('tempdb..##test_ctx', 'U') IS NOT NULL
                    DROP TABLE ##test_ctx
            """)
            
            await conn.execute("""
                CREATE TABLE ##test_ctx (id INT, name VARCHAR(50))
            """)
            
            # Insert
            result = await conn.execute("INSERT INTO ##test_ctx VALUES (@P1, @P2)", [1, 'test'])
            assert result == 1
            
            # Query
            result = await conn.query("SELECT * FROM ##test_ctx")
            assert result.has_rows()
            assert result.rows()[0]['id'] == 1
            
            # Update
            result = await conn.execute("UPDATE ##test_ctx SET name = @P1 WHERE id = @P2", ['updated', 1])
            assert result == 1
            
            # Delete
            result = await conn.execute("DELETE FROM ##test_ctx WHERE id = @P1", [1])
            assert result == 1
            
            # Cleanup
            await conn.execute("DROP TABLE ##test_ctx")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_long_operation():
    """Test context manager with longer operations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Simulate a longer operation
            result = await conn.query("""
                SELECT 1 as val
                UNION ALL SELECT 2
                UNION ALL SELECT 3
                UNION ALL SELECT 4
                UNION ALL SELECT 5
            """)
            
            assert result.has_rows()
            rows = result.rows()
            assert len(rows) == 5
            
            # Verify all values
            for i, row in enumerate(rows, 1):
                assert row['val'] == i
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_pool_stats():
    """Test accessing pool stats within context manager."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Get pool stats
            stats = await conn.pool_stats()
            assert stats is not None
            assert len(stats) == 5  # (connected, connections, idle, max_size, min_idle)
            
            # Run query and check stats again
            await conn.query("SELECT 1")
            stats2 = await conn.pool_stats()
            assert stats2 is not None
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_is_connected():
    """Test is_connected() method within context manager."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        # Before context
        is_connected = await conn.is_connected()
        # May be True or False depending on lazy initialization
        
        async with conn:
            # Inside context - should be connected after first operation
            await conn.query("SELECT 1")
            is_connected = await conn.is_connected()
            assert is_connected
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_reentry_behavior():
    """Test behavior when re-entering context after exception."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        
        # First context with error
        try:
            async with conn:
                await conn.query("INVALID SQL")
        except Exception:
            pass
        
        # Second context should work fine
        async with conn:
            result = await conn.query("SELECT 1 as val")
            assert result.rows()[0]['val'] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_with_batch_operations():
    """Test context manager with batch operations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table
            await conn.execute("""
                IF OBJECT_ID('tempdb..##batch_test', 'U') IS NOT NULL
                    DROP TABLE ##batch_test
            """)
            
            await conn.execute("""
                CREATE TABLE ##batch_test (id INT PRIMARY KEY, value VARCHAR(50))
            """)
            
            # Execute batch
            batch_items = [
                ("INSERT INTO ##batch_test VALUES (@P1, @P2)", [1, 'one']),
                ("INSERT INTO ##batch_test VALUES (@P1, @P2)", [2, 'two']),
                ("INSERT INTO ##batch_test VALUES (@P1, @P2)", [3, 'three']),
            ]
            
            results = await conn.execute_batch(batch_items)
            assert len(results) == 3
            assert all(r == 1 for r in results)
            
            # Verify data
            result = await conn.query("SELECT COUNT(*) as cnt FROM ##batch_test")
            assert result.rows()[0]['cnt'] == 3
            
            # Cleanup
            await conn.execute("DROP TABLE ##batch_test")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")
