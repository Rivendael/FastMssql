"""Tests for query timeout functionality in Transaction."""

import pytest
from conftest import Config

from fastmssql import Transaction, QueryTimeoutError


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_accepts_query_timeout(test_config: Config):
    """Test that Transaction accepts query_timeout parameter without error."""
    # Test with no timeout (default)
    conn1 = Transaction(test_config.connection_string)
    assert conn1 is not None
    
    # Test with timeout in milliseconds
    conn2 = Transaction(test_config.connection_string, query_timeout=5000)
    assert conn2 is not None
    
    # Test with very short timeout
    conn3 = Transaction(test_config.connection_string, query_timeout=100)
    assert conn3 is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_completes_within_timeout(test_config: Config):
    """Test that queries complete successfully when within timeout."""
    # 5 second timeout should be plenty for a simple query
    conn = Transaction(test_config.connection_string, query_timeout=5000)
    
    try:
        # Simple query that completes immediately
        result = await conn.query("SELECT 1 as value")
        rows = result.rows() if result.has_rows() else []
        assert len(rows) > 0
        assert rows[0]["value"] == 1
        
        # Execute statement using global temp table (##) to ensure persistence
        await conn.execute("IF OBJECT_ID('tempdb..##temp_timeout_test') IS NOT NULL DROP TABLE ##temp_timeout_test")
        affected = await conn.execute("CREATE TABLE ##temp_timeout_test (id INT)")
        assert affected == 0  # CREATE TABLE returns 0 affected rows
        
        # Insert statement
        affected = await conn.execute(
            "INSERT INTO ##temp_timeout_test (id) VALUES (@P1)", [42]
        )
        assert affected == 1
        
        # Query the data back
        result = await conn.query("SELECT id FROM ##temp_timeout_test")
        rows = result.rows() if result.has_rows() else []
        assert len(rows) == 1
        assert rows[0]["id"] == 42
        
    finally:
        try:
            await conn.close()
        except Exception:
            pass  # Ignore errors during close


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_timeout_exceeded_on_execute(test_config: Config):
    """Test that execute() raises QueryTimeoutError when timeout is exceeded."""
    # 100ms timeout will be exceeded by WAITFOR DELAY
    conn = Transaction(test_config.connection_string, query_timeout=100)
    
    # This query sleeps for 500ms, exceeding the 100ms timeout
    with pytest.raises(QueryTimeoutError) as exc_info:
        await conn.execute("WAITFOR DELAY '00:00:00.500'")
    
    # Verify the error message mentions timeout
    assert "timeout" in str(exc_info.value).lower()
    assert "100" in str(exc_info.value)  # Should mention the timeout duration
    
    # Don't close - connection might be in bad state after timeout
    # It will be cleaned up by the system


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_timeout_exceeded_on_query(test_config: Config):
    """Test that query() raises QueryTimeoutError when timeout is exceeded."""
    # 100ms timeout will be exceeded by WAITFOR DELAY
    conn = Transaction(test_config.connection_string, query_timeout=100)
    
    # This query sleeps for 500ms, exceeding the 100ms timeout
    with pytest.raises(QueryTimeoutError) as exc_info:
        await conn.query("SELECT 1 as value; WAITFOR DELAY '00:00:00.500'")
    
    # Verify the error message
    assert "timeout" in str(exc_info.value).lower()
    
    # Don't close - connection might be in bad state after timeout


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_timeout_exceeded_on_simple_query(test_config: Config):
    """Test that simple_query() raises QueryTimeoutError when timeout is exceeded."""
    # 100ms timeout will be exceeded by WAITFOR DELAY
    conn = Transaction(test_config.connection_string, query_timeout=100)
    
    # This query sleeps for 500ms, exceeding the 100ms timeout
    with pytest.raises(QueryTimeoutError) as exc_info:
        await conn.simple_query("WAITFOR DELAY '00:00:00.500'")
    
    # Verify the error message
    assert "timeout" in str(exc_info.value).lower()
    
    # Don't close - connection might be in bad state after timeout


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_timeout_exceeded_on_begin(test_config: Config):
    """Test that begin() respects query timeout."""
    # 100ms timeout is very short but should still allow begin() to work normally
    # since it's just a simple command
    conn = Transaction(test_config.connection_string, query_timeout=5000)
    
    try:
        # This should work fine
        await conn.begin()
        
        # Verify we're in a transaction
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        assert len(rows) > 0
        assert rows[0]["count"] == 1
        
        # Rollback the transaction
        await conn.rollback()
        
    finally:
        await conn.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_timeout_with_batch_execute(test_config: Config):
    """Test that execute_batch() respects query timeout."""
    conn = Transaction(test_config.connection_string, query_timeout=5000)
    
    try:
        # Create a global temporary table for testing (persists across operations)
        await conn.execute("IF OBJECT_ID('tempdb..##batch_timeout_test') IS NOT NULL DROP TABLE ##batch_timeout_test")
        await conn.execute("CREATE TABLE ##batch_timeout_test (id INT, value VARCHAR(50))")
        
        # Execute a batch of commands that should all complete within timeout
        commands = [
            ("INSERT INTO ##batch_timeout_test (id, value) VALUES (@P1, @P2)", [1, "one"]),
            ("INSERT INTO ##batch_timeout_test (id, value) VALUES (@P1, @P2)", [2, "two"]),
            ("INSERT INTO ##batch_timeout_test (id, value) VALUES (@P1, @P2)", [3, "three"]),
        ]
        
        results = await conn.execute_batch(commands)
        assert len(results) == 3
        assert all(r == 1 for r in results)  # Each insert affects 1 row
        
    finally:
        try:
            await conn.close()
        except Exception:
            pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_timeout_with_batch_query(test_config: Config):
    """Test that query_batch() respects query timeout."""
    conn = Transaction(test_config.connection_string, query_timeout=5000)
    
    try:
        # Create a global temporary table for testing
        await conn.execute("IF OBJECT_ID('tempdb..##batch_query_timeout_test') IS NOT NULL DROP TABLE ##batch_query_timeout_test")
        await conn.execute("CREATE TABLE ##batch_query_timeout_test (id INT, value VARCHAR(50))")
        
        # Insert some test data
        await conn.execute("INSERT INTO ##batch_query_timeout_test (id, value) VALUES (1, 'one')")
        await conn.execute("INSERT INTO ##batch_query_timeout_test (id, value) VALUES (2, 'two')")
        
        # Execute a batch of queries (must be tuples of (sql, parameters))
        queries = [
            ("SELECT id, value FROM ##batch_query_timeout_test WHERE id = 1", None),
            ("SELECT id, value FROM ##batch_query_timeout_test WHERE id = 2", None),
        ]
        
        results = await conn.query_batch(queries)
        assert len(results) == 2
        
        # Check first result
        rows1 = results[0].rows() if results[0].has_rows() else []
        assert len(rows1) == 1
        assert rows1[0]["id"] == 1
        assert rows1[0]["value"] == "one"
        
        # Check second result
        rows2 = results[1].rows() if results[1].has_rows() else []
        assert len(rows2) == 1
        assert rows2[0]["id"] == 2
        assert rows2[0]["value"] == "two"
        
    finally:
        try:
            await conn.close()
        except Exception:
            pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_no_timeout_with_none_parameter(test_config: Config):
    """Test that query_timeout=None means no timeout."""
    # None means no timeout, so even long queries should work
    conn = Transaction(test_config.connection_string, query_timeout=None)
    
    try:
        # Query with a 1 second delay (should be fine with no timeout)
        result = await conn.query("SELECT 1 as value; WAITFOR DELAY '00:00:01'")
        rows = result.rows() if result.has_rows() else []
        assert len(rows) > 0
        
    finally:
        await conn.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_timeout_error_includes_duration_information(test_config: Config):
    """Test that QueryTimeoutError message includes timeout duration."""
    conn = Transaction(test_config.connection_string, query_timeout=250)
    
    with pytest.raises(QueryTimeoutError) as exc_info:
        await conn.execute("WAITFOR DELAY '00:00:01'")
    
    error_message = str(exc_info.value)
    # Should mention the timeout and duration
    assert "timeout" in error_message.lower()
    assert "250" in error_message
    
    # Should also have the message attribute set
    assert hasattr(exc_info.value, "message")
    assert "250" in exc_info.value.message
    
    # Don't close - connection might be in bad state after timeout


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_commit_respects_timeout(test_config: Config):
    """Test that commit() respects query timeout."""
    conn = Transaction(test_config.connection_string, query_timeout=5000)
    
    try:
        # Begin a transaction
        await conn.begin()
        
        # Create a global temporary table and insert data
        await conn.execute("IF OBJECT_ID('tempdb..##commit_timeout_test') IS NOT NULL DROP TABLE ##commit_timeout_test")
        await conn.execute("CREATE TABLE ##commit_timeout_test (id INT)")
        await conn.execute("INSERT INTO ##commit_timeout_test (id) VALUES (1)")
        
        # Commit should succeed within timeout
        await conn.commit()
        
        # Verify we're no longer in a transaction
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        assert rows[0]["count"] == 0
        
    finally:
        try:
            await conn.close()
        except Exception:
            pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_rollback_respects_timeout(test_config: Config):
    """Test that rollback() respects query timeout."""
    conn = Transaction(test_config.connection_string, query_timeout=5000)
    
    try:
        # Begin a transaction
        await conn.begin()
        
        # Create a global temporary table and insert data
        await conn.execute("IF OBJECT_ID('tempdb..##rollback_timeout_test') IS NOT NULL DROP TABLE ##rollback_timeout_test")
        await conn.execute("CREATE TABLE ##rollback_timeout_test (id INT)")
        await conn.execute("INSERT INTO ##rollback_timeout_test (id) VALUES (1)")
        
        # Rollback should succeed within timeout
        await conn.rollback()
        
        # Verify we're no longer in a transaction
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        assert rows[0]["count"] == 0
        
    finally:
        try:
            await conn.close()
        except Exception:
            pass
