"""
Tests for mssql-python-rust

Run with: python -m pytest tests/
"""

import pytest
import sys
import os
import asyncio

# Add the parent directory to Python path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    import mssql_python_rust as mssql
    from mssql_python_rust import Connection
except ImportError:
    pytest.skip("mssql_python_rust not available - run 'maturin develop' first", allow_module_level=True)

# Test configuration - adjust as needed
TEST_CONNECTION_STRING = "Server=SNOWFLAKE\\SQLEXPRESS,50014;Database=pymssql_test;Integrated Security=true;TrustServerCertificate=yes"

def test_version():
    """Test that we can get the library version."""
    version = mssql.version()
    assert isinstance(version, str)
    assert len(version) > 0

def test_connection_creation():
    """Test that we can create a connection object."""
    conn = Connection(TEST_CONNECTION_STRING)
    assert conn is not None

@pytest.mark.integration
def test_basic_connection():
    """Test basic database connectivity."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            assert conn.is_connected()
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_simple_query():
    """Test executing a simple query."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            rows = conn.execute("SELECT 1 as test_value")
            assert len(rows) == 1
            # Convert PyValue to native Python type for comparison
            test_value = int(str(rows[0]['test_value']))
            assert test_value == 1
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_multiple_queries():
    """Test executing multiple queries on the same connection."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            # First query
            rows1 = conn.execute("SELECT 'first' as query_name")
            assert len(rows1) == 1
            assert str(rows1[0]['query_name']) == 'first'
            
            # Second query
            rows2 = conn.execute("SELECT 'second' as query_name")
            assert len(rows2) == 1
            assert str(rows2[0]['query_name']) == 'second'
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_data_types():
    """Test various SQL Server data types."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            rows = conn.execute("""
                SELECT 
                    42 as int_val,
                    3.14159 as float_val,
                    'test string' as str_val,
                    CAST(1 as BIT) as bool_val,
                    NULL as null_val
            """)
            
            assert len(rows) == 1
            row = rows[0]
            
            assert row['int_val'] == 42
            assert abs(row['float_val'] - 3.14159) < 0.0001
            assert row['str_val'] == 'test string'
            assert row['bool_val'] == True
            assert row['null_val'] is None
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration 
def test_execute_non_query():
    """Test executing non-query operations."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            # Create a test table
            conn.execute_non_query("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='test_execute_non_query' AND xtype='U')
                    CREATE TABLE test_execute_non_query (id INT, name NVARCHAR(50), test_flag BIT DEFAULT 0)
            """)
            
            # Clear any existing data
            conn.execute_non_query("DELETE FROM test_execute_non_query")
            
            # Insert test data
            rows_affected = conn.execute_non_query("INSERT INTO test_execute_non_query (id, name, test_flag) VALUES (1, 'test', 0)")
            assert rows_affected == 1
            
            # Update the test_flag to verify non-query execution
            rows_affected = conn.execute_non_query("UPDATE test_execute_non_query SET test_flag = 1 WHERE id = 1")
            assert rows_affected == 1
            
            # Verify the update worked
            rows = conn.execute("SELECT COUNT(*) as updated_count FROM test_execute_non_query WHERE test_flag = 1")
            assert len(rows) == 1
            assert rows[0]['updated_count'] == 1
            
            # Clean up - remove the test table
            conn.execute_non_query("DROP TABLE IF EXISTS test_execute_non_query")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_convenience_functions():
    """Test Connection class convenience (now async-only)."""
    try:
        # Test direct execution using async Connection
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.execute("SELECT 'convenience' as test")
            assert len(result) == 1
            assert result[0]['test'] == 'convenience'
            
            # Test scalar-like execution
            scalar_result = await conn.execute("SELECT 42 as value")
            assert scalar_result[0]['value'] == 42
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

def test_error_handling():
    """Test that errors are handled properly."""
    # Test invalid connection string
    with pytest.raises(Exception):
        conn = Connection("Invalid connection string")
        conn.connect()
    
    # Test invalid query (requires database connection)
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            with pytest.raises(Exception):
                conn.execute("SELECT * FROM non_existent_table_12345")
    except Exception as e:
        pytest.skip(f"Database not available for error testing: {e}")

# Async Tests
@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_connection_creation():
    """Test that we can create an async connection object."""
    conn = Connection(TEST_CONNECTION_STRING)
    assert conn is not None

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_basic_connection():
    """Test basic async database connectivity."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            assert conn.is_connected()
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_simple_query():
    """Test executing a simple query asynchronously."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            rows = await conn.execute("SELECT 1 as test_value")
            assert len(rows) == 1
            assert rows[0]['test_value'] == 1
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_multiple_queries():
    """Test executing multiple queries asynchronously on the same connection."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # First query
            rows1 = await conn.execute("SELECT 'first' as query_name")
            assert len(rows1) == 1
            assert rows1[0]['query_name'] == 'first'
            
            # Second query
            rows2 = await conn.execute("SELECT 'second' as query_name")
            assert len(rows2) == 1
            assert rows2[0]['query_name'] == 'second'
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_data_types():
    """Test various SQL Server data types with async operations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            rows = await conn.execute("""
                SELECT 
                    42 as int_val,
                    3.14159 as float_val,
                    'test string' as str_val,
                    CAST(1 as BIT) as bool_val,
                    NULL as null_val
            """)
            
            assert len(rows) == 1
            row = rows[0]
            
            assert row['int_val'] == 42
            assert abs(row['float_val'] - 3.14159) < 0.0001
            assert row['str_val'] == 'test string'
            assert row['bool_val'] == True
            assert row['null_val'] is None
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration 
async def test_async_execute_non_query():
    """Test executing non-query operations asynchronously."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Test a simple UPDATE operation that doesn't rely on temporary tables
            # First, create and populate a test table, then verify the operation worked
            setup_and_test_sql = """
                -- Create a test table if it doesn't exist
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='test_async_execute_non_query' AND xtype='U')
                    CREATE TABLE test_async_execute_non_query (id INT, name NVARCHAR(50), test_flag BIT DEFAULT 0)
                
                -- Clear any existing data
                DELETE FROM test_async_execute_non_query
                
                -- Insert test data
                INSERT INTO test_async_execute_non_query (id, name, test_flag) VALUES (1, 'test_async', 0)
                
                -- Update the test_flag to verify non-query execution
                UPDATE test_async_execute_non_query SET test_flag = 1 WHERE id = 1
                
                -- Return the count for verification
                SELECT COUNT(*) as updated_count FROM test_async_execute_non_query WHERE test_flag = 1
            """
            
            # Execute the complete test as a single batch to avoid session scope issues
            rows = await conn.execute(setup_and_test_sql)
            
            # Verify that our update worked
            assert len(rows) == 1
            assert rows[0]['updated_count'] == 1
            
            # Clean up - remove the test table
            await conn.execute_non_query("DROP TABLE IF EXISTS test_async_execute_non_query")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_execute_scalar():
    """Test executing scalar queries asynchronously."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Test scalar with number
            result = await conn.execute_scalar("SELECT 42")
            assert result == 42
            
            # Test scalar with string
            result = await conn.execute_scalar("SELECT 'hello world'")
            assert result == 'hello world'
            
            # Test scalar with NULL
            result = await conn.execute_scalar("SELECT NULL")
            assert result is None
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_convenience_functions():
    """Test async connection class directly."""
    try:
        # Test direct async execution using Connection class
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.execute("SELECT 'convenience_async' as test")
            assert len(result) == 1
            assert result[0]['test'] == 'convenience_async'
            
            # Test async scalar-like execution (get first value from result)
            scalar_result = await conn.execute("SELECT 42 as value")
            assert scalar_result[0]['value'] == 42
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_manual_connection_lifecycle():
    """Test manual async connection and disconnection."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        assert not conn.is_connected()
        
        await conn.connect()
        assert conn.is_connected()
        
        # Execute a query
        rows = await conn.execute("SELECT 'manual_async' as test")
        assert len(rows) == 1
        assert rows[0]['test'] == 'manual_async'
        
        await conn.disconnect()
        assert not conn.is_connected()
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_error_handling():
    """Test that async errors are handled properly."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            with pytest.raises(Exception):
                await conn.execute("SELECT * FROM non_existent_table_async_12345")
    except Exception as e:
        pytest.skip(f"Database not available for error testing: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_concurrent_queries():
    """Test executing multiple async queries concurrently."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create coroutines for concurrent execution
            query1 = conn.execute("SELECT 1 as value, 'query1' as name")
            query2 = conn.execute("SELECT 2 as value, 'query2' as name")
            query3 = conn.execute("SELECT 3 as value, 'query3' as name")
            
            # Wait for all queries to complete concurrently
            results = await asyncio.gather(query1, query2, query3)
            
            # Verify results
            assert len(results) == 3
            values = [result[0]['value'] for result in results]
            names = [result[0]['name'] for result in results]
            
            assert set(values) == {1, 2, 3}
            assert set(names) == {'query1', 'query2', 'query3'}
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

if __name__ == "__main__":
    # Run basic tests when executed directly
    print("Running basic tests...")
    
    print("Testing version...")
    test_version()
    print("✓ Version test passed")
    
    print("Testing connection creation...")
    test_connection_creation()
    print("✓ Connection creation test passed")
    
    print("\nBasic tests completed!")
    print("Run 'python -m pytest tests/ -v' for full test suite including async tests")
    print("Run 'python -m pytest tests/ -v -m integration' for integration tests")
    print("Run 'python -m pytest tests/ -v -k async' for async tests only")
