"""
Tests for mssql-python-rust

Run with: python -m pytest tests/
"""

import pytest
import os
import asyncio

try:
    import fastmssql
    from fastmssql import Connection
except ImportError:
    pytest.fail("mssql wrapper not available - make sure mssql.py is importable", allow_module_level=True)

# Test configuration - adjust as needed
TEST_CONNECTION_STRING = os.getenv(
    "FASTMSSQL_TEST_CONNECTION_STRING",
)

async def test_error_handling():
    """Test that errors are handled properly."""
    try:
        conn = Connection("Invalid connection string")
        async with conn:
            await conn.query("SELECT 1")
    except Exception:
        pass
    
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            with pytest.raises(Exception):
                await conn.execute("SELECT * FROM non_existent_table_12345")
    except Exception as e:
        pytest.fail(f"Database not available for error testing: {e}")

def test_version():
    """Test that we can get the library version."""
    version = fastmssql.version()
    assert isinstance(version, str)
    assert len(version) > 0

def test_connection_creation():
    """Test that we can create a connection object."""
    conn = Connection(TEST_CONNECTION_STRING)
    assert conn is not None

@pytest.mark.integration
@pytest.mark.asyncio
async def test_basic_connection():
    """Test basic database connectivity."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            assert await conn.is_connected()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query():
    """Test executing a simple query."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 1 as test_value")
            rows = result.rows() if result.has_rows() else []
            assert len(rows) == 1
            test_value = rows[0]['test_value']
            assert test_value == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_queries():
    """Test executing multiple queries on the same connection."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # First query
            result1 = await conn.query("SELECT 'first' as query_name")
            rows1 = result1.rows()
            assert len(rows1) == 1
            assert rows1[0]['query_name'] == 'first'

            # Second query
            result2 = await conn.query("SELECT 'second' as query_name")
            rows2 = result2.rows()
            assert len(rows2) == 1
            assert rows2[0]['query_name'] == 'second'
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_data_types():
    """Test various SQL Server data types."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    42 as int_val,
                    3.14159 as float_val,
                    'test string' as str_val,
                    CAST(1 as BIT) as bool_val,
                    NULL as null_val
            """)
            
            rows = result.rows() if result.has_rows() else []
            assert len(rows) == 1
            row = rows[0]
            
            assert row['int_val'] == 42
            assert abs(row['float_val'] - 3.14159) < 0.0001
            assert row['str_val'] == 'test string'
            assert row['bool_val'] == True
            assert row['null_val'] is None
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_convenience_functions():
    """Test Connection class convenience (now async-only)."""
    try:
        # Test direct execution using async Connection
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 'convenience' as test")
            rows = result.rows() if result.has_rows() else []
            assert len(rows) == 1
            assert rows[0]['test'] == 'convenience'
            
            # Test scalar-like execution
            scalar_result = await conn.query("SELECT 42 as value")
            scalar_rows = scalar_result.rows()
            assert scalar_rows[0]['value'] == 42
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_error_handling():
    """Test that errors are handled properly."""
    # Test invalid connection string
    try:
        conn = Connection("Invalid connection string")
        async with conn:
            # This should fail when we try to use the connection
            await conn.query("SELECT 1")
    except Exception:
        pass  # Expected to fail
    
    # Test invalid query (requires database connection)
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            with pytest.raises(Exception):
                await conn.execute("SELECT * FROM non_existent_table_12345")
    except Exception as e:
        pytest.fail(f"Database not available for error testing: {e}")

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
            assert await conn.is_connected()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_simple_query():
    """Test executing a simple query asynchronously."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 1 as test_value")
            rows = result.rows() if result.has_rows() else []
            assert len(rows) == 1
            assert rows[0]['test_value'] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_multiple_queries():
    """Test executing multiple queries asynchronously on the same connection."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # First query
            result1 = await conn.query("SELECT 'first' as query_name")
            rows1 = result1.rows()
            assert len(rows1) == 1
            assert rows1[0]['query_name'] == 'first'
            
            # Second query
            result2 = await conn.query("SELECT 'second' as query_name")
            rows2 = result2.rows()
            assert len(rows2) == 1
            assert rows2[0]['query_name'] == 'second'
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_data_types():
    """Test various SQL Server data types with async operations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    42 as int_val,
                    3.14159 as float_val,
                    'test string' as str_val,
                    CAST(1 as BIT) as bool_val,
                    NULL as null_val
            """)
            
            rows = result.rows() if result.has_rows() else []
            assert len(rows) == 1
            row = rows[0]
            
            assert row['int_val'] == 42
            assert abs(row['float_val'] - 3.14159) < 0.0001
            assert row['str_val'] == 'test string'
            assert row['bool_val'] == True
            assert row['null_val'] is None
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

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
            result = await conn.query(setup_and_test_sql)
            
            # Verify that our update worked
            rows = result.rows() if result.has_rows() else []
            assert len(rows) == 1
            assert rows[0]['updated_count'] == 1
            
            # Clean up - remove the test table
            await conn.execute("DROP TABLE IF EXISTS test_async_execute_non_query")
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_execute_scalar():
    """Test executing scalar queries asynchronously."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Test scalar with number
            result = await conn.query("SELECT 42 as scalar_value")
            rows = result.rows() if result.has_rows() else []
            assert rows[0]['scalar_value'] == 42
            
            # Test scalar with string
            result = await conn.query("SELECT 'hello world' as scalar_value")
            rows = result.rows() if result.has_rows() else []
            assert rows[0]['scalar_value'] == 'hello world'
            
            # Test scalar with NULL
            result = await conn.query("SELECT NULL as scalar_value")
            rows = result.rows() if result.has_rows() else []
            assert rows[0]['scalar_value'] is None
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_convenience_functions():
    """Test async connection class directly."""
    try:
        # Test direct async execution using Connection class
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 'convenience_async' as test")
            rows = result.rows() if result.has_rows() else []
            assert len(rows) == 1
            assert rows[0]['test'] == 'convenience_async'
            
            # Test async scalar-like execution (get first value from result)
            scalar_result = await conn.query("SELECT 42 as value")
            scalar_rows = scalar_result.rows()
            assert scalar_rows[0]['value'] == 42
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_error_handling():
    """Test that async errors are handled properly."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            with pytest.raises(Exception):
                await conn.execute("SELECT * FROM non_existent_table_async_12345")
    except Exception as e:
        pytest.fail(f"Database not available for error testing: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_simple_parameterized_query():
    """Test basic parameterized queries with list parameters."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query(
                "SELECT @P1 as param1, @P2 as param2, @P3 as param3",
                [42, "test", True]
            )
            
            rows = result.rows() if result.has_rows() else []
            assert len(rows) == 1
            row = rows[0]
            
            assert row['param1'] == 42
            assert row['param2'] == "test"
            assert row['param3'] == True
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_parameters_object_basic():
    """Test using Parameters object instead of simple list."""
    try:
        from fastmssql import Parameters
        
        async with Connection(TEST_CONNECTION_STRING) as conn:
            params = Parameters(100, "Parameters Object", 3.14)
            
            result = await conn.query(
                "SELECT @P1 as id, @P2 as description, @P3 as value",
                params
            )
            
            rows = result.rows() if result.has_rows() else []
            assert len(rows) == 1
            row = rows[0]
            
            assert row['id'] == 100
            assert row['description'] == "Parameters Object"
            assert abs(row['value'] - 3.14) < 0.001
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_parameters_method_chaining():
    """Test Parameters object with method chaining."""
    try:
        from fastmssql import Parameters
        
        async with Connection(TEST_CONNECTION_STRING) as conn:
            params = (Parameters()
                     .add(200)
                     .add("Chained"))
            
            result = await conn.query(
                "SELECT @P1 as chained_id, @P2 as chained_name",
                params
            )
            
            rows = result.rows() if result.has_rows() else []
            assert len(rows) == 1
            row = rows[0]
            
            assert row['chained_id'] == 200
            assert row['chained_name'] == "Chained"
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_concurrent_queries():
    """Test executing multiple async queries concurrently."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create coroutines for concurrent execution
            query1 = conn.query("SELECT 1 as value, 'query1' as name")
            query2 = conn.query("SELECT 2 as value, 'query2' as name")
            query3 = conn.query("SELECT 3 as value, 'query3' as name")

            # Wait for all queries to complete concurrently
            results = await asyncio.gather(query1, query2, query3)
            
            # Verify results
            assert len(results) == 3
            values = [(result.rows()[0] if result.has_rows() else {}).get('value') for result in results]
            names = [(result.rows()[0] if result.has_rows() else {}).get('name') for result in results]

            assert set(values) == {1, 2, 3}
            assert set(names) == {'query1', 'query2', 'query3'}
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

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
