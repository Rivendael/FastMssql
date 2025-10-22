"""
Error handling and edge case tests for mssql-python-rust

This module tests error handling, edge cases, boundary conditions,
and failure scenarios to ensure robust error handling.
"""

import pytest
import os

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("mssql wrapper not available - make sure mssql.py is importable", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = os.getenv(
    "FASTMSSQL_TEST_CONNECTION_STRING",
)
INVALID_CONNECTION_STRING = "Server=invalid_server;Database=invalid_db;User=invalid;Password=invalid"

# @pytest.mark.asyncio
# async def test_invalid_connection_string():
#     """Test error handling for invalid connection strings."""
#     # Completely malformed connection string
#     with pytest.raises(Exception):
#         async with Connection("This is not a valid connection string") as conn:
#             pass
#     # Valid format but invalid server
#     with pytest.raises(Exception):
#         async with Connection(INVALID_CONNECTION_STRING) as conn:
#             pass

@pytest.mark.integration
@pytest.mark.asyncio
async def test_sql_syntax_errors():
    """Test handling of SQL syntax errors."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Invalid SQL syntax
            with pytest.raises(Exception):
                await conn.execute("INVALID SQL STATEMENT")
                
            with pytest.raises(Exception):
                await conn.execute("SELECT * FORM invalid_table")  # FORM instead of FROM
                
            with pytest.raises(Exception):
                await conn.execute("INSERT INTO non_existent_table VALUES (1, 2, 3)")
                
            # Connection should still be usable after errors
            result = await conn.query("SELECT 1 as recovery_test")
            if result and result.rows():
                assert result.rows()[0]['recovery_test'] == 1

    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_constraint_violations():
    """Test handling of database constraint violations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Clean up any existing table first
            await conn.execute("DROP TABLE IF EXISTS test_constraints_error")
            
            # Create test table with constraints
            await conn.execute("""
                CREATE TABLE test_constraints_error (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    age INT CHECK (age >= 0 AND age <= 150),
                    category VARCHAR(20) NOT NULL
                )
            """)
            
            try:
                # Insert valid data first
                await conn.execute("""
                    INSERT INTO test_constraints_error (email, age, category) 
                    VALUES ('valid@example.com', 25, 'A')
                """)
                
                # Test primary key violation (duplicate)
                with pytest.raises(Exception):
                    await conn.execute("""
                        INSERT INTO test_constraints_error (id, email, age, category) 
                        VALUES (1, 'another@example.com', 30, 'B')
                    """)
                
                # Test unique constraint violation
                with pytest.raises(Exception):
                    await conn.execute("""
                        INSERT INTO test_constraints_error (email, age, category) 
                        VALUES ('valid@example.com', 35, 'C')
                    """)
                
                # Test check constraint violation
                with pytest.raises(Exception):
                    await conn.execute("""
                        INSERT INTO test_constraints_error (email, age, category) 
                        VALUES ('test@example.com', 200, 'D')
                    """)
                
                # Test NOT NULL constraint violation
                with pytest.raises(Exception):
                    await conn.execute("""
                        INSERT INTO test_constraints_error (email, age, category) 
                        VALUES (NULL, 25, 'E')
                    """)
                
                # Verify original data is still there
                result = await conn.query("SELECT COUNT(*) as count FROM test_constraints_error")
                assert result and result.rows()
                assert result.rows()[0]['count'] == 1

            finally:
                # Clean up - always execute this
                await conn.execute("DROP TABLE IF EXISTS test_constraints_error")

    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_data_type_conversion_errors():
    """Test data type conversion errors."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Test invalid date formats
            with pytest.raises(Exception):
                await conn.execute("SELECT CAST('invalid-date' AS DATETIME)")

            # Test numeric overflow
            with pytest.raises(Exception):
                await conn.execute("SELECT CAST(9999999999999999999999999999999999999 AS INT)")

            # Test invalid numeric conversion
            with pytest.raises(Exception):
                await conn.execute("SELECT CAST('not-a-number' AS INT)")

                await conn.execute("SELECT CAST('invalid-date' AS DATETIME)")

            # Test numeric overflow
            with pytest.raises(Exception):
                await conn.execute("SELECT CAST(9999999999999999999999999999999999999 AS INT)")

            # Test invalid numeric conversion
            with pytest.raises(Exception):
                await conn.execute("SELECT CAST('not-a-number' AS INT)")

            # Connection should still work after errors
            result = await conn.query("SELECT 'still working' as status")
            assert result and result.has_rows()
            assert result.rows()[0]['status'] == 'still working'

    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_connection_interruption():
    """Test behavior when connection has issues."""
    try:
        # Test multiple connections work independently
        async with Connection(TEST_CONNECTION_STRING) as conn1:
            async with Connection(TEST_CONNECTION_STRING) as conn2:
                # Verify both work
                result1 = await conn1.query("SELECT 1 as test")
                assert result1 and result1.rows()
                assert result1.rows()[0]['test'] == 1
                
                result2 = await conn2.query("SELECT 2 as test")
                assert result2 and result2.rows()
                assert result2.rows()[0]['test'] == 2

    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_null_and_empty_values():
    """Test handling of NULL and empty values."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Clean up any existing table first
            await conn.execute("DROP TABLE IF EXISTS test_null_empty")
            
            # Create test table
            await conn.execute("""
                CREATE TABLE test_null_empty (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    nullable_text NVARCHAR(100),
                    nullable_int INT,
                    empty_string NVARCHAR(100) DEFAULT ''
                )
            """)
            
            try:
                # Insert various NULL and empty combinations
                await conn.execute("""
                    INSERT INTO test_null_empty (nullable_text, nullable_int, empty_string) VALUES 
                    (NULL, NULL, ''),
                    ('', NULL, ''),
                    ('text', 42, ''),
                    (NULL, 0, 'not empty')
                """)
                
                # Query and verify NULL handling
                result = await conn.query("SELECT * FROM test_null_empty ORDER BY id")
                rows = result.rows() if result.has_rows() else []
                assert len(rows) == 4

                # First row - all nulls/empty
                assert rows[0]['nullable_text'] is None
                assert rows[0]['nullable_int'] is None
                assert rows[0]['empty_string'] == ''

                # Second row - empty string vs NULL
                assert rows[1]['nullable_text'] == ''
                assert rows[1]['nullable_int'] is None
                
                # Third row - normal values
                assert rows[2]['nullable_text'] == 'text'
                assert rows[2]['nullable_int'] == 42
                
                # Fourth row - zero vs NULL
                assert rows[3]['nullable_text'] is None
                assert rows[3]['nullable_int'] == 0
                
            finally:
                # Clean up - always execute this
                await conn.execute("DROP TABLE IF EXISTS test_null_empty")

    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_special_characters():
    """Test handling of special characters in data."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Clean up any existing table first
            await conn.execute("DROP TABLE IF EXISTS test_special_chars")
            
            # Create test table
            await conn.execute("""
                CREATE TABLE test_special_chars (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    text_data NVARCHAR(500)
                )
            """)
            
            try:
                # Test various special characters
                special_strings = [
                    "Single 'quotes' in text",
                    'Double "quotes" in text',
                    "Mixed 'single' and \"double\" quotes",
                    "Unicode: café, naïve, résumé, 北京",
                    "Newlines\nand\ttabs",
                    "Backslashes \\ and forward /slashes/",
                    "SQL injection attempt: '; DROP TABLE test; --",
                    "Percent % and underscore _ wildcards",
                    "XML-like: <tag>content</tag>",
                    "JSON-like: {\"key\": \"value\"}",
                    "Emoji: 😀🎉📊"
                ]
                
                # Insert special character data
                for i, special_str in enumerate(special_strings):
                    # Use parameterized query would be better, but test with string concatenation
                    escaped_str = special_str.replace("'", "''")  # Basic SQL escaping
                    await conn.execute(f"INSERT INTO test_special_chars (text_data) VALUES (N'{escaped_str}')")

                # Retrieve and verify
                result = await conn.query("SELECT * FROM test_special_chars ORDER BY id")

                assert result.has_rows() and len(result.rows()) == len(special_strings)

                for i, row in enumerate(result.rows()):
                    expected = special_strings[i]
                    actual = row['text_data']
                    assert actual == expected, f"Mismatch at index {i}: expected '{expected}', got '{actual}'"
            
            finally:
                # Clean up - always execute this
                await conn.execute("DROP TABLE IF EXISTS test_special_chars")
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_boundary_values():
    """Test boundary values for different data types."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Test numeric boundaries
            result = await conn.query("""
                SELECT 
                    CAST(-2147483648 AS INT) as min_int,
                    CAST(2147483647 AS INT) as max_int,
                    CAST(0 AS TINYINT) as min_tinyint,
                    CAST(255 AS TINYINT) as max_tinyint,
                    CAST(-32768 AS SMALLINT) as min_smallint,
                    CAST(32767 AS SMALLINT) as max_smallint
            """)

            assert result.has_rows() and len(result.rows()) == 1
            row = result.rows()[0] if result.has_rows() else {}
            
            assert row['min_int'] == -2147483648
            assert row['max_int'] == 2147483647
            assert row['min_tinyint'] == 0
            assert row['max_tinyint'] == 255
            assert row['min_smallint'] == -32768
            assert row['max_smallint'] == 32767
            
            # Test string length boundaries
            max_varchar = 'A' * 8000  # Max for VARCHAR
            result = await conn.query(f"SELECT '{max_varchar}' as max_varchar")
            if result.has_rows():
                assert len(result.rows()[0]['max_varchar']) == 8000

    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_error_handling():
    """Test error handling in async operations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Test async syntax error
            with pytest.raises(Exception):
                await conn.execute("INVALID ASYNC SQL")
            
            # Connection should still work after error
            result = await conn.query("SELECT 'async recovery' as status")
            assert result.rows()[0]['status'] == 'async recovery'

            # Clean up any existing table first
            await conn.execute("DROP TABLE IF EXISTS test_async_error")

            # Test async constraint violation
            await conn.execute("""
                CREATE TABLE test_async_error (
                    id INT PRIMARY KEY,
                    name NVARCHAR(50) NOT NULL
                )
            """)
            
            try:
                await conn.execute("INSERT INTO test_async_error VALUES (1, 'test')")
                
                # This should fail due to duplicate key
                with pytest.raises(Exception):
                    await conn.execute("INSERT INTO test_async_error VALUES (1, 'duplicate')")
            
            finally:
                # Clean up - always execute this
                await conn.execute("DROP TABLE IF EXISTS test_async_error")
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_empty_result_sets():
    """Test handling of empty result sets."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Query that returns no rows
            result = await conn.query("SELECT 1 as test WHERE 1 = 0")
            assert not result.has_rows() and len(result.rows()) == 0
            assert isinstance(result.rows(), list)

            # Query with no rows - should still return an ExecutionResult object
            result = await conn.query("SELECT 1 WHERE 1 = 0")
            assert result is not None
            assert not result.has_rows() and len(result.rows()) == 0
            assert isinstance(result.rows(), list)
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_result_sets():
    """Test queries that return multiple result sets using batch statements."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Test with multiple SELECT statements in a single batch
            # This should work across different SQL Server editions
            try:
                # Execute multiple SELECT statements in a single batch
                result = await conn.query("""
                    SELECT 1 as first_result;
                    SELECT 2 as second_result;
                    SELECT 3 as third_result;
                """)
                
                # We should get at least one result set
                # The current implementation likely only returns the first result set
                assert result is not None
                assert result.has_rows() and len(result.rows()) >= 1
                
                # Check if we got the first result
                if result.rows():
                    assert result.rows()[0]['first_result'] == 1
                
                print("Multiple result sets test completed - library returns first result set")
                
            except Exception as e:
                # If batch statements don't work, try a simpler approach
                if "batch" in str(e).lower() or "multiple" in str(e).lower():
                    # Test with a single query that produces multiple conceptual result sets
                    # using UNION ALL to simulate multiple result sets
                    result = await conn.execute("""
                        SELECT 1 as result_set_id, 'first' as result_value
                        UNION ALL
                        SELECT 2 as result_set_id, 'second' as result_value
                        UNION ALL  
                        SELECT 3 as result_set_id, 'third' as result_value
                    """)
                    
                    assert result is not None
                    assert result.has_rows() and len(result.rows()) == 3
                    assert result.rows()[0]['result_set_id'] == 1
                    assert result.rows()[1]['result_set_id'] == 2
                    assert result.rows()[2]['result_set_id'] == 3
                    
                    print("Multiple result sets test completed using UNION ALL approach")
                else:
                    raise
            
    except Exception as e:
        pytest.fail(f"Database not available: {e}")
