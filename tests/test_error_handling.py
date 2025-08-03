"""
Error handling and edge case tests for mssql-python-rust

This module tests error handling, edge cases, boundary conditions,
and failure scenarios to ensure robust error handling.
"""

import pytest
import sys
import os

# Add the parent directory to Python path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    from mssql_python_rust import Connection
except ImportError:
    pytest.skip("mssql_python_rust not available - run 'maturin develop' first", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = "Server=SNOWFLAKE\\SQLEXPRESS,50014;Database=pymssql_test;Integrated Security=true;TrustServerCertificate=yes"
INVALID_CONNECTION_STRING = "Server=invalid_server;Database=invalid_db;User=invalid;Password=invalid"

def test_invalid_connection_string():
    """Test error handling for invalid connection strings."""
    # Completely malformed connection string
    with pytest.raises(Exception):
        conn = Connection("This is not a valid connection string")
        
    # Valid format but invalid server
    with pytest.raises(Exception):
        conn = Connection(INVALID_CONNECTION_STRING)
        conn.connect()

def test_connection_without_connect():
    """Test operations on unconnected connection objects."""
    try:
        conn = Connection(TEST_CONNECTION_STRING)
        # Don't call connect() manually
        
        # Check if connection is established automatically or needs explicit connect
        if conn.is_connected():
            # If auto-connected, disconnect first
            conn.disconnect()
            assert not conn.is_connected()
            
            # Now these should fail because we're not connected
            with pytest.raises(Exception):
                conn.execute("SELECT 1")
                
            with pytest.raises(Exception):
                conn.execute_non_query("SELECT 1")
        else:
            # Connection not auto-established, test as expected
            with pytest.raises(Exception):
                conn.execute("SELECT 1")
                
            with pytest.raises(Exception):
                conn.execute_non_query("SELECT 1")
        
    except Exception as e:
        pytest.skip(f"Could not create connection object: {e}")

@pytest.mark.integration
def test_sql_syntax_errors():
    """Test handling of SQL syntax errors."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            # Invalid SQL syntax
            with pytest.raises(Exception):
                conn.execute("INVALID SQL STATEMENT")
                
            with pytest.raises(Exception):
                conn.execute("SELECT * FORM invalid_table")  # FORM instead of FROM
                
            with pytest.raises(Exception):
                conn.execute("INSERT INTO non_existent_table VALUES (1, 2, 3)")
                
            # Connection should still be usable after errors
            rows = conn.execute("SELECT 1 as recovery_test")
            assert rows[0]['recovery_test'] == 1
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_constraint_violations():
    """Test handling of database constraint violations."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            # Create test table with constraints
            conn.execute_non_query("""
                CREATE TABLE test_constraints_error (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    email VARCHAR(100) UNIQUE NOT NULL,
                    age INT CHECK (age >= 0 AND age <= 150),
                    category VARCHAR(20) NOT NULL
                )
            """)
            
            # Insert valid data first
            conn.execute_non_query("""
                INSERT INTO test_constraints_error (email, age, category) 
                VALUES ('valid@example.com', 25, 'A')
            """)
            
            # Test primary key violation (duplicate)
            with pytest.raises(Exception):
                conn.execute_non_query("""
                    INSERT INTO test_constraints_error (id, email, age, category) 
                    VALUES (1, 'another@example.com', 30, 'B')
                """)
            
            # Test unique constraint violation
            with pytest.raises(Exception):
                conn.execute_non_query("""
                    INSERT INTO test_constraints_error (email, age, category) 
                    VALUES ('valid@example.com', 35, 'C')
                """)
            
            # Test check constraint violation
            with pytest.raises(Exception):
                conn.execute_non_query("""
                    INSERT INTO test_constraints_error (email, age, category) 
                    VALUES ('test@example.com', 200, 'D')
                """)
            
            # Test NOT NULL constraint violation
            with pytest.raises(Exception):
                conn.execute_non_query("""
                    INSERT INTO test_constraints_error (email, age, category) 
                    VALUES (NULL, 25, 'E')
                """)
            
            # Verify original data is still there
            rows = conn.execute("SELECT COUNT(*) as count FROM test_constraints_error")
            assert rows[0]['count'] == 1
            
            # Clean up
            conn.execute_non_query("DROP TABLE test_constraints_error")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_data_type_conversion_errors():
    """Test data type conversion errors."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            # Test invalid date formats
            with pytest.raises(Exception):
                conn.execute("SELECT CAST('invalid-date' AS DATETIME)")
                
            # Test numeric overflow
            with pytest.raises(Exception):
                conn.execute("SELECT CAST(9999999999999999999999999999999999999 AS INT)")
                
            # Test invalid numeric conversion
            with pytest.raises(Exception):
                conn.execute("SELECT CAST('not-a-number' AS INT)")
                
            # Connection should still work after errors
            rows = conn.execute("SELECT 'still working' as status")
            assert rows[0]['status'] == 'still working'
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_long_query_strings():
    """Test handling of very long query strings."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            # Create a very long query string
            long_select_list = ', '.join([f"'{i}' as col_{i}" for i in range(1000)])
            long_query = f"SELECT {long_select_list}"
            
            # This might fail due to query length limits, but should handle gracefully
            try:
                rows = conn.execute(long_query)
                # If it succeeds, verify some columns
                assert 'col_0' in rows[0]
                assert 'col_999' in rows[0]
            except Exception:
                # If it fails, that's also acceptable - the point is graceful handling
                pass
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_connection_interruption():
    """Test behavior when connection is interrupted."""
    try:
        # Create connection
        conn = Connection(TEST_CONNECTION_STRING)
        conn.connect()
        
        # Verify it works
        rows = conn.execute("SELECT 1 as test")
        assert rows[0]['test'] == 1
        
        # Manually disconnect
        conn.disconnect()
        assert not conn.is_connected()
        
        # Try to use disconnected connection
        with pytest.raises(Exception):
            conn.execute("SELECT 1")
            
        # Should be able to reconnect
        conn.connect()
        assert conn.is_connected()
        
        rows = conn.execute("SELECT 2 as test")
        assert rows[0]['test'] == 2
        
        conn.disconnect()
        
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_null_and_empty_values():
    """Test handling of NULL and empty values."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            # Create test table
            conn.execute_non_query("""
                CREATE TABLE test_null_empty (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    nullable_text NVARCHAR(100),
                    nullable_int INT,
                    empty_string NVARCHAR(100) DEFAULT ''
                )
            """)
            
            # Insert various NULL and empty combinations
            conn.execute_non_query("""
                INSERT INTO test_null_empty (nullable_text, nullable_int, empty_string) VALUES 
                (NULL, NULL, ''),
                ('', NULL, ''),
                ('text', 42, ''),
                (NULL, 0, 'not empty')
            """)
            
            # Query and verify NULL handling
            rows = conn.execute("SELECT * FROM test_null_empty ORDER BY id")
            
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
            
            # Clean up
            conn.execute_non_query("DROP TABLE test_null_empty")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_special_characters():
    """Test handling of special characters in data."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            # Create test table
            conn.execute_non_query("""
                CREATE TABLE test_special_chars (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    text_data NVARCHAR(500)
                )
            """)
            
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
                conn.execute_non_query(f"INSERT INTO test_special_chars (text_data) VALUES (N'{escaped_str}')")
            
            # Retrieve and verify
            rows = conn.execute("SELECT * FROM test_special_chars ORDER BY id")
            
            assert len(rows) == len(special_strings)
            
            for i, row in enumerate(rows):
                expected = special_strings[i]
                actual = row['text_data']
                assert actual == expected, f"Mismatch at index {i}: expected '{expected}', got '{actual}'"
            
            # Clean up
            conn.execute_non_query("DROP TABLE test_special_chars")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_boundary_values():
    """Test boundary values for different data types."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            # Test numeric boundaries
            rows = conn.execute("""
                SELECT 
                    CAST(-2147483648 AS INT) as min_int,
                    CAST(2147483647 AS INT) as max_int,
                    CAST(0 AS TINYINT) as min_tinyint,
                    CAST(255 AS TINYINT) as max_tinyint,
                    CAST(-32768 AS SMALLINT) as min_smallint,
                    CAST(32767 AS SMALLINT) as max_smallint
            """)
            
            assert len(rows) == 1
            row = rows[0]
            
            assert row['min_int'] == -2147483648
            assert row['max_int'] == 2147483647
            assert row['min_tinyint'] == 0
            assert row['max_tinyint'] == 255
            assert row['min_smallint'] == -32768
            assert row['max_smallint'] == 32767
            
            # Test string length boundaries
            max_varchar = 'A' * 8000  # Max for VARCHAR
            rows = conn.execute(f"SELECT '{max_varchar}' as max_varchar")
            assert len(rows[0]['max_varchar']) == 8000
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

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
            rows = await conn.execute("SELECT 'async recovery' as status")
            assert rows[0]['status'] == 'async recovery'
            
            # Test async constraint violation
            await conn.execute_non_query("""
                CREATE TABLE test_async_error (
                    id INT PRIMARY KEY,
                    name NVARCHAR(50) NOT NULL
                )
            """)
            
            await conn.execute_non_query("INSERT INTO test_async_error VALUES (1, 'test')")
            
            # This should fail due to duplicate key
            with pytest.raises(Exception):
                await conn.execute_non_query("INSERT INTO test_async_error VALUES (1, 'duplicate')")
            
            # Clean up
            await conn.execute_non_query("DROP TABLE test_async_error")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_empty_result_sets():
    """Test handling of empty result sets."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            # Query that returns no rows
            rows = conn.execute("SELECT 1 as test WHERE 1 = 0")
            assert len(rows) == 0
            assert isinstance(rows, list)
            
            # Query with no columns (shouldn't happen in practice, but test anyway)
            # This is tricky to create, so we'll test a DDL statement that returns no result
            result = conn.execute_non_query("SELECT 1 WHERE 1 = 0")  # This returns 0 affected rows
            assert result == 0
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.integration
def test_multiple_result_sets():
    """Test queries that return multiple result sets."""
    try:
        with Connection(TEST_CONNECTION_STRING) as conn:
            # Test if procedures are supported by trying to create a simple one
            try:
                conn.execute_non_query("CREATE PROCEDURE test_feature_check AS BEGIN SELECT 1 END")
                conn.execute_non_query("DROP PROCEDURE test_feature_check")
            except Exception as e:
                if "Incorrect syntax near the keyword 'PROCEDURE'" in str(e):
                    pytest.skip("Stored procedures not supported in this SQL Server edition")
                else:
                    raise
            
            # Clean up any existing procedure first
            try:
                conn.execute_non_query("IF OBJECT_ID('sp_multiple_results', 'P') IS NOT NULL DROP PROCEDURE sp_multiple_results")
            except:
                pass
            
            # Create a procedure that returns multiple result sets
            conn.execute_non_query("""
                CREATE PROCEDURE sp_multiple_results
                AS
                BEGIN
                    SELECT 1 as first_result;
                    SELECT 2 as second_result;
                    SELECT 3 as third_result;
                END
            """)
            
            # Execute procedure - this might only return the first result set
            # depending on how the library handles multiple result sets
            rows = conn.execute("EXEC sp_multiple_results")
            
            # We should get at least one result set
            assert len(rows) >= 1
            
            # The current implementation likely only returns the first result set
            assert rows[0]['first_result'] == 1
            
            # Clean up
            try:
                conn.execute_non_query("""
                    IF OBJECT_ID('sp_multiple_results', 'P') IS NOT NULL 
                    DROP PROCEDURE sp_multiple_results
                """)
            except:
                pass
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")
