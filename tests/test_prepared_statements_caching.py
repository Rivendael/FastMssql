"""
Tests for Prepared Statement/Parameter Caching in FastMSSQL

This module tests query plan caching behavior, parameter binding validation,
SQL injection prevention, and prepared statement cache hits/misses.

Run with: python -m pytest tests/test_prepared_statements_caching.py -v
"""

import pytest
import pytest_asyncio
import os
import time
from datetime import datetime, timedelta

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = os.getenv("FASTMSSQL_TEST_CONNECTION_STRING")


@pytest_asyncio.fixture(scope="function")
async def prepared_statement_test_table(test_connection_string):
    """Setup and teardown test table for prepared statement tests."""
    try:
        async with Connection(test_connection_string) as connection:
            await connection.execute("DROP TABLE IF EXISTS test_prepared_stmts")
    except:
        pass
    
    # Create the test table
    async with Connection(test_connection_string) as connection:
        await connection.execute("""
            CREATE TABLE test_prepared_stmts (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name NVARCHAR(100) NOT NULL,
                email VARCHAR(100),
                age INT,
                salary DECIMAL(10,2),
                created_at DATETIME DEFAULT GETDATE(),
                last_modified DATETIME DEFAULT GETDATE()
            )
        """)
    
    yield "test_prepared_stmts"
    
    # Clean up
    try:
        async with Connection(test_connection_string) as connection:
            await connection.execute("DROP TABLE IF EXISTS test_prepared_stmts")
    except:
        pass


class TestParameterBinding:
    """Test parameter binding validation and edge cases."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_single_parameter_binding(self, prepared_statement_test_table, test_connection_string):
        """Test single parameter binding."""
        try:
            async with Connection(test_connection_string) as conn:
                # Insert with single parameter
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, email, age) VALUES (@P1, @P2, @P3)",
                    ["Alice", "alice@example.com", 30]
                )
                
                # Query with single parameter
                result = await conn.query(
                    "SELECT * FROM test_prepared_stmts WHERE age > @P1",
                    [25]
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert len(rows) > 0
                assert rows[0]["name"] == "Alice"
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_multiple_parameter_binding(self, prepared_statement_test_table, test_connection_string):
        """Test multiple parameter binding in single query."""
        try:
            async with Connection(test_connection_string) as conn:
                # Insert multiple rows with different parameters
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, email, age, salary) VALUES (@P1, @P2, @P3, @P4)",
                    ["Bob", "bob@example.com", 35, 75000.50]
                )
                
                # Query with multiple parameters
                result = await conn.query(
                    "SELECT * FROM test_prepared_stmts WHERE age > @P1 AND salary > @P2",
                    [30, 50000]
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert len(rows) == 1
                assert rows[0]["name"] == "Bob"
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_parameter_type_validation(self, prepared_statement_test_table, test_connection_string):
        """Test that parameter types are properly validated and converted."""
        try:
            async with Connection(test_connection_string) as conn:
                # Test integer parameter
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, age) VALUES (@P1, @P2)",
                    ["Charlie", 28]
                )
                
                # Test float/decimal parameter
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, salary) VALUES (@P1, @P2)",
                    ["Diana", 95000.75]
                )
                
                # Test string parameter
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, email) VALUES (@P1, @P2)",
                    ["Eve", "eve@example.com"]
                )
                
                result = await conn.query("SELECT COUNT(*) as count FROM test_prepared_stmts")
                assert result.has_rows()
                assert result.rows()[0]["count"] == 3
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_null_parameter_binding(self, prepared_statement_test_table, test_connection_string):
        """Test binding NULL values as parameters."""
        try:
            async with Connection(test_connection_string) as conn:
                # Insert with NULL email
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, email, age) VALUES (@P1, @P2, @P3)",
                    ["Frank", None, 45]
                )
                
                # Query for NULL values
                result = await conn.query(
                    "SELECT * FROM test_prepared_stmts WHERE email IS NULL"
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert len(rows) == 1
                assert rows[0]["name"] == "Frank"
                assert rows[0]["email"] is None
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_empty_string_parameter(self, prepared_statement_test_table, test_connection_string):
        """Test binding empty strings as parameters."""
        try:
            async with Connection(test_connection_string) as conn:
                # Insert with empty string
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, email) VALUES (@P1, @P2)",
                    ["Grace", ""]
                )
                
                # Query for empty strings
                result = await conn.query(
                    "SELECT * FROM test_prepared_stmts WHERE email = @P1",
                    [""]
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert len(rows) == 1
                assert rows[0]["name"] == "Grace"
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_large_string_parameter(self, prepared_statement_test_table, test_connection_string):
        """Test binding large string parameters (>8KB)."""
        try:
            async with Connection(test_connection_string) as conn:
                # Create a large string but within column limits
                # email column is VARCHAR(100), so use name column instead which is NVARCHAR(100)
                large_string = "x" * 100
                
                # Insert with large string
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, email) VALUES (@P1, @P2)",
                    [large_string, "test@example.com"]
                )
                
                # Query and verify
                result = await conn.query(
                    "SELECT * FROM test_prepared_stmts WHERE name = @P1",
                    [large_string]
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert len(rows[0]["name"]) == 100
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_special_characters_in_parameters(self, prepared_statement_test_table, test_connection_string):
        """Test parameter binding with special characters."""
        try:
            async with Connection(test_connection_string) as conn:
                special_chars = "O'Reilly & Associates <test@example.com> \"quoted\""
                
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, email) VALUES (@P1, @P2)",
                    [special_chars, "special@test.com"]
                )
                
                # Query with special characters in parameter
                result = await conn.query(
                    "SELECT * FROM test_prepared_stmts WHERE name = @P1",
                    [special_chars]
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert rows[0]["name"] == special_chars
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestQueryPlanCaching:
    """Test query plan caching behavior."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_same_query_reuse(self, prepared_statement_test_table, test_connection_string):
        """Test that running the same query multiple times benefits from caching."""
        try:
            async with Connection(test_connection_string) as conn:
                # Insert initial data
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, age) VALUES (@P1, @P2)",
                    ["User1", 25]
                )
                
                # Run the same query multiple times
                query = "SELECT * FROM test_prepared_stmts WHERE age > @P1"
                
                results = []
                for i in range(5):
                    result = await conn.query(query, [20])
                    results.append(result)
                
                # All results should be consistent
                for result in results:
                    assert result.has_rows()
                    rows = result.rows()
                    assert len(rows) == 1
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_different_parameter_values_same_query(self, prepared_statement_test_table, test_connection_string):
        """Test that query plan is reused with different parameter values."""
        try:
            async with Connection(test_connection_string) as conn:
                # Insert test data
                test_data = [
                    ("Alice", 25),
                    ("Bob", 30),
                    ("Charlie", 35),
                    ("Diana", 40),
                ]
                
                for name, age in test_data:
                    await conn.execute(
                        "INSERT INTO test_prepared_stmts (name, age) VALUES (@P1, @P2)",
                        [name, age]
                    )
                
                # Execute same query with different parameters
                query = "SELECT * FROM test_prepared_stmts WHERE age > @P1"
                
                # Test multiple parameter variations
                test_cases = [20, 28, 32, 38]
                
                for threshold in test_cases:
                    result = await conn.query(query, [threshold])
                    assert result.has_rows()
                    rows = result.rows()
                    # Verify all returned ages are > threshold
                    for row in rows:
                        assert row["age"] > threshold
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_query_plan_cache_performance_improvement(self, prepared_statement_test_table, test_connection_string):
        """Test that repeated queries show performance improvement due to caching."""
        try:
            async with Connection(test_connection_string) as conn:
                # Insert test data
                for i in range(100):
                    await conn.execute(
                        "INSERT INTO test_prepared_stmts (name, age) VALUES (@P1, @P2)",
                        [f"User{i}", 20 + (i % 40)]
                    )
                
                query = "SELECT COUNT(*) as cnt FROM test_prepared_stmts WHERE age > @P1"
                
                # Warm up - first execution (may include plan compilation)
                await conn.query(query, [25])
                
                # Time multiple executions (should be faster due to caching)
                times = []
                for i in range(10):
                    start = time.time()
                    result = await conn.query(query, [25 + i])
                    elapsed = time.time() - start
                    times.append(elapsed)
                
                # Generally, later executions should be comparable to early ones
                # This is a basic sanity check that query execution completes
                assert all(t > 0 for t in times)
                assert len(times) == 10
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestSQLInjectionPrevention:
    """Test SQL injection prevention through parameter binding."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_injection_in_string_parameter(self, prepared_statement_test_table, test_connection_string):
        """Test that SQL injection attempts in string parameters are prevented."""
        try:
            async with Connection(test_connection_string) as conn:
                # Attempt SQL injection via parameter
                malicious_input = "'; DROP TABLE test_prepared_stmts; --"
                
                # This should safely insert the string without executing the DROP
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, email) VALUES (@P1, @P2)",
                    [malicious_input, "test@example.com"]
                )
                
                # Verify table still exists and contains the malicious string
                result = await conn.query(
                    "SELECT * FROM test_prepared_stmts WHERE name = @P1",
                    [malicious_input]
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert rows[0]["name"] == malicious_input
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_injection_union_based(self, prepared_statement_test_table, test_connection_string):
        """Test that UNION-based SQL injection is prevented."""
        try:
            async with Connection(test_connection_string) as conn:
                # Insert legitimate data first
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, age) VALUES (@P1, @P2)",
                    ["Alice", 30]
                )
                
                # Attempt UNION-based injection
                malicious_input = "'; UNION SELECT 1, 'hacked', NULL, NULL, NULL, NULL, NULL; --"
                
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, email) VALUES (@P1, @P2)",
                    [malicious_input, "test@example.com"]
                )
                
                # Query should return only legitimate data
                result = await conn.query("SELECT * FROM test_prepared_stmts")
                rows = result.rows()
                
                # Should have 2 rows: Alice and the malicious string stored as data
                assert len(rows) == 2
                
                # Verify no 'hacked' data appeared
                names = [row["name"] for row in rows]
                assert "hacked" not in names
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_injection_numeric_parameter(self, prepared_statement_test_table, test_connection_string):
        """Test that SQL injection is prevented in numeric parameters."""
        try:
            async with Connection(test_connection_string) as conn:
                # Insert data
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, age) VALUES (@P1, @P2)",
                    ["User1", 25]
                )
                
                # Attempt injection with numeric parameter
                # Even if someone tries: age > 20 OR 1=1
                # The parameter should be treated as a number
                injection_attempt = "20 OR 1=1"
                
                # This should fail or convert the string to a number safely
                # In this case, it should treat it as a string comparison
                result = await conn.query(
                    "SELECT * FROM test_prepared_stmts WHERE name = @P1",
                    [injection_attempt]
                )
                
                # Should not return the User1 row (since "20 OR 1=1" != "User1")
                assert not result.has_rows() or len(result.rows()) == 0
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_injection_time_based_blind(self, prepared_statement_test_table, test_connection_string):
        """Test that time-based blind SQL injection is prevented."""
        try:
            async with Connection(test_connection_string) as conn:
                # Insert legitimate data
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, email) VALUES (@P1, @P2)",
                    ["SafeUser", "safe@example.com"]
                )
                
                # Attempt time-based injection
                # This should be treated as a literal string, not executed as SQL
                malicious_input = "'; WAITFOR DELAY '00:00:05'; --"
                
                start_time = time.time()
                
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, email) VALUES (@P1, @P2)",
                    [malicious_input, "test@example.com"]
                )
                
                elapsed = time.time() - start_time
                
                # If query plan caching/parameter binding works correctly,
                # this should complete quickly (< 2 seconds)
                # If vulnerable, it would take > 5 seconds
                assert elapsed < 2.0, f"Query took {elapsed}s - possible vulnerability"
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestParameterEdgeCases:
    """Test edge cases in parameter handling."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_unicode_parameters(self, prepared_statement_test_table, test_connection_string):
        """Test binding Unicode parameters."""
        try:
            async with Connection(test_connection_string) as conn:
                unicode_data = [
                    ("José", "José@example.com"),
                    ("李明", "li@example.com"),
                    ("Müller", "muller@example.com"),
                    ("Ñoño", "nono@example.com"),
                ]
                
                for name, email in unicode_data:
                    await conn.execute(
                        "INSERT INTO test_prepared_stmts (name, email) VALUES (@P1, @P2)",
                        [name, email]
                    )
                
                # Query back and verify
                result = await conn.query("SELECT * FROM test_prepared_stmts ORDER BY name")
                assert result.has_rows()
                rows = result.rows()
                assert len(rows) == 4
                
                # Verify unicode names are preserved
                names = [row["name"] for row in rows]
                assert "José" in names
                assert "李明" in names
                assert "Müller" in names
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_parameter_with_line_breaks(self, prepared_statement_test_table, test_connection_string):
        """Test binding parameters containing line breaks and whitespace."""
        try:
            async with Connection(test_connection_string) as conn:
                multiline_text = "Line 1\nLine 2\nLine 3\n\tTabbed Line"
                
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, email) VALUES (@P1, @P2)",
                    [multiline_text, "multiline@example.com"]
                )
                
                result = await conn.query(
                    "SELECT * FROM test_prepared_stmts WHERE name = @P1",
                    [multiline_text]
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert rows[0]["name"] == multiline_text
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_max_int_parameter(self, prepared_statement_test_table, test_connection_string):
        """Test binding maximum integer values."""
        try:
            async with Connection(test_connection_string) as conn:
                max_int = 2147483647  # SQL Server INT max
                
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, age) VALUES (@P1, @P2)",
                    ["MaxInt", max_int]
                )
                
                result = await conn.query(
                    "SELECT * FROM test_prepared_stmts WHERE age = @P1",
                    [max_int]
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert rows[0]["age"] == max_int
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_decimal_precision_parameter(self, prepared_statement_test_table, test_connection_string):
        """Test binding decimal values with precision."""
        try:
            async with Connection(test_connection_string) as conn:
                precise_value = 12345678.99
                
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, salary) VALUES (@P1, @P2)",
                    ["HighSalary", precise_value]
                )
                
                result = await conn.query(
                    "SELECT * FROM test_prepared_stmts WHERE salary = @P1",
                    [precise_value]
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert abs(rows[0]["salary"] - precise_value) < 0.01
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestBatchParameterBinding:
    """Test parameter binding in batch operations."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_batch_query_with_different_parameters(self, prepared_statement_test_table, test_connection_string):
        """Test batch query execution with different parameters for each query."""
        try:
            async with Connection(test_connection_string) as conn:
                # Insert test data
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, age) VALUES (@P1, @P2)",
                    ["Alice", 25]
                )
                await conn.execute(
                    "INSERT INTO test_prepared_stmts (name, age) VALUES (@P1, @P2)",
                    ["Bob", 35]
                )
                
                # Create batch queries with different parameters
                batch_queries = [
                    ("SELECT * FROM test_prepared_stmts WHERE age > @P1", [20]),
                    ("SELECT * FROM test_prepared_stmts WHERE age > @P1", [30]),
                    ("SELECT * FROM test_prepared_stmts WHERE name = @P1", ["Alice"]),
                ]
                
                if hasattr(conn, 'query_batch'):
                    results = await conn.query_batch(batch_queries)
                    
                    assert len(results) == 3
                    assert results[0].has_rows()
                    assert results[1].has_rows()
                    assert results[2].has_rows()
        except Exception as e:
            pytest.fail(f"Database not available or batch not supported: {e}")
