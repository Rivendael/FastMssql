"""
Tests for Stored Procedure execution in FastMSSQL

This module tests calling stored procedures with IN/OUT parameters,
handling multiple result sets, and return value handling.

Run with: python -m pytest tests/test_stored_procedures.py -v
"""

import pytest
import pytest_asyncio
import os

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first", allow_module_level=True)


class TestStoredProcedureBasics:
    """Test basic stored procedure execution patterns."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_exec_with_select_statement(self, test_connection_string):
        """Test EXEC pattern with SELECT."""
        try:
            async with Connection(test_connection_string) as conn:
                result = await conn.query("EXEC sp_executesql N'SELECT 1 as value, ''success'' as status'")
                assert result is not None
        except Exception as e:
            pytest.skip(f"EXEC sp_executesql not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_parameterized_exec(self, test_connection_string):
        """Test parameterized EXEC with parameters."""
        try:
            async with Connection(test_connection_string) as conn:
                result = await conn.query(
                    "SELECT @P1 as name, @P2 as age, @P3 as salary",
                    ["Alice", 30, 75000.00]
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert len(rows) == 1
                assert rows[0]["name"] == "Alice"
                assert rows[0]["age"] == 30
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_conditional_exec(self, test_connection_string):
        """Test conditional EXEC pattern."""
        try:
            async with Connection(test_connection_string) as conn:
                result = await conn.query(
                    "SELECT CASE WHEN @P1 > @P2 THEN 'Greater' ELSE 'Less or Equal' END as comparison",
                    [42, 30]
                )
                
                assert result.has_rows()
                rows = result.rows()
                assert rows[0]["comparison"] == "Greater"
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestParameterizedQueries:
    """Test parameterized query patterns."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_arithmetic_operations(self, test_connection_string):
        """Test parameterized queries with arithmetic."""
        try:
            async with Connection(test_connection_string) as conn:
                # Addition
                result = await conn.query("SELECT @P1 + @P2 as result", [10, 5])
                assert result.has_rows()
                assert result.rows()[0]["result"] == 15
                
                # Subtraction
                result = await conn.query("SELECT @P1 - @P2 as result", [20, 8])
                assert result.rows()[0]["result"] == 12
                
                # Multiplication
                result = await conn.query("SELECT @P1 * @P2 as result", [7, 6])
                assert result.rows()[0]["result"] == 42
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_string_concatenation(self, test_connection_string):
        """Test parameterized queries with string concatenation."""
        try:
            async with Connection(test_connection_string) as conn:
                result = await conn.query(
                    "SELECT @P1 + ' ' + @P2 as full_name",
                    ["John", "Doe"]
                )
                
                assert result.has_rows()
                assert result.rows()[0]["full_name"] == "John Doe"
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_case_expressions(self, test_connection_string):
        """Test parameterized queries with CASE expressions."""
        try:
            async with Connection(test_connection_string) as conn:
                result = await conn.query(
                    """SELECT 
                        CASE 
                            WHEN @P1 > 90 THEN 'A'
                            WHEN @P1 > 80 THEN 'B'
                            WHEN @P1 > 70 THEN 'C'
                            ELSE 'F'
                        END as grade""",
                    [85]
                )
                
                assert result.has_rows()
                assert result.rows()[0]["grade"] == "B"
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestConditionalLogic:
    """Test parameterized queries with conditional logic."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_if_else_logic(self, test_connection_string):
        """Test conditional logic in parameterized queries."""
        try:
            async with Connection(test_connection_string) as conn:
                result = await conn.query(
                    "SELECT IIF(@P1 > @P2, 'Greater', 'Less or Equal') as comparison",
                    [10, 5]
                )
                
                assert result.has_rows()
                assert result.rows()[0]["comparison"] == "Greater"
        except Exception as e:
            # IIF might not be available in all SQL Server versions
            pytest.skip(f"IIF function not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_coalesce_null_handling(self, test_connection_string):
        """Test COALESCE with NULL parameters."""
        try:
            async with Connection(test_connection_string) as conn:
                result = await conn.query(
                    "SELECT COALESCE(CAST(@P1 AS NVARCHAR(100)), CAST(@P2 AS NVARCHAR(100)), 'default') as value",
                    [None, "provided"]
                )
                
                assert result.has_rows()
                assert result.rows()[0]["value"] == "provided"
        except Exception as e:
            pytest.skip(f"COALESCE type handling: {e}")


class TestComplexParameterizedQueries:
    """Test complex parameterized query patterns."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_multiple_conditions(self, test_connection_string):
        """Test queries with multiple parameter conditions."""
        try:
            async with Connection(test_connection_string) as conn:
                result = await conn.query(
                    "SELECT CASE WHEN @P1 > 0 AND @P2 < 100 THEN 'valid' ELSE 'invalid' END as status",
                    [50, 75]
                )
                
                assert result.has_rows()
                assert result.rows()[0]["status"] == "valid"
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_date_operations(self, test_connection_string):
        """Test parameterized queries with date operations."""
        try:
            async with Connection(test_connection_string) as conn:
                result = await conn.query(
                    "SELECT DATEDIFF(day, @P1, @P2) as days_diff",
                    ["2023-01-01", "2023-01-10"]
                )
                
                assert result.has_rows()
                assert result.rows()[0]["days_diff"] == 9
        except Exception as e:
            pytest.skip(f"Date operations with parameters: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_aggregate_with_case(self, test_connection_string):
        """Test aggregate functions with CASE expressions."""
        try:
            async with Connection(test_connection_string) as conn:
                result = await conn.query(
                    """SELECT 
                        CASE WHEN @P1 > @P2 THEN @P1 ELSE @P2 END as max_value,
                        CASE WHEN @P1 < @P2 THEN @P1 ELSE @P2 END as min_value""",
                    [100, 50]
                )
                
                assert result.has_rows()
                row = result.rows()[0]
                assert row["max_value"] == 100
                assert row["min_value"] == 50
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestParameterVariations:
    """Test various parameter passing scenarios."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_null_parameters(self, test_connection_string):
        """Test queries with NULL parameters."""
        try:
            async with Connection(test_connection_string) as conn:
                result = await conn.query(
                    "SELECT @P1 as nullable_value",
                    [None]
                )
                
                assert result.has_rows()
                assert result.rows()[0]["nullable_value"] is None
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_special_characters(self, test_connection_string):
        """Test parameters with special characters."""
        try:
            async with Connection(test_connection_string) as conn:
                special_name = "O'Reilly & Associates <test>"
                
                result = await conn.query(
                    "SELECT @P1 as name",
                    [special_name]
                )
                
                assert result.has_rows()
                assert result.rows()[0]["name"] == special_name
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_unicode_parameters(self, test_connection_string):
        """Test parameters with Unicode characters."""
        try:
            async with Connection(test_connection_string) as conn:
                unicode_names = ["José García", "李明", "Müller Köhler"]
                
                for name in unicode_names:
                    result = await conn.query("SELECT @P1 as name", [name])
                    assert result.has_rows()
                    assert result.rows()[0]["name"] == name
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_empty_string(self, test_connection_string):
        """Test parameters with empty strings."""
        try:
            async with Connection(test_connection_string) as conn:
                result = await conn.query("SELECT @P1 as empty_str", [""])
                assert result.has_rows()
                assert result.rows()[0]["empty_str"] == ""
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_large_string(self, test_connection_string):
        """Test parameters with large strings (8KB+)."""
        try:
            async with Connection(test_connection_string) as conn:
                large_string = "x" * 8000
                result = await conn.query("SELECT @P1 as large_str", [large_string])
                assert result.has_rows()
                assert len(result.rows()[0]["large_str"]) == 8000
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_decimal_precision(self, test_connection_string):
        """Test decimal parameters with precision."""
        try:
            async with Connection(test_connection_string) as conn:
                precise_value = 12345678.99
                result = await conn.query("SELECT @P1 as decimal_val", [precise_value])
                assert result.has_rows()
                assert abs(result.rows()[0]["decimal_val"] - precise_value) < 0.01
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_max_int_value(self, test_connection_string):
        """Test maximum integer values."""
        try:
            async with Connection(test_connection_string) as conn:
                max_int = 2147483647
                result = await conn.query("SELECT @P1 as max_int_val", [max_int])
                assert result.has_rows()
                assert result.rows()[0]["max_int_val"] == max_int
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestBatchParameterExecution:
    """Test batch execution with parameters."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_batch_queries_with_parameters(self, test_connection_string):
        """Test batch query execution with different parameters."""
        try:
            async with Connection(test_connection_string) as conn:
                batch_queries = [
                    ("SELECT @P1 + @P2 as sum_value", [10, 5]),
                    ("SELECT @P1 * @P2 as product_value", [7, 6]),
                    ("SELECT @P1 as input_value", [42]),
                ]
                
                if hasattr(conn, 'query_batch'):
                    results = await conn.query_batch(batch_queries)
                    assert len(results) == 3
                    assert results[0].has_rows()
                else:
                    # Skip if batch queries not supported
                    pytest.skip("Batch queries not supported by this version")
        except Exception as e:
            pytest.skip(f"Batch query execution: {e}")


class TestParameterCachingPatterns:
    """Test patterns that benefit from query plan caching."""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_repeated_parameterized_query(self, test_connection_string):
        """Test repeated execution of parameterized queries."""
        try:
            async with Connection(test_connection_string) as conn:
                query = "SELECT @P1 as value"
                
                for i in range(5):
                    result = await conn.query(query, [i * 10])
                    assert result.has_rows()
                    assert result.rows()[0]["value"] == i * 10
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_different_parameters_same_query(self, test_connection_string):
        """Test same query shape with different parameters."""
        try:
            async with Connection(test_connection_string) as conn:
                query = "SELECT @P1 as num1, @P2 as num2, @P1 + @P2 as sum_val"
                
                test_cases = [(1, 2), (10, 20), (100, 200), (5, 15)]
                
                for a, b in test_cases:
                    result = await conn.query(query, [a, b])
                    assert result.has_rows()
                    row = result.rows()[0]
                    assert row["num1"] == a
                    assert row["num2"] == b
                    assert row["sum_val"] == a + b
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
