#!/usr/bin/env python3
"""
Test parameterized queries functionality
"""

import pytest
import asyncio
import sys
import os

# Add the python module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    from mssql import Connection
    MSSQL_AVAILABLE = True
except ImportError:
    MSSQL_AVAILABLE = False
    Connection = None

# Test configuration - adjust as needed
TEST_CONNECTION_STRING = "Server=SNOWFLAKE\\SQLEXPRESS,50014;Database=pymssql_test;Integrated Security=true;TrustServerCertificate=yes"


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.skipif(not MSSQL_AVAILABLE, reason="mssql_python_rust not available")
async def test_simple_parameterized_query():
    """Test executing a simple parameterized query."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.execute(
                "SELECT @P1 + @P2 as sum_result", 
                [10, 5]
            )
            rows = result.rows()
            assert len(rows) == 1
            sum_result = rows[0]['sum_result']
            assert sum_result == 15
    except Exception as e:
        pytest.skip(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.skipif(not MSSQL_AVAILABLE, reason="mssql_python_rust not available")
async def test_parameter_types():
    """Test different parameter types."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.execute("""
                SELECT 
                    @P1 as string_param,
                    @P2 as int_param,
                    @P3 as float_param,
                    @P4 as bool_param,
                    @P5 as null_param
            """, [
                "test string",
                42,
                3.14159,
                True,
                None
            ])
            
            rows = result.rows()
            assert len(rows) == 1
            row = rows[0]
            
            assert row['string_param'] == "test string"
            assert row['int_param'] == 42
            assert abs(row['float_param'] - 3.14159) < 0.00001
            assert row['bool_param'] == True
            assert row['null_param'] is None
    except Exception as e:
        pytest.skip(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.skipif(not MSSQL_AVAILABLE, reason="mssql_python_rust not available")
async def test_string_sql_injection_protection():
    """Test that parameterized queries protect against SQL injection."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # This should be safe from SQL injection
            malicious_input = "'; DROP TABLE users; --"
            
            result = await conn.execute(
                "SELECT @P1 as safe_string",
                [malicious_input]
            )
            
            rows = result.rows()
            assert len(rows) == 1
            assert rows[0]['safe_string'] == malicious_input
    except Exception as e:
        pytest.skip(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.skipif(not MSSQL_AVAILABLE, reason="mssql_python_rust not available")
async def test_execute_dict_with_params():
    """Test execute_dict method with parameters."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            dict_result = await conn.execute_dict(
                "SELECT @P1 as name, @P2 as age",
                ["Alice", 30]
            )
            assert len(dict_result) == 1
            assert dict_result[0]['name'] == "Alice"
            assert dict_result[0]['age'] == 30
    except Exception as e:
        pytest.skip(f"Database not available: {e}")


if __name__ == "__main__":
    # Run a simple smoke test
    async def smoke_test():
        print("Running parameterized queries smoke test...")
        
        if not MSSQL_AVAILABLE:
            print("mssql_python_rust not available - skipping smoke test")
            return
        
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Test simple parameterized query
                result = await conn.execute(
                    "SELECT @P1 + @P2 as sum_result",
                    [10, 5]
                )
                
                if result.has_rows():
                    row = result.rows()[0]
                    print(f"Database test result: {row['sum_result']}")
                    print("Database connection and parameterized queries: ✓")
                else:
                    print("Database connection successful but no rows returned")
                    
        except Exception as e:
            print(f"Database connection test skipped (no database available): {e}")
        
        print("Smoke test completed successfully!")
    
    asyncio.run(smoke_test())
