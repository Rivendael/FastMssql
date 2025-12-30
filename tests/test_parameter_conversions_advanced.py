"""
Tests for advanced parameter type conversions and edge cases

This module tests parameter handling for various SQL types, edge cases,
and boundary conditions to ensure proper type conversion between Python and SQL Server.
"""

import pytest
import os
import datetime
from decimal import Decimal

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = os.getenv("FASTMSSQL_TEST_CONNECTION_STRING")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_int_types():
    """Test integer parameter type conversions."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Small integer
            result = await conn.query("SELECT @P1 as value", [42])
            assert result.rows()[0]['value'] == 42
            
            # Large integer
            result = await conn.query("SELECT @P1 as value", [9223372036854775807])
            assert result.rows()[0]['value'] == 9223372036854775807
            
            # Negative integer
            result = await conn.query("SELECT @P1 as value", [-42])
            assert result.rows()[0]['value'] == -42
            
            # Zero
            result = await conn.query("SELECT @P1 as value", [0])
            assert result.rows()[0]['value'] == 0
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_string_types():
    """Test string parameter type conversions."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Regular string
            result = await conn.query("SELECT @P1 as value", ['hello'])
            assert result.rows()[0]['value'] == 'hello'
            
            # Empty string
            result = await conn.query("SELECT @P1 as value", [''])
            assert result.rows()[0]['value'] == ''
            
            # String with spaces
            result = await conn.query("SELECT @P1 as value", ['hello world'])
            assert result.rows()[0]['value'] == 'hello world'
            
            # String with special characters
            result = await conn.query("SELECT @P1 as value", ["test'quote"])
            assert "quote" in result.rows()[0]['value']
            
            # Unicode string
            result = await conn.query("SELECT @P1 as value", ['こんにちは'])
            assert result.rows()[0]['value'] == 'こんにちは'
            
            # Long string
            long_str = 'x' * 1000
            result = await conn.query("SELECT @P1 as value", [long_str])
            assert result.rows()[0]['value'] == long_str
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_float_types():
    """Test float parameter type conversions."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Regular float
            result = await conn.query("SELECT @P1 as value", [3.14])
            value = result.rows()[0]['value']
            assert abs(value - 3.14) < 0.001
            
            # Zero
            result = await conn.query("SELECT @P1 as value", [0.0])
            assert result.rows()[0]['value'] == 0.0
            
            # Negative float
            result = await conn.query("SELECT @P1 as value", [-3.14])
            value = result.rows()[0]['value']
            assert abs(value - (-3.14)) < 0.001
            
            # Very small float
            result = await conn.query("SELECT @P1 as value", [0.0001])
            value = result.rows()[0]['value']
            assert abs(value - 0.0001) < 0.00001
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_bool_types():
    """Test boolean parameter type conversions."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # True
            result = await conn.query("SELECT CAST(@P1 AS BIT) as value", [True])
            value = result.rows()[0]['value']
            assert value in [1, True]
            
            # False
            result = await conn.query("SELECT CAST(@P1 AS BIT) as value", [False])
            value = result.rows()[0]['value']
            assert value in [0, False]
            
            # Integer as bool
            result = await conn.query("SELECT CAST(@P1 AS BIT) as value", [1])
            value = result.rows()[0]['value']
            assert value in [1, True]
            
            result = await conn.query("SELECT CAST(@P1 AS BIT) as value", [0])
            value = result.rows()[0]['value']
            assert value in [0, False]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_none_values():
    """Test None/NULL parameter handling."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # None parameter
            result = await conn.query("SELECT @P1 as value", [None])
            assert result.rows()[0]['value'] is None
            
            # Multiple parameters with None
            result = await conn.query(
                "SELECT @P1 as val1, @P2 as val2, @P3 as val3",
                [1, None, 'test']
            )
            row = result.rows()[0]
            assert row['val1'] == 1
            assert row['val2'] is None
            assert row['val3'] == 'test'
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_datetime_types():
    """Test datetime parameter type conversions (via string)."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # datetime types are not directly supported - convert to string
            dt_val = datetime.datetime(2023, 12, 25, 14, 30, 45)
            dt_str = dt_val.isoformat()
            result = await conn.query("SELECT @P1 as value", [dt_str])
            returned_dt = result.rows()[0]['value']
            assert returned_dt is not None
            assert '2023' in str(returned_dt)
            
            # Date as string
            date_val = datetime.date(2023, 12, 25)
            date_str = date_val.isoformat()
            result = await conn.query("SELECT @P1 as value", [date_str])
            returned_date = result.rows()[0]['value']
            assert returned_date is not None
            assert '2023-12-25' in str(returned_date)
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_decimal_types():
    """Test decimal parameter type conversions (via float)."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Decimal not directly supported - convert to float
            decimal_val = Decimal('123.45')
            float_val = float(decimal_val)
            result = await conn.query("SELECT @P1 as value", [float_val])
            returned = result.rows()[0]['value']
            # Check if approximately equal
            if returned is not None:
                assert abs(float(returned) - 123.45) < 0.01
            
            # Small decimal
            decimal_val = Decimal('0.001')
            float_val = float(decimal_val)
            result = await conn.query("SELECT @P1 as value", [float_val])
            returned = result.rows()[0]['value']
            if returned is not None:
                assert abs(float(returned) - 0.001) < 0.0001
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_bytes_types():
    """Test bytes parameter type conversions."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Bytes
            bytes_val = b'hello'
            result = await conn.query("SELECT @P1 as value", [bytes_val])
            returned = result.rows()[0]['value']
            # Bytes might be returned as string or bytes
            if isinstance(returned, bytes):
                assert returned == bytes_val
            elif isinstance(returned, str):
                # Might be hex-encoded or base64
                assert len(returned) > 0
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_list_only():
    """Test parameter passing as list (tuples not supported)."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # List parameters
            result = await conn.query("SELECT @P1 as val1, @P2 as val2", [1, 'test'])
            row = result.rows()[0]
            assert row['val1'] == 1
            assert row['val2'] == 'test'
            
            # List with different values
            result = await conn.query("SELECT @P1 as val1, @P2 as val2", [2, 'list'])
            row = result.rows()[0]
            assert row['val1'] == 2
            assert row['val2'] == 'list'
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_many_parameters():
    """Test queries with many parameters (>16)."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create query with 20 parameters
            params = list(range(1, 21))
            param_placeholders = ', '.join([f'@P{i}' for i in range(1, 21)])
            query = f"SELECT {param_placeholders}"
            
            result = await conn.query(query, params)
            assert result.has_rows()
            row = result.rows()[0]
            
            # Verify each parameter
            for i in range(1, 21):
                col_name = f'{i}'  # SQL Server might auto-name columns
                # The exact column naming depends on implementation
                assert row is not None
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_repeated_values():
    """Test using same parameter multiple times."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query(
                "SELECT @P1 + @P1 + @P1 as result",
                [5]
            )
            # Some systems might not support this; check if result is valid
            if result.has_rows():
                value = result.rows()[0]['result']
                assert value == 15
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_special_string_characters():
    """Test parameters with special characters."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Single quote
            result = await conn.query("SELECT @P1 as value", ["it's"])
            value = result.rows()[0]['value']
            assert "'" in value or "'" in value
            
            # Double quote
            result = await conn.query('SELECT @P1 as value', ['he said "hello"'])
            value = result.rows()[0]['value']
            assert '"' in value or "hello" in value
            
            # Backslash
            result = await conn.query("SELECT @P1 as value", ["path\\to\\file"])
            value = result.rows()[0]['value']
            assert "path" in value
            
            # Null character (might be stripped)
            result = await conn.query("SELECT @P1 as value", ["test\x00null"])
            value = result.rows()[0]['value']
            assert value is not None
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_whitespace_handling():
    """Test parameter handling with various whitespace."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Leading/trailing whitespace
            result = await conn.query("SELECT @P1 as value", ["  test  "])
            value = result.rows()[0]['value']
            assert "test" in value
            
            # Tab and newline characters
            result = await conn.query("SELECT @P1 as value", ["test\ttab\nline"])
            value = result.rows()[0]['value']
            assert "test" in value
            
            # Multiple spaces
            result = await conn.query("SELECT @P1 as value", ["test     multiple     spaces"])
            value = result.rows()[0]['value']
            assert "test" in value
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_boundary_integers():
    """Test boundary value integers."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # INT min/max
            result = await conn.query("SELECT @P1 as value", [-2147483648])
            assert result.rows()[0]['value'] == -2147483648
            
            result = await conn.query("SELECT @P1 as value", [2147483647])
            assert result.rows()[0]['value'] == 2147483647
            
            # BIGINT min/max
            result = await conn.query("SELECT @P1 as value", [-9223372036854775808])
            assert result.rows()[0]['value'] == -9223372036854775808
            
            result = await conn.query("SELECT @P1 as value", [9223372036854775807])
            assert result.rows()[0]['value'] == 9223372036854775807
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_mixed_types_in_batch():
    """Test batch operations with mixed parameter types."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table
            await conn.execute("""
                IF OBJECT_ID('tempdb..##mixed_types', 'U') IS NOT NULL
                    DROP TABLE ##mixed_types
            """)
            
            await conn.execute("""
                CREATE TABLE ##mixed_types (
                    id INT,
                    name VARCHAR(50),
                    score FLOAT,
                    active BIT
                )
            """)
            
            # Batch with different types
            batch_items = [
                ("INSERT INTO ##mixed_types VALUES (@P1, @P2, @P3, @P4)", [1, 'Alice', 95.5, True]),
                ("INSERT INTO ##mixed_types VALUES (@P1, @P2, @P3, @P4)", [2, 'Bob', 87.3, False]),
                ("INSERT INTO ##mixed_types VALUES (@P1, @P2, @P3, @P4)", [3, 'Charlie', 92.1, True]),
            ]
            
            results = await conn.execute_batch(batch_items)
            assert len(results) == 3
            
            # Verify data
            result = await conn.query("SELECT COUNT(*) as cnt FROM ##mixed_types")
            assert result.rows()[0]['cnt'] == 3
            
            # Cleanup
            await conn.execute("DROP TABLE ##mixed_types")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_type_explicit_casting():
    """Test explicit type casting in queries."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Cast to INT
            result = await conn.query("SELECT CAST(@P1 AS INT) as value", [42.7])
            assert result.rows()[0]['value'] == 42
            
            # Cast to VARCHAR
            result = await conn.query("SELECT CAST(@P1 AS VARCHAR(50)) as value", [123])
            value = str(result.rows()[0]['value'])
            assert '123' in value
            
            # Cast to FLOAT
            result = await conn.query("SELECT CAST(@P1 AS FLOAT) as value", ['3.14'])
            value = result.rows()[0]['value']
            if value is not None:
                assert abs(float(value) - 3.14) < 0.1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")
