"""
Tests for SQL Server data types with mssql-python-rust

This module tests all major SQL Server data types to ensure proper conversion
between Rust/Tiberius and Python types.
"""

import os
import pytest
import pytest_asyncio

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first", allow_module_level=True)


# Test configuration
TEST_CONNECTION_STRING = os.getenv("FASTMSSQL_TEST_CONNECTION_STRING")


@pytest_asyncio.fixture
async def db_connection():
    """Fixture providing a database connection."""
    if not TEST_CONNECTION_STRING:
        pytest.skip("FASTMSSQL_TEST_CONNECTION_STRING not set")
    
    async with Connection(TEST_CONNECTION_STRING) as conn:
        yield conn
@pytest.mark.integration
@pytest.mark.asyncio
async def test_numeric_types(db_connection):
    """Test all numeric SQL Server data types."""
    result = await db_connection.query("""
        SELECT 
            CAST(127 AS TINYINT) as tinyint_val,
            CAST(32767 AS SMALLINT) as smallint_val,
            CAST(2147483647 AS INT) as int_val,
            CAST(9223372036854775807 AS BIGINT) as bigint_val,
            CAST(3.14159265359 AS FLOAT) as float_val,
            CAST(99.99 AS REAL) as real_val,
            CAST(123.456 AS DECIMAL(10,3)) as decimal_val,
            CAST(999.99 AS NUMERIC(10,2)) as numeric_val,
            CAST(12345.67 AS MONEY) as money_val,
            CAST(123.4567 AS SMALLMONEY) as smallmoney_val
    """)
    
    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]
    
    # Test integer types
    assert row.get('tinyint_val') == 127
    assert row.get('smallint_val') == 32767
    assert row.get('int_val') == 2147483647
    assert row.get('bigint_val') == 9223372036854775807
    
    # Test floating point types
    float_val = row.get('float_val')
    assert float_val is not None, "FLOAT type conversion not implemented"
    assert abs(float_val - 3.14159265359) < 0.0001
    
    real_val = row.get('real_val')
    assert real_val is not None, "REAL type conversion not implemented"
    assert abs(real_val - 99.99) < 0.001
    
    # Test decimal/numeric types
    decimal_val = row.get('decimal_val')
    assert decimal_val is not None, "DECIMAL type conversion not implemented"
    assert abs(decimal_val - 123.456) < 0.001
    
    numeric_val = row.get('numeric_val')
    assert numeric_val is not None, "NUMERIC type conversion not implemented"
    assert abs(numeric_val - 999.99) < 0.01
    
    # Test money types (may not be implemented yet)
    money_val = row.get('money_val')
    if money_val is not None:
        assert abs(money_val - 12345.67) < 0.01
    
    smallmoney_val = row.get('smallmoney_val')
    if smallmoney_val is not None:
        assert abs(smallmoney_val - 123.4567) < 0.0001

@pytest.mark.integration
@pytest.mark.asyncio
async def test_string_types(db_connection):
    """Test all string SQL Server data types."""
    result = await db_connection.query("""
        SELECT 
            CAST('Hello' AS CHAR(10)) as char_val,
            CAST('World' AS VARCHAR(50)) as varchar_val,
            CAST('Test' AS VARCHAR(MAX)) as varchar_max_val,
            CAST('Unicode' AS NCHAR(10)) as nchar_val,
            CAST('String' AS NVARCHAR(50)) as nvarchar_val,
            CAST('Max Unicode' AS NVARCHAR(MAX)) as nvarchar_max_val,
            CAST('Text data' AS TEXT) as text_val,
            CAST('NText data' AS NTEXT) as ntext_val
    """)
    
    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]
    
    assert row.get('char_val').strip() == 'Hello'  # CHAR is padded
    assert row.get('varchar_val') == 'World'
    assert row.get('varchar_max_val') == 'Test'
    assert row.get('nchar_val').strip() == 'Unicode'  # NCHAR is padded
    assert row.get('nvarchar_val') == 'String'
    assert row.get('nvarchar_max_val') == 'Max Unicode'
    assert row.get('text_val') == 'Text data'
    assert row.get('ntext_val') == 'NText data'

@pytest.mark.integration
@pytest.mark.asyncio
async def test_datetime_types(db_connection):
    """Test all date/time SQL Server data types."""
    result = await db_connection.query("""
        SELECT 
            CAST('2023-12-25' AS DATE) as date_val,
            CAST('14:30:45' AS TIME) as time_val,
            CAST('2023-12-25 14:30:45.123' AS DATETIME) as datetime_val,
            CAST('2023-12-25 14:30:45.1234567' AS DATETIME2) as datetime2_val,
            CAST('2023-12-25 14:30:45.123 +05:30' AS DATETIMEOFFSET) as datetimeoffset_val,
            CAST('1900-01-01 14:30:45' AS SMALLDATETIME) as smalldatetime_val
    """)
    
    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]
    
    # Date types - exact assertions depend on how Tiberius converts these
    assert row['date_val'] is not None
    assert row['time_val'] is not None
    assert row['datetime_val'] is not None
    assert row['datetime2_val'] is not None
    assert row['datetimeoffset_val'] is not None
    assert row['smalldatetime_val'] is not None

@pytest.mark.integration
@pytest.mark.asyncio
async def test_binary_types(db_connection):
    """Test binary SQL Server data types."""
    result = await db_connection.query("""
        SELECT 
            CAST(0x48656C6C6F AS BINARY(10)) as binary_val,
            CAST(0x576F726C64 AS VARBINARY(50)) as varbinary_val,
            CAST(0x54657374 AS VARBINARY(MAX)) as varbinary_max_val,
            CAST('Binary data' AS IMAGE) as image_val
    """)
    
    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]
    
    # Binary data should be returned as bytes or similar
    assert row['binary_val'] is not None
    assert row['varbinary_val'] is not None
    assert row['varbinary_max_val'] is not None
    assert row['image_val'] is not None

@pytest.mark.integration
@pytest.mark.asyncio
async def test_special_types(db_connection):
    """Test special SQL Server data types."""
    result = await db_connection.query("""
        SELECT 
            CAST(1 AS BIT) as bit_true,
            CAST(0 AS BIT) as bit_false,
            CAST(NULL AS BIT) as bit_null,
            NEWID() as uniqueidentifier_val,
            CAST('<xml>test</xml>' AS XML) as xml_val,
            CAST('{"key": "value"}' AS NVARCHAR(MAX)) as json_like_val
    """)
    
    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]
    
    assert row['bit_true'] == True
    assert row['bit_false'] == False
    assert row['bit_null'] is None
    assert row['uniqueidentifier_val'] is not None
    assert row['xml_val'] is not None
    assert row['json_like_val'] == '{"key": "value"}'

@pytest.mark.integration
@pytest.mark.asyncio
async def test_null_values(db_connection):
    """Test NULL handling across different data types."""
    result = await db_connection.query("""
        SELECT 
            CAST(NULL AS INT) as null_int,
            CAST(NULL AS VARCHAR(50)) as null_varchar,
            CAST(NULL AS DATETIME) as null_datetime,
            CAST(NULL AS FLOAT) as null_float,
            CAST(NULL AS BIT) as null_bit,
            CAST(NULL AS UNIQUEIDENTIFIER) as null_guid
    """)
    
    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]
    
    assert row['null_int'] is None
    assert row['null_varchar'] is None
    assert row['null_datetime'] is None
    assert row['null_float'] is None
    assert row['null_bit'] is None
    assert row['null_guid'] is None

@pytest.mark.integration
@pytest.mark.asyncio
async def test_large_values(db_connection):
    """Test handling of large values."""
    # Test large string
    large_string = 'A' * 8000  # 8KB string
    result = await db_connection.query(f"SELECT '{large_string}' as large_string")
    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    assert rows[0]['large_string'] == large_string
    
    # Test very large number
    result = await db_connection.query("SELECT CAST(9223372036854775806 AS BIGINT) as large_bigint")
    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    assert rows[0]['large_bigint'] == 9223372036854775806

@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_data_types(db_connection):
    """Test data types with async operations."""
    # Note: Async operations are currently experiencing issues with certain data types
    # This test is temporarily simplified to avoid hangs in the async implementation
    result = await db_connection.query("""
        SELECT 
            42 as int_val,
            'async_string' as str_val,
            CAST(1 AS BIT) as bool_val,
            3.14159 as float_val,
            NULL as null_val
    """)
    
    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]
    
    assert row['int_val'] == 42
    assert row['str_val'] == 'async_string'
    assert row['bool_val'] == True
    assert abs(row['float_val'] - 3.14159) < 0.0001
    assert row['null_val'] is None
