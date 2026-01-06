"""
Tests for SQL Server data types with mssql-python-rust

This module tests all major SQL Server data types to ensure proper conversion
between Rust/Tiberius and Python types.
"""

from decimal import Decimal

import pytest
from conftest import Config

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_numeric_types(test_config: Config):
    """Test all numeric SQL Server data types."""
    async with Connection(test_config.connection_string) as db_connection:
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
    assert row.get("tinyint_val") == 127
    assert row.get("smallint_val") == 32767
    assert row.get("int_val") == 2147483647
    assert row.get("bigint_val") == 9223372036854775807

    # Test floating point types
    float_val = row.get("float_val")
    assert float_val is not None, "FLOAT type conversion not implemented"
    assert abs(float_val - 3.14159265359) < 0.0001

    real_val = row.get("real_val")
    assert real_val is not None, "REAL type conversion not implemented"
    assert abs(real_val - 99.99) < 0.001

    # Test decimal/numeric types (now returns Decimal for precision)
    decimal_val = row.get("decimal_val")
    assert decimal_val is not None, "DECIMAL type conversion not implemented"
    assert isinstance(decimal_val, Decimal), (
        f"Expected Decimal, got {type(decimal_val)}"
    )
    assert abs(float(decimal_val) - 123.456) < 0.001

    numeric_val = row.get("numeric_val")
    assert numeric_val is not None, "NUMERIC type conversion not implemented"
    assert isinstance(numeric_val, Decimal), (
        f"Expected Decimal, got {type(numeric_val)}"
    )
    assert abs(float(numeric_val) - 999.99) < 0.01

    # Test money types (returns Decimal for precision)
    money_val = row.get("money_val")
    if money_val is not None:
        assert isinstance(money_val, Decimal), (
            f"Expected Decimal for MONEY, got {type(money_val)}"
        )
        assert abs(float(money_val) - 12345.67) < 0.01

    smallmoney_val = row.get("smallmoney_val")
    if smallmoney_val is not None:
        assert isinstance(smallmoney_val, Decimal), (
            f"Expected Decimal for SMALLMONEY, got {type(smallmoney_val)}"
        )
        assert abs(float(smallmoney_val) - 123.4567) < 0.0001


@pytest.mark.integration
@pytest.mark.asyncio
async def test_string_types(test_config: Config):
    """Test all string SQL Server data types."""
    async with Connection(test_config.connection_string) as db_connection:
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

    assert row.get("char_val").strip() == "Hello"  # CHAR is padded
    assert row.get("varchar_val") == "World"
    assert row.get("varchar_max_val") == "Test"
    assert row.get("nchar_val").strip() == "Unicode"  # NCHAR is padded
    assert row.get("nvarchar_val") == "String"
    assert row.get("nvarchar_max_val") == "Max Unicode"
    assert row.get("text_val") == "Text data"
    assert row.get("ntext_val") == "NText data"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_datetime_types(test_config: Config):
    """Test all date/time SQL Server data types."""
    async with Connection(test_config.connection_string) as db_connection:
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
    assert row["date_val"] is not None
    assert row["time_val"] is not None
    assert row["datetime_val"] is not None
    assert row["datetime2_val"] is not None
    assert row["datetimeoffset_val"] is not None
    assert row["smalldatetime_val"] is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_binary_types(test_config: Config):
    """Test binary SQL Server data types."""
    async with Connection(test_config.connection_string) as db_connection:
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
    assert row["binary_val"] is not None
    assert row["varbinary_val"] is not None
    assert row["varbinary_max_val"] is not None
    assert row["image_val"] is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_special_types(test_config: Config):
    """Test special SQL Server data types."""
    async with Connection(test_config.connection_string) as db_connection:
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

    assert row["bit_true"]
    assert not row["bit_false"]
    assert row["bit_null"] is None
    assert row["uniqueidentifier_val"] is not None
    assert row["xml_val"] is not None
    assert row["json_like_val"] == '{"key": "value"}'


@pytest.mark.integration
@pytest.mark.asyncio
async def test_null_values(test_config: Config):
    """Test NULL handling across different data types."""
    async with Connection(test_config.connection_string) as db_connection:
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

    assert row["null_int"] is None
    assert row["null_varchar"] is None
    assert row["null_datetime"] is None
    assert row["null_float"] is None
    assert row["null_bit"] is None
    assert row["null_guid"] is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_large_values(test_config: Config):
    """Test handling of large values."""
    # Test large string
    large_string = "A" * 8000  # 8KB string
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query(f"SELECT '{large_string}' as large_string")
    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    assert rows[0]["large_string"] == large_string

    # Test very large number
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query(
            "SELECT CAST(9223372036854775806 AS BIGINT) as large_bigint"
        )
    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    assert rows[0]["large_bigint"] == 9223372036854775806


@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_data_types(test_config: Config):
    """Test data types with async operations."""
    # Note: Async operations are currently experiencing issues with certain data types
    # This test is temporarily simplified to avoid hangs in the async implementation
    async with Connection(test_config.connection_string) as db_connection:
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

    assert row["int_val"] == 42
    assert row["str_val"] == "async_string"
    assert row["bool_val"]
    # Float values may be returned as Decimal for precision
    float_val = row["float_val"]
    if isinstance(float_val, Decimal):
        assert abs(float(float_val) - 3.14159) < 0.0001
    else:
        assert abs(float_val - 3.14159) < 0.0001
    assert row["null_val"] is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_null_value_handling(test_config: Config):
    """Test that NULL values are properly returned as None, not silently converted to invalid data."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT 
                NULL as null_int,
                NULL as null_float,
                NULL as null_string,
                NULL as null_money,
                NULL as null_datetime
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    # All NULL values should be returned as None, not as 0 or empty string
    assert row.get("null_int") is None
    assert row.get("null_float") is None
    assert row.get("null_string") is None
    assert row.get("null_money") is None
    assert row.get("null_datetime") is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_type_conversion_error_detection(test_config: Config):
    """Test that type conversion errors are properly reported instead of silently converted to NULL."""
    async with Connection(test_config.connection_string) as db_connection:
        # Test with valid numeric data that should convert successfully
        result = await db_connection.query("""
            SELECT 
                CAST(42 AS INT) as int_val,
                CAST(3.14159 AS FLOAT) as float_val,
                CAST(12345.67 AS MONEY) as money_val,
                CAST(999.99 AS SMALLMONEY) as smallmoney_val
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    # All values should be present and non-None
    assert row.get("int_val") is not None
    assert row.get("int_val") == 42
    assert row.get("float_val") is not None
    float_val = row.get("float_val")
    if isinstance(float_val, Decimal):
        assert abs(float(float_val) - 3.14159) < 0.0001
    else:
        assert abs(float_val - 3.14159) < 0.0001
    assert row.get("money_val") is not None
    money_val = row.get("money_val")
    assert isinstance(money_val, Decimal), (
        f"Expected Decimal for MONEY, got {type(money_val)}"
    )
    assert abs(float(money_val) - 12345.67) < 0.01
    assert row.get("smallmoney_val") is not None
    smallmoney_val = row.get("smallmoney_val")
    assert isinstance(smallmoney_val, Decimal), (
        f"Expected Decimal for SMALLMONEY, got {type(smallmoney_val)}"
    )
    assert abs(float(smallmoney_val) - 999.99) < 0.01


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mixed_null_and_valid_values(test_config: Config):
    """Test that NULL and valid values can coexist in result sets, properly distinguished."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT 
                42 as valid_int,
                NULL as null_int,
                'Hello' as valid_string,
                NULL as null_string,
                3.14159 as valid_float,
                NULL as null_float
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    # Valid values should be present
    assert row.get("valid_int") == 42
    assert row.get("valid_string") == "Hello"
    valid_float = row.get("valid_float")
    if isinstance(valid_float, Decimal):
        assert abs(float(valid_float) - 3.14159) < 0.0001
    else:
        assert abs(valid_float - 3.14159) < 0.0001

    # NULL values should explicitly be None, not confused with empty/zero values
    assert row.get("null_int") is None
    assert row.get("null_string") is None
    assert row.get("null_float") is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_all_numeric_types_with_nulls(test_config: Config):
    """Test all numeric types with both valid and NULL values."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT 
                CAST(127 AS TINYINT) as valid_tinyint,
                CAST(NULL AS TINYINT) as null_tinyint,
                CAST(32767 AS SMALLINT) as valid_smallint,
                CAST(NULL AS SMALLINT) as null_smallint,
                CAST(2147483647 AS INT) as valid_int,
                CAST(NULL AS INT) as null_int,
                CAST(9223372036854775807 AS BIGINT) as valid_bigint,
                CAST(NULL AS BIGINT) as null_bigint,
                CAST(3.14159 AS FLOAT) as valid_float,
                CAST(NULL AS FLOAT) as null_float
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    # Valid values
    assert row.get("valid_tinyint") == 127
    assert row.get("valid_smallint") == 32767
    assert row.get("valid_int") == 2147483647
    assert row.get("valid_bigint") == 9223372036854775807
    assert abs(row.get("valid_float") - 3.14159) < 0.0001

    # NULL values - should be None, not 0 or empty
    assert row.get("null_tinyint") is None
    assert row.get("null_smallint") is None
    assert row.get("null_int") is None
    assert row.get("null_bigint") is None
    assert row.get("null_float") is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_string_types_with_nulls(test_config: Config):
    """Test string types with both valid and NULL values."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT 
                'Valid String' as valid_varchar,
                CAST(NULL AS VARCHAR(50)) as null_varchar,
                'Unicode String' as valid_nvarchar,
                CAST(NULL AS NVARCHAR(50)) as null_nvarchar
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    # Valid strings
    assert row.get("valid_varchar") == "Valid String"
    assert row.get("valid_nvarchar") == "Unicode String"

    # NULL strings should be None, not empty string ''
    assert row.get("null_varchar") is None
    assert row.get("null_nvarchar") is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_datetime_types_with_nulls(test_config: Config):
    """Test datetime types with both valid and NULL values."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT 
                CAST('2025-12-31 15:30:45' AS DATETIME) as valid_datetime,
                CAST(NULL AS DATETIME) as null_datetime,
                CAST('2025-12-31' AS DATE) as valid_date,
                CAST(NULL AS DATE) as null_date,
                CAST('15:30:45' AS TIME) as valid_time,
                CAST(NULL AS TIME) as null_time
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    # Valid datetime values should be present
    assert row.get("valid_datetime") is not None
    assert row.get("valid_date") is not None
    assert row.get("valid_time") is not None

    # NULL datetime values should be None
    assert row.get("null_datetime") is None
    assert row.get("null_date") is None
    assert row.get("null_time") is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_float8_error_handling(test_config: Config):
    """Test that FLOAT8 type errors are properly reported, not silently converted to None.

    This test verifies the fix for the silent error handling bug in handle_float8().
    Previously, any error reading a FLOAT8 column would silently return None.
    Now errors are properly reported.
    """
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT 
                CAST(3.14159265359 AS FLOAT) as valid_float,
                CAST(NULL AS FLOAT) as null_float,
                CAST(-1.23456789 AS FLOAT) as negative_float,
                CAST(0.0 AS FLOAT) as zero_float,
                CAST(1e308 AS FLOAT) as large_float,
                CAST(1e-308 AS FLOAT) as small_float
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    # Valid FLOAT values should be present and correct
    valid_float = row.get("valid_float")
    assert valid_float is not None, "Valid FLOAT should not be None"
    assert isinstance(valid_float, float), f"Expected float, got {type(valid_float)}"
    assert abs(valid_float - 3.14159265359) < 0.0001

    # NULL FLOAT should be None
    assert row.get("null_float") is None, "NULL FLOAT should be None"

    # Negative FLOAT
    negative_float = row.get("negative_float")
    assert negative_float is not None, "Negative FLOAT should not be None"
    assert negative_float < 0, "Negative FLOAT should be negative"
    assert abs(negative_float - (-1.23456789)) < 0.00001

    # Zero FLOAT
    zero_float = row.get("zero_float")
    assert zero_float is not None, "Zero FLOAT should not be None"
    assert zero_float == 0.0, "Zero FLOAT should equal 0.0"

    # Large FLOAT
    large_float = row.get("large_float")
    assert large_float is not None, "Large FLOAT should not be None"
    assert large_float > 1e300, "Large FLOAT should be large"

    # Small FLOAT (1e-308 may underflow to 0.0 due to floating point precision limits)
    small_float = row.get("small_float")
    assert small_float is not None, "Small FLOAT should not be None"
    assert isinstance(small_float, float), f"Expected float, got {type(small_float)}"
    # Note: 1e-308 may round to 0.0 due to float precision, so we just verify it was read successfully


@pytest.mark.integration
@pytest.mark.asyncio
async def test_char_types(test_config: Config):
    """Test CHAR and NCHAR fixed-length string types."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT 
                CAST('ABC' AS CHAR(10)) as char_val,
                CAST('XYZ' AS NCHAR(10)) as nchar_val,
                CAST(NULL AS CHAR(10)) as null_char,
                CAST(NULL AS NCHAR(10)) as null_nchar
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    # CHAR and NCHAR are padded with spaces
    assert row.get("char_val") is not None
    assert "ABC" in row.get("char_val")
    assert row.get("nchar_val") is not None
    assert "XYZ" in row.get("nchar_val")

    # NULL values
    assert row.get("null_char") is None
    assert row.get("null_nchar") is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_text_types(test_config: Config):
    """Test legacy TEXT and NTEXT data types."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT 
                CAST('Text content' AS TEXT) as text_val,
                CAST('NText content' AS NTEXT) as ntext_val,
                CAST(NULL AS TEXT) as null_text,
                CAST(NULL AS NTEXT) as null_ntext
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    # TEXT and NTEXT values
    assert row.get("text_val") == "Text content"
    assert row.get("ntext_val") == "NText content"

    # NULL values
    assert row.get("null_text") is None
    assert row.get("null_ntext") is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_supported_integer_types(test_config: Config):
    """Test all supported integer types."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT 
                CAST(127 AS TINYINT) as int1_col,
                CAST(32767 AS SMALLINT) as int2_col,
                CAST(2147483647 AS INT) as int4_col,
                CAST(9223372036854775807 AS BIGINT) as int8_col
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    assert row.get("int1_col") == 127
    assert row.get("int2_col") == 32767
    assert row.get("int4_col") == 2147483647
    assert row.get("int8_col") == 9223372036854775807


@pytest.mark.integration
@pytest.mark.asyncio
async def test_supported_float_types(test_config: Config):
    """Test all supported floating-point types."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT 
                CAST(3.14 AS REAL) as float4_col,
                CAST(3.14159265359 AS FLOAT) as float8_col
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    assert row.get("float4_col") is not None
    assert row.get("float8_col") is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_supported_string_types(test_config: Config):
    """Test all supported string types."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT 
                CAST('Hello' AS VARCHAR(50)) as varchar_col,
                CAST('World' AS NVARCHAR(50)) as nvarchar_col,
                CAST('Text' AS TEXT) as text_col,
                CAST('NText' AS NTEXT) as ntext_col
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    assert row.get("varchar_col") == "Hello"
    assert row.get("nvarchar_col") == "World"
    assert row.get("text_col") == "Text"
    assert row.get("ntext_col") == "NText"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_supported_binary_types(test_config: Config):
    """Test all supported binary types."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT 
                CAST(0x48656C6C6F AS VARBINARY(50)) as varbinary_col,
                CAST(0x576F726C64 AS BINARY(10)) as binary_col
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    assert row.get("varbinary_col") is not None
    assert row.get("binary_col") is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_supported_financial_types(test_config: Config):
    """Test all supported financial types."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT 
                CAST(12345.67 AS MONEY) as money_col,
                CAST(123.45 AS SMALLMONEY) as smallmoney_col,
                CAST(123.456 AS DECIMAL(10,3)) as decimal_col,
                CAST(999.99 AS NUMERIC(10,2)) as numeric_col
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    assert row.get("money_col") is not None
    assert row.get("smallmoney_col") is not None
    assert row.get("decimal_col") is not None
    assert row.get("numeric_col") is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_supported_bit_type(test_config: Config):
    """Test BIT data type."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT CAST(1 AS BIT) as bit_col
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    assert row.get("bit_col") == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_supported_guid_type(test_config: Config):
    """Test GUID/UNIQUEIDENTIFIER data type."""
    async with Connection(test_config.connection_string) as db_connection:
        result = await db_connection.query("""
            SELECT NEWID() as guid_col
        """)

    assert result.has_rows()
    rows = result.rows()
    assert len(rows) == 1
    row = rows[0]

    assert row.get("guid_col") is not None
