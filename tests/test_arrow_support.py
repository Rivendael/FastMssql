import pytest
from conftest import Config

try:
    import pyarrow as pa
except ImportError:
    pa = None

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first")


# Skip all Arrow tests if PyArrow is not available
pytestmark = pytest.mark.skipif(pa is None, reason="PyArrow not installed")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_basic_to_arrow_conversion(test_config: Config):
    """Test basic conversion of query results to Arrow table."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as test_value, 'hello' as text_value")

            assert result.has_rows(), "Expected result to have rows"

            arrow_table = result.to_arrow()

            assert isinstance(arrow_table, pa.Table), "Expected PyArrow Table object"

            assert arrow_table.column_names == [
                "test_value",
                "text_value",
            ], "Column names mismatch"

            assert arrow_table.num_rows == 1, "Expected 1 row"
            assert arrow_table["test_value"][0].as_py() == 1
            assert arrow_table["text_value"][0].as_py() == "hello"
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_with_integers(test_config: Config):
    """Test Arrow conversion with various integer types."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT
                    CAST(42 as TINYINT) as tiny_int,
                    CAST(1000 as SMALLINT) as small_int,
                    CAST(100000 as INT) as reg_int,
                    CAST(9223372036854775800 as BIGINT) as big_int
                """
            )

            arrow_table = result.to_arrow()

            assert arrow_table.num_rows == 1
            assert arrow_table["tiny_int"][0].as_py() == 42
            assert arrow_table["small_int"][0].as_py() == 1000
            assert arrow_table["reg_int"][0].as_py() == 100000
            assert arrow_table["big_int"][0].as_py() == 9223372036854775800
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_with_floating_point(test_config: Config):
    """Test Arrow conversion with floating point numbers."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT
                    CAST(3.14 as FLOAT) as float_val,
                    CAST(2.71828 as REAL) as real_val
                """
            )

            arrow_table = result.to_arrow()

            assert arrow_table.num_rows == 1
            float_val = arrow_table["float_val"][0].as_py()
            assert float_val is not None
            real_val = arrow_table["real_val"][0].as_py()
            assert real_val is not None
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_with_strings(test_config: Config):
    """Test Arrow conversion with string data types."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT
                    'varchar data' as varchar_col,
                    N'nvarchar data' as nvarchar_col,
                    'fixed char' as char_col
                """
            )

            arrow_table = result.to_arrow()

            assert arrow_table.num_rows == 1
            assert arrow_table["varchar_col"][0].as_py() == "varchar data"
            assert arrow_table["nvarchar_col"][0].as_py() == "nvarchar data"
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_with_booleans(test_config: Config):
    """Test Arrow conversion with boolean data."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT
                    CAST(1 as BIT) as true_val,
                    CAST(0 as BIT) as false_val
                """
            )

            arrow_table = result.to_arrow()

            assert arrow_table.num_rows == 1
            assert arrow_table["true_val"][0].as_py()
            assert not arrow_table["false_val"][0].as_py()
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_with_nulls(test_config: Config):
    """Test Arrow conversion with NULL values."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT
                    NULL as null_int,
                    NULL as null_string,
                    1 as not_null,
                    NULL as another_null
                """
            )

            arrow_table = result.to_arrow()

            assert arrow_table.num_rows == 1
            assert arrow_table["null_int"][0].as_py() is None
            assert arrow_table["null_string"][0].as_py() is None
            assert arrow_table["not_null"][0].as_py() == 1
            assert arrow_table["another_null"][0].as_py() is None
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_with_dates(test_config: Config):
    """Test Arrow conversion with date and datetime types."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT
                    CAST('2024-02-13' as DATE) as date_val,
                    CAST('2024-02-13 14:30:45' as DATETIME) as datetime_val
                """
            )

            arrow_table = result.to_arrow()

            assert arrow_table.num_rows == 1
            assert "date_val" in arrow_table.column_names
            assert "datetime_val" in arrow_table.column_names
            date_val = arrow_table["date_val"][0].as_py()
            assert date_val is not None
            datetime_val = arrow_table["datetime_val"][0].as_py()
            assert datetime_val is not None
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_with_decimals(test_config: Config):
    """Test Arrow conversion with DECIMAL and MONEY types."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT
                    CAST(123.45 as DECIMAL(10, 2)) as decimal_val,
                    CAST(99.99 as MONEY) as money_val
                """
            )

            arrow_table = result.to_arrow()

            assert arrow_table.num_rows == 1
            # Verify columns exist
            assert "decimal_val" in arrow_table.column_names
            assert "money_val" in arrow_table.column_names
            # These should be present in the table
            _ = arrow_table["decimal_val"][0].as_py()
            # Money values come through as Decimal objects
            money_val = arrow_table["money_val"][0].as_py()
            assert money_val is not None
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_multiple_rows(test_config: Config):
    """Test Arrow conversion with multiple rows."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT 1 as id, 'first' as name
                UNION ALL
                SELECT 2, 'second'
                UNION ALL
                SELECT 3, 'third'
                """
            )

            arrow_table = result.to_arrow()

            assert arrow_table.num_rows == 3
            assert arrow_table["id"][0].as_py() == 1
            assert arrow_table["id"][1].as_py() == 2
            assert arrow_table["id"][2].as_py() == 3
            assert arrow_table["name"][0].as_py() == "first"
            assert arrow_table["name"][1].as_py() == "second"
            assert arrow_table["name"][2].as_py() == "third"
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_many_columns(test_config: Config):
    """Test Arrow conversion with many columns."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT 
                    1 as col1, 2 as col2, 3 as col3, 4 as col4, 5 as col5,
                    6 as col6, 7 as col7, 8 as col8, 9 as col9, 10 as col10
                """
            )

            arrow_table = result.to_arrow()

            assert arrow_table.num_rows == 1
            assert len(arrow_table.column_names) == 10
            for i in range(1, 11):
                assert arrow_table[f"col{i}"][0].as_py() == i
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_schema_types(test_config: Config):
    """Test that Arrow schema has correct types."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT
                    1 as int_col,
                    CAST(3.14 as REAL) as float_col,
                    'text' as string_col,
                    CAST(1 as BIT) as bool_col
                """
            )

            arrow_table = result.to_arrow()
            schema = arrow_table.schema

            # Verify schema field types
            assert schema.field("int_col").type == pa.int64()
            # Float columns might be float32 or float64 depending on REAL vs FLOAT
            assert schema.field("float_col").type in [pa.float32(), pa.float64()]
            assert schema.field("string_col").type == pa.string()
            # Note: bool_ might be stored as int or other type depending on pyarrow version
            assert schema.field("bool_col").type in [pa.bool_(), pa.int64()]
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_column_names_preserved(test_config: Config):
    """Test that column names are correctly preserved in Arrow table."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT
                    1 as FirstColumn,
                    2 as SecondColumn,
                    3 as ThirdColumn
                """
            )

            arrow_table = result.to_arrow()

            assert "FirstColumn" in arrow_table.column_names
            assert "SecondColumn" in arrow_table.column_names
            assert "ThirdColumn" in arrow_table.column_names
            assert len(arrow_table.column_names) == 3
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_large_dataset(test_config: Config):
    """Test Arrow conversion with larger dataset."""
    try:
        async with Connection(test_config.connection_string) as conn:
            # Generate 100+ rows
            result = await conn.query(
                """
                WITH Numbers AS (
                    SELECT 1 as num
                    UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                    UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                    UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20
                )
                SELECT n1.num * 10 + n2.num as value FROM Numbers n1, Numbers n2 WHERE n1.num <= 5
                """
            )

            arrow_table = result.to_arrow()

            # Should have at least 100 rows
            assert arrow_table.num_rows >= 100
            assert len(arrow_table.column_names) == 1
            assert arrow_table.column_names[0] == "value"
            # Verify data exists
            for i in range(min(10, arrow_table.num_rows)):
                assert arrow_table["value"][i].as_py() is not None
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_pyarrow_methods(test_config: Config):
    """Test that returned Arrow table supports PyArrow methods."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT 1 as id, 'test' as name
                UNION ALL
                SELECT 2, 'data'
                """
            )

            arrow_table = result.to_arrow()

            # Test PyArrow methods
            assert arrow_table.num_rows == 2
            assert arrow_table.num_columns == 2

            # Test column access methods
            col1 = arrow_table.column("id")
            assert len(col1) == 2

            # Test iteration
            for i in range(arrow_table.num_rows):
                row = arrow_table.take([i])
                assert row.num_rows == 1
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_with_parameterized_query(test_config: Config):
    """Test Arrow conversion with parameterized queries."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT @P1 as param_value, @P2 as param_string",
                [42, "test string"],
            )

            arrow_table = result.to_arrow()

            assert arrow_table.num_rows == 1
            assert arrow_table["param_value"][0].as_py() == 42
            assert arrow_table["param_string"][0].as_py() == "test string"
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_null_handling_comprehensive(test_config: Config):
    """Test comprehensive null value handling in Arrow conversion."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT
                    NULL as null_int,
                    NULL as null_float,
                    NULL as null_string,
                    1 as val1,
                    'notNull' as val2,
                    NULL as val3,
                    42 as val4
                """
            )

            arrow_table = result.to_arrow()

            assert arrow_table.num_rows == 1

            # Check nulls
            assert arrow_table["null_int"][0].as_py() is None
            assert arrow_table["null_float"][0].as_py() is None
            assert arrow_table["null_string"][0].as_py() is None

            # Check non-nulls
            assert arrow_table["val1"][0].as_py() == 1
            assert arrow_table["val2"][0].as_py() == "notNull"
            assert arrow_table["val3"][0].as_py() is None
            assert arrow_table["val4"][0].as_py() == 42
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_mixed_types_single_row(test_config: Config):
    """Test Arrow conversion with mixed data types in single row."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT
                    1 as int_col,
                    CAST(3.14 as REAL) as float_col,
                    'mixed' as string_col,
                    CAST(1 as BIT) as bool_col,
                    NULL as null_col,
                    CAST(99.99 as MONEY) as money_col
                """
            )

            arrow_table = result.to_arrow()

            assert arrow_table.num_rows == 1
            assert len(arrow_table.column_names) == 6

            # Verify each column
            assert arrow_table["int_col"][0].as_py() == 1
            float_val = arrow_table["float_col"][0].as_py()
            assert float_val is not None
            assert arrow_table["string_col"][0].as_py() == "mixed"
            # Bool might be stored as int(1) or bool(True)
            bool_val = arrow_table["bool_col"][0].as_py()
            assert bool_val in [True, 1]
            assert arrow_table["null_col"][0].as_py() is None
            # Money value should be present
            money_val = arrow_table["money_col"][0].as_py()
            assert money_val is not None
    except Exception as e:
        pytest.fail(f"Test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_arrow_consistency_with_rows(test_config: Config):
    """Test that Arrow conversion is consistent with rows() data."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT 1 as id, 'first' as name
                UNION ALL
                SELECT 2, 'second'
                """
            )

            # Get traditional rows
            rows = result.rows()

            # Reset and get Arrow
            result_arrow = await conn.query(
                """
                SELECT 1 as id, 'first' as name
                UNION ALL
                SELECT 2, 'second'
                """
            )
            arrow_table = result_arrow.to_arrow()

            # Verify consistency
            assert len(rows) == arrow_table.num_rows

            for i, row in enumerate(rows):
                assert row["id"] == arrow_table["id"][i].as_py()
                assert row["name"] == arrow_table["name"][i].as_py()
    except Exception as e:
        pytest.fail(f"Test failed: {e}")
