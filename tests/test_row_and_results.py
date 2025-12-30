"""
Tests for PyFastRow and PyFastExecutionResult

This module tests the row and result set classes to ensure proper access patterns,
type handling, and edge cases when retrieving data from queries.
"""

import pytest
import os

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = os.getenv("FASTMSSQL_TEST_CONNECTION_STRING")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_dict_access():
    """Test accessing row columns using dictionary-style access."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 1 as id, 'test' as name, 3.14 as value")
            assert result.has_rows()
            rows = result.rows()
            assert len(rows) == 1
            
            row = rows[0]
            assert row['id'] == 1
            assert row['name'] == 'test'
            assert abs(row['value'] - 3.14) < 0.001
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_get_method():
    """Test accessing row columns using get() method with defaults."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 1 as id, NULL as missing_value")
            assert result.has_rows()
            rows = result.rows()
            row = rows[0]
            
            # Test get() with existing column
            assert row.get('id') == 1
            
            # Test get() with NULL value
            assert row.get('missing_value') is None
            
            # Test get() with non-existent column (should raise ValueError or return None)
            try:
                value = row.get('non_existent_column')
                # If it doesn't raise, it should return None
                assert value is None
            except (KeyError, ValueError):
                # This is acceptable behavior - raises on missing column
                pass
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_none_values():
    """Test handling of NULL values in row columns."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    NULL as null_int,
                    NULL as null_string,
                    CAST(NULL AS VARCHAR(50)) as null_varchar,
                    1 as non_null_value
            """)
            assert result.has_rows()
            row = result.rows()[0]
            
            # All NULL columns should return None
            assert row.get('null_int') is None
            assert row.get('null_string') is None
            assert row.get('null_varchar') is None
            
            # Non-NULL column should have a value
            assert row.get('non_null_value') == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_column_iteration():
    """Test iterating over row columns."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 1 as col1, 'test' as col2, 3.14 as col3")
            assert result.has_rows()
            row = result.rows()[0]
            
            # Try to iterate over columns
            column_names = []
            column_values = []
            
            # Test if row supports iteration or keys method
            if hasattr(row, 'keys'):
                column_names = list(row.keys())
            elif hasattr(row, '__iter__'):
                for item in row:
                    if isinstance(item, tuple):
                        column_names.append(item[0])
                        column_values.append(item[1])
            
            # Verify we got the columns we expected
            if column_names:
                assert 'col1' in column_names
                assert 'col2' in column_names
                assert 'col3' in column_names
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_special_column_names():
    """Test handling of columns with special names."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Test columns with spaces, reserved words, etc.
            result = await conn.query("""
                SELECT 
                    1 as [Column With Spaces],
                    'test' as [select],
                    3.14 as [order],
                    42 as regular_column
            """)
            assert result.has_rows()
            row = result.rows()[0]
            
            # Access columns with special names
            assert row['Column With Spaces'] == 1
            assert row['select'] == 'test'
            assert row['order'] == 3.14
            assert row['regular_column'] == 42
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execution_result_has_rows():
    """Test has_rows() method for different query types."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # SELECT query should have rows
            result = await conn.query("SELECT 1 as val")
            assert result.has_rows()
            
            # Query with no results
            result = await conn.query("SELECT * FROM (SELECT 1 as val WHERE 0=1) as empty")
            assert not result.has_rows()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execution_result_rows_method():
    """Test rows() method returns list of rows."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3")
            assert result.has_rows()
            
            rows = result.rows()
            assert isinstance(rows, list)
            assert len(rows) == 3
            
            # Verify row values
            assert rows[0]['id'] == 1
            assert rows[1]['id'] == 2
            assert rows[2]['id'] == 3
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execution_result_empty_rows():
    """Test rows() method when result has no rows."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 1 as val WHERE 0=1")
            assert not result.has_rows()
            
            rows = result.rows()
            assert isinstance(rows, list)
            assert len(rows) == 0
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execution_result_affected_rows():
    """Test affected_rows() for INSERT/UPDATE/DELETE operations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table for testing
            await conn.execute("""
                IF OBJECT_ID('tempdb..##test_rows', 'U') IS NOT NULL
                    DROP TABLE ##test_rows
            """)
            
            await conn.execute("""
                CREATE TABLE ##test_rows (
                    id INT PRIMARY KEY,
                    value VARCHAR(50)
                )
            """)
            
            # Test INSERT
            result = await conn.execute("INSERT INTO ##test_rows (id, value) VALUES (@P1, @P2)", [1, 'test'])
            assert result == 1
            
            # Test UPDATE
            result = await conn.execute("UPDATE ##test_rows SET value = @P1 WHERE id = @P2", ['updated', 1])
            assert result == 1
            
            # Test DELETE
            result = await conn.execute("DELETE FROM ##test_rows WHERE id = @P1", [1])
            assert result == 1
            
            # Cleanup
            await conn.execute("DROP TABLE ##test_rows")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_with_multiple_columns():
    """Test row access with many columns."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    1 as col1,
                    2 as col2,
                    3 as col3,
                    4 as col4,
                    5 as col5,
                    6 as col6,
                    7 as col7,
                    8 as col8,
                    'value' as col9,
                    'another' as col10
            """)
            assert result.has_rows()
            row = result.rows()[0]
            
            # Access all columns
            for i in range(1, 9):
                assert row[f'col{i}'] == i
            assert row['col9'] == 'value'
            assert row['col10'] == 'another'
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_type_preservation():
    """Test that row values preserve their types correctly."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    CAST(42 AS INT) as int_val,
                    CAST('string' AS VARCHAR(50)) as string_val,
                    CAST(3.14 AS FLOAT) as float_val,
                    CAST(0 AS BIT) as bit_false,
                    CAST(1 AS BIT) as bit_true
            """)
            assert result.has_rows()
            row = result.rows()[0]
            
            # Verify types
            assert isinstance(row['int_val'], int)
            assert row['int_val'] == 42
            
            assert isinstance(row['string_val'], str)
            assert row['string_val'] == 'string'
            
            # Float comparison with tolerance
            assert isinstance(row['float_val'], (int, float))
            assert abs(row['float_val'] - 3.14) < 0.001
            
            # Bit values (should be boolean-like)
            bit_false = row['bit_false']
            bit_true = row['bit_true']
            assert bit_false in [0, False, None]  # Accept different representations
            assert bit_true in [1, True]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_case_sensitivity():
    """Test row column access case sensitivity."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 1 as TestColumn")
            assert result.has_rows()
            row = result.rows()[0]
            
            # Try accessing with original case
            assert row['TestColumn'] == 1
            
            # Try other case variations (SQL Server is case-insensitive for column names)
            try:
                assert row['testcolumn'] == 1  # May work if implementation is case-insensitive
            except (KeyError, TypeError, ValueError):
                # This is also acceptable if the implementation is case-sensitive
                pass
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_result_rows_independence():
    """Test that multiple rows are independent objects."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 1 as id, 'first' as val UNION ALL SELECT 2, 'second'")
            assert result.has_rows()
            
            rows = result.rows()
            row1 = rows[0]
            row2 = rows[1]
            
            # Rows should have different values
            assert row1['id'] != row2['id']
            assert row1['val'] != row2['val']
            assert row1['id'] == 1
            assert row2['id'] == 2
    except Exception as e:
        pytest.fail(f"Database not available: {e}")
