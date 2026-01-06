"""
Tests for PyFastRow and PyQueryStream

This module tests the row and result set classes to ensure proper access patterns,
type handling, and edge cases when retrieving data from queries. Includes comprehensive
tests for async iteration, backward-compatible methods, and result streaming.
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
async def test_row_dict_access(test_config: Config):
    """Test accessing row columns using dictionary-style access."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as id, 'test' as name, 3.14 as value")
            assert result.has_rows()
            rows = result.rows()
            assert len(rows) == 1

            row = rows[0]
            assert row["id"] == 1
            assert row["name"] == "test"
            value = row["value"]
            if isinstance(value, Decimal):
                assert abs(float(value) - 3.14) < 0.001
            else:
                assert abs(value - 3.14) < 0.001
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_get_method(test_config: Config):
    """Test accessing row columns using get() method with defaults."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as id, NULL as missing_value")
            assert result.has_rows()
            rows = result.rows()
            row = rows[0]

            # Test get() with existing column
            assert row.get("id") == 1

            # Test get() with NULL value
            assert row.get("missing_value") is None

            # Test get() with non-existent column (should raise ValueError or return None)
            try:
                value = row.get("non_existent_column")
                # If it doesn't raise, it should return None
                assert value is None
            except (KeyError, ValueError):
                # This is acceptable behavior - raises on missing column
                pass
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_none_values(test_config: Config):
    """Test handling of NULL values in row columns."""
    try:
        async with Connection(test_config.connection_string) as conn:
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
            assert row.get("null_int") is None
            assert row.get("null_string") is None
            assert row.get("null_varchar") is None

            # Non-NULL column should have a value
            assert row.get("non_null_value") == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_column_iteration(test_config: Config):
    """Test iterating over row columns."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as col1, 'test' as col2, 3.14 as col3")
            assert result.has_rows()
            row = result.rows()[0]

            # Try to iterate over columns
            column_names = []
            column_values = []

            # Test if row supports iteration or keys method
            if hasattr(row, "keys"):
                column_names = list(row.keys())
            elif hasattr(row, "__iter__"):
                for item in row:
                    if isinstance(item, tuple):
                        column_names.append(item[0])
                        column_values.append(item[1])

            # Verify we got the columns we expected
            if column_names:
                assert "col1" in column_names
                assert "col2" in column_names
                assert "col3" in column_names
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_special_column_names(test_config: Config):
    """Test handling of columns with special names."""
    try:
        async with Connection(test_config.connection_string) as conn:
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
            assert row["Column With Spaces"] == 1
            assert row["select"] == "test"
            order_val = row["order"]
            if isinstance(order_val, Decimal):
                assert float(order_val) == 3.14
            else:
                assert order_val == 3.14
            assert row["regular_column"] == 42
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execution_result_has_rows(test_config: Config):
    """Test has_rows() method for different query types."""
    try:
        async with Connection(test_config.connection_string) as conn:
            # SELECT query should have rows
            result = await conn.query("SELECT 1 as val")
            assert result.has_rows()

            # Query with no results
            result = await conn.query(
                "SELECT * FROM (SELECT 1 as val WHERE 0=1) as empty"
            )
            assert not result.has_rows()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execution_result_rows_method(test_config: Config):
    """Test rows() method returns list of rows."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3"
            )
            assert result.has_rows()

            rows = result.rows()
            assert isinstance(rows, list)
            assert len(rows) == 3

            # Verify row values
            assert rows[0]["id"] == 1
            assert rows[1]["id"] == 2
            assert rows[2]["id"] == 3
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execution_result_empty_rows(test_config: Config):
    """Test rows() method when result has no rows."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as val WHERE 0=1")
            assert not result.has_rows()

            rows = result.rows()
            assert isinstance(rows, list)
            assert len(rows) == 0
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execution_result_affected_rows(test_config: Config):
    """Test affected_rows() for INSERT/UPDATE/DELETE operations."""
    try:
        async with Connection(test_config.connection_string) as conn:
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
            result = await conn.execute(
                "INSERT INTO ##test_rows (id, value) VALUES (@P1, @P2)", [1, "test"]
            )
            assert result == 1

            # Test UPDATE
            result = await conn.execute(
                "UPDATE ##test_rows SET value = @P1 WHERE id = @P2", ["updated", 1]
            )
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
async def test_row_with_multiple_columns(test_config: Config):
    """Test row access with many columns."""
    try:
        async with Connection(test_config.connection_string) as conn:
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
                assert row[f"col{i}"] == i
            assert row["col9"] == "value"
            assert row["col10"] == "another"
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_type_preservation(test_config: Config):
    """Test that row values preserve their types correctly."""
    try:
        async with Connection(test_config.connection_string) as conn:
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
            assert isinstance(row["int_val"], int)
            assert row["int_val"] == 42

            assert isinstance(row["string_val"], str)
            assert row["string_val"] == "string"

            # Float comparison with tolerance
            assert isinstance(row["float_val"], (int, float))
            assert abs(row["float_val"] - 3.14) < 0.001

            # Bit values (should be boolean-like)
            bit_false = row["bit_false"]
            bit_true = row["bit_true"]
            assert bit_false in [0, False, None]  # Accept different representations
            assert bit_true in [1, True]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_row_case_sensitivity(test_config: Config):
    """Test row column access case sensitivity."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as TestColumn")
            assert result.has_rows()
            row = result.rows()[0]

            # Try accessing with original case
            assert row["TestColumn"] == 1

            # Try other case variations (SQL Server is case-insensitive for column names)
            try:
                assert (
                    row["testcolumn"] == 1
                )  # May work if implementation is case-insensitive
            except (KeyError, TypeError, ValueError):
                # This is also acceptable if the implementation is case-sensitive
                pass
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_result_rows_independence(test_config: Config):
    """Test that multiple rows are independent objects."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id, 'first' as val UNION ALL SELECT 2, 'second'"
            )
            assert result.has_rows()

            rows = result.rows()
            row1 = rows[0]
            row2 = rows[1]

            # Rows should have different values
            assert row1["id"] != row2["id"]
            assert row1["val"] != row2["val"]
            assert row1["id"] == 1
            assert row2["id"] == 2
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_stream_fetchone_method(test_config: Config):
    """Test fetchone() method returns first row."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3"
            )

            row = result.fetchone()
            assert row is not None
            assert row["id"] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_stream_fetchone_empty(test_config: Config):
    """Test fetchone() on empty result."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as id WHERE 0=1")

            row = result.fetchone()
            assert row is None
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_stream_fetchmany_method(test_config: Config):
    """Test fetchmany() method."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 
                UNION ALL SELECT 4 UNION ALL SELECT 5
            """)

            # Fetch 2 rows
            rows = result.fetchmany(2)
            assert len(rows) == 2
            assert rows[0]["id"] == 1
            assert rows[1]["id"] == 2
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_stream_fetchmany_more_than_available(test_config: Config):
    """Test fetchmany() requesting more rows than available."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as id UNION ALL SELECT 2")

            # Try to fetch 10 rows (only 2 available)
            rows = result.fetchmany(10)
            assert len(rows) == 2
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_stream_fetchall_method(test_config: Config):
    """Test fetchall() method."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3
            """)

            rows = result.fetchall()
            assert len(rows) == 3
            assert rows[0]["id"] == 1
            assert rows[1]["id"] == 2
            assert rows[2]["id"] == 3
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_stream_fetchall_empty(test_config: Config):
    """Test fetchall() on empty result."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as id WHERE 0=1")

            rows = result.fetchall()
            assert len(rows) == 0
            assert isinstance(rows, list)
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_stream_position_tracking(test_config: Config):
    """Test position tracking during iteration."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3
            """)

            assert result.position() == 0

            result.fetchone()
            assert result.position() == 1

            result.fetchone()
            assert result.position() == 2

            result.fetchone()
            assert result.position() == 3
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_stream_reset_method(test_config: Config):
    """Test reset() method to restart iteration."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3
            """)

            # Read some rows
            row1 = result.fetchone()
            assert row1["id"] == 1
            assert result.position() == 1

            # Reset
            result.reset()
            assert result.position() == 0

            # Should be able to read from start again
            row1_again = result.fetchone()
            assert row1_again["id"] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_stream_columns_metadata(test_config: Config):
    """Test accessing columns metadata."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as col1, 'text' as col2, 3.14 as col3
            """)

            columns = result.columns()
            assert columns is not None
            assert len(columns) == 3
            assert "col1" in columns
            assert "col2" in columns
            assert "col3" in columns
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_stream_length(test_config: Config):
    """Test len() on QueryStream."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3
            """)

            assert len(result) == 3
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_stream_is_empty(test_config: Config):
    """Test is_empty() method."""
    try:
        async with Connection(test_config.connection_string) as conn:
            # Non-empty result
            result1 = await conn.query("SELECT 1 as id")
            assert not result1.is_empty()

            # Empty result
            result2 = await conn.query("SELECT 1 as id WHERE 0=1")
            assert result2.is_empty()
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


# ============================================================================
# ITERATION TESTS - Manual iteration over results
# ============================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iterate_with_fetchone_loop(test_config: Config):
    """Test manual iteration using fetchone() in a loop."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            """)

            # Manual iteration with fetchone
            rows = []
            while True:
                row = result.fetchone()
                if row is None:
                    break
                rows.append(row)

            assert len(rows) == 4
            assert rows[0]["id"] == 1
            assert rows[1]["id"] == 2
            assert rows[2]["id"] == 3
            assert rows[3]["id"] == 4
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iterate_with_fetchmany_loop(test_config: Config):
    """Test batch iteration using fetchmany() in a loop."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 
                UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6
            """)

            # Iterate in batches of 2
            all_rows = []
            batch_size = 2
            while True:
                batch = result.fetchmany(batch_size)
                if not batch:
                    break
                all_rows.extend(batch)

            assert len(all_rows) == 6
            for i, row in enumerate(all_rows, 1):
                assert row["id"] == i
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iterate_rows_list_comprehension(test_config: Config):
    """Test iteration using rows() with list comprehension."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id, 'a' as letter
                UNION ALL SELECT 2, 'b'
                UNION ALL SELECT 3, 'c'
                UNION ALL SELECT 4, 'd'
                UNION ALL SELECT 5, 'e'
            """)

            rows = result.rows()

            # Use list comprehension to filter
            even_ids = [r for r in rows if r["id"] % 2 == 0]
            assert len(even_ids) == 2
            assert even_ids[0]["id"] == 2
            assert even_ids[1]["id"] == 4

            # Use list comprehension to transform
            letters = [r["letter"].upper() for r in rows]
            assert letters == ["A", "B", "C", "D", "E"]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iterate_rows_for_loop(test_config: Config):
    """Test iteration using rows() in a for loop."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as value UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
            """)

            rows = result.rows()

            # Simple for loop
            sum_value = 0
            count = 0
            for row in rows:
                sum_value += row["value"]
                count += 1

            assert count == 5
            assert sum_value == 15  # 1+2+3+4+5
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iterate_with_enumerate(test_config: Config):
    """Test iteration with enumerate to get index."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 'first' as name
                UNION ALL SELECT 'second'
                UNION ALL SELECT 'third'
            """)

            rows = result.rows()

            for idx, row in enumerate(rows):
                if idx == 0:
                    assert row["name"] == "first"
                elif idx == 1:
                    assert row["name"] == "second"
                elif idx == 2:
                    assert row["name"] == "third"
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iterate_reset_and_reiterate(test_config: Config):
    """Test resetting position and re-iterating."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3
            """)

            # First iteration
            first_rows = []
            while True:
                row = result.fetchone()
                if row is None:
                    break
                first_rows.append(row)

            assert len(first_rows) == 3
            assert result.position() == 3

            # Reset and iterate again
            result.reset()
            assert result.position() == 0

            second_rows = []
            while True:
                row = result.fetchone()
                if row is None:
                    break
                second_rows.append(row)

            assert len(second_rows) == 3
            assert first_rows[0]["id"] == second_rows[0]["id"]
            assert first_rows[1]["id"] == second_rows[1]["id"]
            assert first_rows[2]["id"] == second_rows[2]["id"]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iterate_with_zip(test_config: Config):
    """Test iteration combining multiple result sets."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result1 = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3"
            )
            result2 = await conn.query(
                "SELECT 'a' as letter UNION ALL SELECT 'b' UNION ALL SELECT 'c'"
            )

            rows1 = result1.rows()
            rows2 = result2.rows()

            # Zip two result sets
            paired = list(zip(rows1, rows2))
            assert len(paired) == 3
            assert paired[0][0]["id"] == 1 and paired[0][1]["letter"] == "a"
            assert paired[1][0]["id"] == 2 and paired[1][1]["letter"] == "b"
            assert paired[2][0]["id"] == 3 and paired[2][1]["letter"] == "c"
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iterate_with_map(test_config: Config):
    """Test iteration with map function."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id, 'text1' as data
                UNION ALL SELECT 2, 'text2'
                UNION ALL SELECT 3, 'text3'
            """)

            rows = result.rows()

            # Extract just the IDs using map
            ids = list(map(lambda r: r["id"], rows))
            assert ids == [1, 2, 3]

            # Extract and transform using map
            upper_data = list(map(lambda r: r["data"].upper(), rows))
            assert upper_data == ["TEXT1", "TEXT2", "TEXT3"]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iterate_partial_then_fetchall(test_config: Config):
    """Test partial iteration followed by fetchall."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            """)

            # Read first 2 rows manually
            row1 = result.fetchone()
            row2 = result.fetchone()
            assert row1["id"] == 1
            assert row2["id"] == 2
            assert result.position() == 2

            # Fetch remaining rows
            remaining = result.fetchall()
            assert len(remaining) == 2  # Only 2 rows left
            assert remaining[0]["id"] == 3
            assert remaining[1]["id"] == 4
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iterate_with_any(test_config: Config):
    """Test using any() to check if condition exists."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id, NULL as optional_field
                UNION ALL SELECT 2, 'value'
                UNION ALL SELECT 3, NULL
            """)

            rows = result.rows()

            # Check if any row has optional_field
            has_optional = any(r["optional_field"] is not None for r in rows)
            assert has_optional is True

            # Check if any row has specific ID
            has_id_5 = any(r["id"] == 5 for r in rows)
            assert has_id_5 is False
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iterate_with_all(test_config: Config):
    """Test using all() to check if all rows meet condition."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id, 10 as value
                UNION ALL SELECT 2, 20
                UNION ALL SELECT 3, 30
            """)

            rows = result.rows()

            # Check if all IDs are positive
            all_positive = all(r["id"] > 0 for r in rows)
            assert all_positive is True

            # Check if all values are > 5
            all_gt_5 = all(r["value"] > 5 for r in rows)
            assert all_gt_5 is True

            # Check if all values are > 20
            all_gt_20 = all(r["value"] > 20 for r in rows)
            assert all_gt_20 is False
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iterate_with_sorted(test_config: Config):
    """Test sorting result set after fetching."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 3 as id, 'c' as letter
                UNION ALL SELECT 1, 'a'
                UNION ALL SELECT 2, 'b'
            """)

            rows = result.rows()

            # Sort by ID
            sorted_by_id = sorted(rows, key=lambda r: r["id"])
            assert sorted_by_id[0]["id"] == 1
            assert sorted_by_id[1]["id"] == 2
            assert sorted_by_id[2]["id"] == 3

            # Sort by letter in reverse
            sorted_by_letter_desc = sorted(
                rows, key=lambda r: r["letter"], reverse=True
            )
            assert sorted_by_letter_desc[0]["letter"] == "c"
            assert sorted_by_letter_desc[1]["letter"] == "b"
            assert sorted_by_letter_desc[2]["letter"] == "a"
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iterate_with_filter(test_config: Config):
    """Test using filter() to select subset of rows."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 
                UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6
            """)

            rows = result.rows()

            # Filter even numbers
            even = list(filter(lambda r: r["id"] % 2 == 0, rows))
            assert len(even) == 3
            assert even[0]["id"] == 2
            assert even[1]["id"] == 4
            assert even[2]["id"] == 6

            # Filter odd numbers
            odd = list(filter(lambda r: r["id"] % 2 == 1, rows))
            assert len(odd) == 3
            assert odd[0]["id"] == 1
            assert odd[1]["id"] == 3
            assert odd[2]["id"] == 5
    except Exception as e:
        pytest.fail(f"Database not available: {e}")
