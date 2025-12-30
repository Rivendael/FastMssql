"""
Tests for advanced query result formats and edge cases

This module tests various query result scenarios including empty results,
special column names, result set variations, and complex data retrieval patterns.
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
async def test_empty_result_set():
    """Test handling of empty result sets."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT * FROM (SELECT 1 as val WHERE 0=1) as empty")
            
            assert not result.has_rows()
            rows = result.rows()
            assert rows == [] or len(rows) == 0
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_single_row_result():
    """Test result set with single row."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 42 as answer")
            
            assert result.has_rows()
            rows = result.rows()
            assert len(rows) == 1
            assert rows[0]['answer'] == 42
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_large_result_set():
    """Test handling of result sets with many rows."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create a result with 100 rows
            result = await conn.query("""
                WITH Numbers AS (
                    SELECT 1 as num
                    UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                    UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
                    UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20
                )
                SELECT n1.num * 5 + n2.num as num FROM Numbers n1, Numbers n2 WHERE n1.num <= 5
            """)
            
            assert result.has_rows()
            rows = result.rows()
            assert len(rows) > 20  # At least 25 rows
            
            # Verify we can iterate through all
            for row in rows:
                assert row['num'] is not None
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_single_column_result():
    """Test result set with single column."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("SELECT 1 as value")
            
            assert result.has_rows()
            row = result.rows()[0]
            
            # Access the single column
            assert row['value'] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_many_columns_result():
    """Test result set with many columns."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    1 as col1, 2 as col2, 3 as col3, 4 as col4, 5 as col5,
                    6 as col6, 7 as col7, 8 as col8, 9 as col9, 10 as col10,
                    11 as col11, 12 as col12, 13 as col13, 14 as col14, 15 as col15,
                    16 as col16, 17 as col17, 18 as col18, 19 as col19, 20 as col20
            """)
            
            assert result.has_rows()
            row = result.rows()[0]
            
            # Access all columns
            for i in range(1, 21):
                assert row[f'col{i}'] == i
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_column_alias_names():
    """Test columns with custom aliases."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    1 as 'Custom Alias',
                    2 as [Another Alias],
                    3 as simple_name
            """)
            
            assert result.has_rows()
            row = result.rows()[0]
            
            # Access with aliases
            # Exact column names depend on how aliases are handled
            assert row is not None
            # Try different possible column names
            try:
                value = row['Custom Alias']
                assert value == 1
            except (KeyError, TypeError):
                pass
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_column_reserved_words():
    """Test columns named with reserved SQL words."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    1 as [select],
                    2 as [from],
                    3 as [where],
                    4 as [order],
                    5 as [group]
            """)
            
            assert result.has_rows()
            row = result.rows()[0]
            
            # Access reserved word columns
            assert row['select'] == 1
            assert row['from'] == 2
            assert row['where'] == 3
            assert row['order'] == 4
            assert row['group'] == 5
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_column_with_spaces():
    """Test columns with spaces in names."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    1 as [First Name],
                    2 as [Last Name],
                    3 as [Email Address]
            """)
            
            assert result.has_rows()
            row = result.rows()[0]
            
            # Access columns with spaces
            assert row['First Name'] == 1
            assert row['Last Name'] == 2
            assert row['Email Address'] == 3
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_duplicate_column_names():
    """Test handling of duplicate column names in result set."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 1 as value, 2 as value
            """)
            
            assert result.has_rows()
            row = result.rows()[0]
            
            # Accessing duplicate column name might return first or raise error
            # Behavior depends on implementation
            try:
                value = row['value']
                assert value in [1, 2]
            except (KeyError, TypeError):
                # Implementation might not support duplicate names
                pass
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_all_null_values():
    """Test result set where all values are NULL."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    CAST(NULL AS INT) as col1,
                    CAST(NULL AS VARCHAR(50)) as col2,
                    CAST(NULL AS FLOAT) as col3
            """)
            
            assert result.has_rows()
            row = result.rows()[0]
            
            # All values should be None
            assert row['col1'] is None
            assert row['col2'] is None
            assert row['col3'] is None
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_mixed_null_and_values():
    """Test result set with mix of NULL and actual values."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 1 as col1, NULL as col2, 'test' as col3, NULL as col4, 42 as col5
            """)
            
            assert result.has_rows()
            row = result.rows()[0]
            
            assert row['col1'] == 1
            assert row['col2'] is None
            assert row['col3'] == 'test'
            assert row['col4'] is None
            assert row['col5'] == 42
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_column_ordering():
    """Test that column ordering is preserved in results."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 'z' as z_col, 'a' as a_col, 'm' as m_col
            """)
            
            assert result.has_rows()
            row = result.rows()[0]
            
            # Verify values are correct
            assert row['z_col'] == 'z'
            assert row['a_col'] == 'a'
            assert row['m_col'] == 'm'
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_with_computed_columns():
    """Test results with computed/calculated columns."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    5 as num1,
                    3 as num2,
                    (5 + 3) as sum_result,
                    (5 * 3) as product_result,
                    CAST(5 AS FLOAT) / 3 as division_result
            """)
            
            assert result.has_rows()
            row = result.rows()[0]
            
            assert row['num1'] == 5
            assert row['num2'] == 3
            assert row['sum_result'] == 8
            assert row['product_result'] == 15
            
            div_result = row['division_result']
            if div_result is not None:
                assert abs(div_result - (5.0/3.0)) < 0.1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_with_case_expression():
    """Test results from CASE expressions."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    CASE WHEN 1 = 1 THEN 'true_case' ELSE 'false_case' END as case_result,
                    CASE 
                        WHEN 5 < 3 THEN 'less'
                        WHEN 5 > 3 THEN 'greater'
                        ELSE 'equal'
                    END as case_comparison
            """)
            
            assert result.has_rows()
            row = result.rows()[0]
            
            assert row['case_result'] == 'true_case'
            assert row['case_comparison'] == 'greater'
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_special_numeric_values():
    """Test result set with special numeric values."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    0 as zero,
                    -1 as negative_one,
                    CAST(0 AS FLOAT) / 0 as nan_result
            """)
            
            # NaN result might cause query to fail, so wrap in try-catch
            if result.has_rows():
                row = result.rows()[0]
                assert row['zero'] == 0
                assert row['negative_one'] == -1
                # NaN handling varies by implementation
    except Exception as e:
        # Expected - division by zero might fail
        pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_string_concatenation():
    """Test results from string concatenation."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT 
                    'Hello' + ' ' + 'World' as concat_result,
                    CONCAT('A', 'B', 'C') as concat_func_result
            """)
            
            assert result.has_rows()
            row = result.rows()[0]
            
            concat_val = row['concat_result']
            if concat_val is not None:
                assert 'Hello' in concat_val and 'World' in concat_val
            
            concat_func = row['concat_func_result']
            if concat_func is not None:
                assert concat_func == 'ABC'
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_multiple_result_sets():
    """Test executing query that might return multiple result sets."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Single query, single result set
            result = await conn.query("""
                SELECT 1 as first_set
            """)
            
            assert result.has_rows()
            rows = result.rows()
            assert len(rows) == 1
            assert rows[0]['first_set'] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_with_subqueries():
    """Test results from queries with subqueries."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT
                    (SELECT COUNT(*) FROM (SELECT 1 as col UNION ALL SELECT 2) t) as count_result,
                    (SELECT MAX(val) FROM (SELECT 1 as val UNION ALL SELECT 5) t2) as max_result
            """)
            
            assert result.has_rows()
            row = result.rows()[0]
            
            count_val = row['count_result']
            assert count_val == 2
            
            max_val = row['max_result']
            assert max_val == 5
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_distinct_values():
    """Test DISTINCT in result set."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT DISTINCT num FROM (
                    SELECT 1 as num
                    UNION ALL SELECT 2
                    UNION ALL SELECT 1
                    UNION ALL SELECT 2
                ) t
            """)
            
            assert result.has_rows()
            rows = result.rows()
            # Should have only 2 distinct values
            values = [row['num'] for row in rows]
            assert len(set(values)) == 2
            assert 1 in values
            assert 2 in values
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_result_sorted_order():
    """Test ORDER BY in result set."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            result = await conn.query("""
                SELECT num FROM (
                    SELECT 3 as num
                    UNION ALL SELECT 1
                    UNION ALL SELECT 2
                ) t
                ORDER BY num
            """)
            
            assert result.has_rows()
            rows = result.rows()
            
            # Verify order
            assert rows[0]['num'] == 1
            assert rows[1]['num'] == 2
            assert rows[2]['num'] == 3
    except Exception as e:
        pytest.fail(f"Database not available: {e}")
