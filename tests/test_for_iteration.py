"""
Tests for synchronous iteration over QueryStream using for loops.

This module tests the __iter__ and __next__ protocol that enables
standard Python for loops to work with QueryStream results.
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
async def test_for_loop_basic_iteration(test_config: Config):
    """Test basic for loop iteration over QueryStream."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3"
            )

            rows = []
            for row in result:
                rows.append(row)

            assert len(rows) == 3
            assert rows[0]["id"] == 1
            assert rows[1]["id"] == 2
            assert rows[2]["id"] == 3
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_empty_result(test_config: Config):
    """Test for loop over empty QueryStream."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as id WHERE 0=1")

            rows = []
            for row in result:
                rows.append(row)

            assert len(rows) == 0
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_with_break(test_config: Config):
    """Test for loop with early break."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 
                UNION ALL SELECT 4 UNION ALL SELECT 5
            """)

            rows = []
            for row in result:
                rows.append(row)
                if len(rows) == 2:
                    break

            assert len(rows) == 2
            assert rows[0]["id"] == 1
            assert rows[1]["id"] == 2
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_with_continue(test_config: Config):
    """Test for loop with continue statement."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            """)

            collected_ids = []
            for row in result:
                if row["id"] == 2:
                    continue
                collected_ids.append(row["id"])

            assert len(collected_ids) == 3
            assert 2 not in collected_ids
            assert collected_ids == [1, 3, 4]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_accumulation(test_config: Config):
    """Test for loop with value accumulation."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 10 as value UNION ALL SELECT 20 UNION ALL SELECT 30 UNION ALL SELECT 40
            """)

            total = 0
            for row in result:
                total += row["value"]

            assert total == 100
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_filtering(test_config: Config):
    """Test for loop with filtering condition."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 
                UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6
            """)

            even_rows = []
            for row in result:
                if row["id"] % 2 == 0:
                    even_rows.append(row)

            assert len(even_rows) == 3
            assert even_rows[0]["id"] == 2
            assert even_rows[1]["id"] == 4
            assert even_rows[2]["id"] == 6
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_enumerate(test_config: Config):
    """Test for loop with enumerate."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 'a' as letter UNION ALL SELECT 'b' UNION ALL SELECT 'c'
            """)

            indexed_rows = []
            for idx, row in enumerate(result):
                indexed_rows.append((idx, row["letter"]))

            assert len(indexed_rows) == 3
            assert indexed_rows[0] == (0, "a")
            assert indexed_rows[1] == (1, "b")
            assert indexed_rows[2] == (2, "c")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_many_rows(test_config: Config):
    """Test for loop with many rows."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                WITH cte AS (
                    SELECT 1 as num
                    UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
                    UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
                )
                SELECT num FROM cte
            """)

            count = 0
            for row in result:
                count += 1
                assert 1 <= row["num"] <= 12

            assert count == 12
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_type_checks(test_config: Config):
    """Test for loop maintains type information."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 42 as int_val, 'text' as string_val, 3.14 as float_val
            """)

            for row in result:
                assert isinstance(row["int_val"], int)
                assert isinstance(row["string_val"], str)
                assert isinstance(row["float_val"], (int, float, Decimal))
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_with_null_values(test_config: Config):
    """Test for loop with NULL values."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id, 'value' as data
                UNION ALL SELECT 2, NULL
                UNION ALL SELECT NULL, 'data'
                UNION ALL SELECT 4, NULL
            """)

            null_data_count = 0
            null_id_count = 0
            for row in result:
                if row["data"] is None:
                    null_data_count += 1
                if row["id"] is None:
                    null_id_count += 1

            assert null_data_count == 2
            assert null_id_count == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_reset_and_reiterate(test_config: Config):
    """Test resetting and re-iterating with for loop."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3
            """)

            # First iteration
            count1 = 0
            for row in result:
                count1 += 1
            assert count1 == 3

            # Reset and iterate again
            result.reset()
            count2 = 0
            for row in result:
                count2 += 1
            assert count2 == 3
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_with_exception_handling(test_config: Config):
    """Test for loop with exception handling."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3
            """)

            rows = []
            try:
                for row in result:
                    rows.append(row)
                    if row["id"] == 2:
                        pass
            except Exception:
                pass

            assert len(rows) == 3
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_mixed_with_fetchone(test_config: Config):
    """Test for loop after using fetchone."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            """)

            # Read one row
            row1 = result.fetchone()
            assert row1["id"] == 1

            # Reset and iterate
            result.reset()
            rows = []
            for row in result:
                rows.append(row)

            assert len(rows) == 4
            assert rows[0]["id"] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_list_comprehension(test_config: Config):
    """Test for loop in list comprehension."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            """)

            ids = [row["id"] for row in result]
            assert len(ids) == 4
            assert ids == [1, 2, 3, 4]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_generator(test_config: Config):
    """Test for loop as generator expression."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
            """)

            gen = (row["id"] for row in result)
            ids = list(gen)

            assert len(ids) == 5
            assert ids == [1, 2, 3, 4, 5]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_with_zip(test_config: Config):
    """Test for loop combining multiple results with zip."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result1 = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3"
            )
            result2 = await conn.query(
                "SELECT 'a' as letter UNION ALL SELECT 'b' UNION ALL SELECT 'c'"
            )

            pairs = []
            for row1, row2 in zip(result1, result2):
                pairs.append((row1["id"], row2["letter"]))

            assert len(pairs) == 3
            assert pairs[0] == (1, "a")
            assert pairs[1] == (2, "b")
            assert pairs[2] == (3, "c")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_with_filter_builtin(test_config: Config):
    """Test for loop with Python's filter builtin."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 
                UNION ALL SELECT 4 UNION ALL SELECT 5
            """)

            even_rows = list(filter(lambda r: r["id"] % 2 == 0, result))

            assert len(even_rows) == 2
            assert even_rows[0]["id"] == 2
            assert even_rows[1]["id"] == 4
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_for_loop_with_map_builtin(test_config: Config):
    """Test for loop with Python's map builtin."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("""
                SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3
            """)

            ids = list(map(lambda r: r["id"] * 10, result))

            assert len(ids) == 3
            assert ids == [10, 20, 30]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")
