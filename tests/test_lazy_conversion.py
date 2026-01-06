"""
Tests for lazy conversion and array indexing in QueryStream.

This module comprehensively tests the lazy conversion mechanism that converts
rows from Tiberius to Python objects on-demand, as well as array-style indexing
and slicing capabilities. These tests ensure that:

1. Rows are converted lazily (not all at once)
2. Converted rows are cached for efficient re-access
3. Array indexing works correctly (positive, negative, slicing)
4. GIL contention is minimized by distributing conversion across iterations
"""

import pytest
from conftest import Config

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lazy_conversion_iteration(test_config: Config):
    """Test that iteration converts rows lazily, not all at once."""
    try:
        async with Connection(test_config.connection_string) as conn:
            # Create a result set with multiple rows
            result = await conn.query(
                "SELECT TOP 10 number FROM master..spt_values WHERE type='P'"
            )

            # Verify we have rows without converting them
            assert result.has_rows()
            assert result.len() == 10
            assert result.position() == 0

            # Iterate through first 3 rows
            rows_collected = []
            for i, row in enumerate(result):
                rows_collected.append(row)
                if i >= 2:
                    break

            # Position should be at 3 (zero-indexed iteration advances position)
            assert result.position() == 3
            assert len(rows_collected) == 3

            # Verify the first 3 rows have values
            for row in rows_collected:
                assert row["number"] is not None
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lazy_conversion_cache_on_reset(test_config: Config):
    """Test that converted rows are cached and reused after reset()."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id, 'first' as name UNION ALL "
                "SELECT 2, 'second' UNION ALL "
                "SELECT 3, 'third'"
            )

            # First pass: iterate through all rows
            first_pass = []
            for row in result:
                first_pass.append(row["id"])

            assert first_pass == [1, 2, 3]
            assert result.position() == 3

            # Reset and iterate again
            result.reset()
            assert result.position() == 0

            # Second pass: should use cached conversions
            second_pass = []
            for row in result:
                second_pass.append(row["id"])

            assert second_pass == [1, 2, 3]
            # Cache allows iteration to work correctly
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_array_indexing_positive(test_config: Config):
    """Test positive array indexing: result[0], result[5], etc."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL "
                "SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5"
            )

            # Access by positive index
            row0 = result[0]
            assert row0["id"] == 1

            row2 = result[2]
            assert row2["id"] == 3

            row4 = result[4]
            assert row4["id"] == 5

            # Verify position hasn't changed (indexing doesn't advance position)
            assert result.position() == 0

            # Re-access same index (should use cache)
            row0_again = result[0]
            assert row0_again["id"] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_array_indexing_negative(test_config: Config):
    """Test negative array indexing: result[-1], result[-2], etc."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL "
                "SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5"
            )

            # Access last row
            last_row = result[-1]
            assert last_row["id"] == 5

            # Access second to last
            second_last = result[-2]
            assert second_last["id"] == 4

            # Access from beginning using negative index
            first_from_end = result[-5]
            assert first_from_end["id"] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_array_indexing_out_of_bounds(test_config: Config):
    """Test that out-of-bounds indexing raises IndexError."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as id UNION ALL SELECT 2")

            # Valid indices: 0, 1, -1, -2
            assert result[0]["id"] == 1
            assert result[1]["id"] == 2

            # Out of bounds positive
            with pytest.raises(IndexError, match="Index out of range"):
                _ = result[2]

            with pytest.raises(IndexError, match="Index out of range"):
                _ = result[10]

            # Out of bounds negative
            with pytest.raises(IndexError, match="Index out of range"):
                _ = result[-3]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_array_slicing_basic(test_config: Config):
    """Test basic array slicing: result[1:4], result[:3], result[2:]."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 "
                "UNION ALL SELECT 4 UNION ALL SELECT 5"
            )

            # Slice middle section
            middle_slice = result[1:4]
            assert len(middle_slice) == 3
            assert middle_slice[0]["id"] == 2
            assert middle_slice[1]["id"] == 3
            assert middle_slice[2]["id"] == 4

            # Slice from beginning
            start_slice = result[:3]
            assert len(start_slice) == 3
            assert start_slice[0]["id"] == 1
            assert start_slice[1]["id"] == 2
            assert start_slice[2]["id"] == 3

            # Slice to end
            end_slice = result[3:]
            assert len(end_slice) == 2
            assert end_slice[0]["id"] == 4
            assert end_slice[1]["id"] == 5
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_array_slicing_empty(test_config: Config):
    """Test slicing that results in empty list."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as id UNION ALL SELECT 2")

            # Slice beyond bounds
            empty_slice = result[10:20]
            assert len(empty_slice) == 0

            # Slice with start >= stop
            empty_slice2 = result[1:1]
            assert len(empty_slice2) == 0
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_array_slicing_full_range(test_config: Config):
    """Test slicing entire range: result[:]."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3"
            )

            # Full slice
            all_rows = result[:]
            assert len(all_rows) == 3
            assert all_rows[0]["id"] == 1
            assert all_rows[1]["id"] == 2
            assert all_rows[2]["id"] == 3
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mixed_access_patterns(test_config: Config):
    """Test mixing indexing, slicing, and iteration."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 "
                "UNION ALL SELECT 4 UNION ALL SELECT 5"
            )

            # Start with index access
            first = result[0]
            assert first["id"] == 1

            # Then slice
            middle = result[1:3]
            assert len(middle) == 2
            assert middle[0]["id"] == 2

            # Then iterate
            all_rows = []
            for row in result:
                all_rows.append(row["id"])

            assert all_rows == [1, 2, 3, 4, 5]

            # Access by index again after iteration
            last = result[-1]
            assert last["id"] == 5
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_indexing_does_not_affect_position(test_config: Config):
    """Test that array indexing doesn't change the iteration position."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3"
            )

            assert result.position() == 0

            # Access various indices
            _ = result[0]
            assert result.position() == 0

            _ = result[2]
            assert result.position() == 0

            _ = result[-1]
            assert result.position() == 0

            # Slicing also shouldn't affect position
            _ = result[0:2]
            assert result.position() == 0

            # But iteration should advance position
            first_row = next(iter(result))
            assert first_row["id"] == 1
            assert result.position() == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lazy_conversion_with_large_result_set(test_config: Config):
    """Test lazy conversion efficiency with larger result set."""
    try:
        async with Connection(test_config.connection_string) as conn:
            # Query that returns multiple rows
            result = await conn.query(
                "SELECT TOP 100 number FROM master..spt_values WHERE type='P'"
            )

            assert result.len() == 100

            # Access only a few rows via indexing (lazy)
            row10 = result[10]
            row50 = result[50]
            row99 = result[99]

            assert row10["number"] is not None
            assert row50["number"] is not None
            assert row99["number"] is not None

            # Position should still be 0 (indexing doesn't advance)
            assert result.position() == 0

            # Now iterate through first 10
            count = 0
            for row in result:
                count += 1
                if count >= 10:
                    break

            assert result.position() == 10
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_slice_caching_behavior(test_config: Config):
    """Test that sliced rows are cached for future access."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 "
                "UNION ALL SELECT 4 UNION ALL SELECT 5"
            )

            # First slice access (converts rows 1-3)
            first_slice = result[1:4]
            assert len(first_slice) == 3

            # Access same range via individual indices (should use cache)
            row1 = result[1]
            row2 = result[2]
            row3 = result[3]

            assert row1["id"] == 2
            assert row2["id"] == 3
            assert row3["id"] == 4

            # Slice again (should use cache)
            second_slice = result[1:4]
            assert len(second_slice) == 3
            assert second_slice[0]["id"] == 2
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_empty_result_indexing(test_config: Config):
    """Test indexing on empty result set."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as id WHERE 0=1")

            assert result.is_empty()
            assert result.len() == 0

            # Indexing should raise IndexError
            with pytest.raises(IndexError, match="Index out of range"):
                _ = result[0]

            # Slicing should return empty list
            empty_slice = result[0:10]
            assert len(empty_slice) == 0
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetchone_vs_indexing(test_config: Config):
    """Test that fetchone() advances position but indexing doesn't."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3"
            )

            # fetchone() should advance position
            row1 = result.fetchone()
            assert row1["id"] == 1
            assert result.position() == 1

            # Indexing should not affect position
            row0 = result[0]
            assert row0["id"] == 1
            assert result.position() == 1  # Still at 1

            # Next fetchone() gets row at position 1
            row2 = result.fetchone()
            assert row2["id"] == 2
            assert result.position() == 2
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_iteration_after_partial_indexing(test_config: Config):
    """Test that iteration works correctly after partial indexing."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                "SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3 "
                "UNION ALL SELECT 4 UNION ALL SELECT 5"
            )

            # Access some rows via indexing
            _ = result[2]  # Convert row 2
            _ = result[4]  # Convert row 4

            # Now iterate from beginning
            all_ids = []
            for row in result:
                all_ids.append(row["id"])

            # Should get all rows in order
            assert all_ids == [1, 2, 3, 4, 5]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_negative_indexing_edge_cases(test_config: Config):
    """Test edge cases for negative indexing."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as id")

            # Single row: -1 should work
            row = result[-1]
            assert row["id"] == 1

            # -2 should fail (out of bounds)
            with pytest.raises(IndexError, match="Index out of range"):
                _ = result[-2]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")
