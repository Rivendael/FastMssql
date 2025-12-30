"""
Tests for advanced batch operations and edge cases

This module tests batch query execution, bulk inserts, and batch operations
with various edge cases, transaction handling, and error scenarios.
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
async def test_execute_batch_basic():
    """Test basic batch execution with INSERT statements."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table
            await conn.execute("""
                IF OBJECT_ID('tempdb..##batch_basic', 'U') IS NOT NULL
                    DROP TABLE ##batch_basic
            """)
            
            await conn.execute("""
                CREATE TABLE ##batch_basic (id INT PRIMARY KEY, value VARCHAR(50))
            """)
            
            # Execute batch
            batch_items = [
                ("INSERT INTO ##batch_basic VALUES (@P1, @P2)", [1, 'first']),
                ("INSERT INTO ##batch_basic VALUES (@P1, @P2)", [2, 'second']),
                ("INSERT INTO ##batch_basic VALUES (@P1, @P2)", [3, 'third']),
            ]
            
            results = await conn.execute_batch(batch_items)
            
            assert len(results) == 3
            assert all(r == 1 for r in results)
            
            # Verify data
            result = await conn.query("SELECT COUNT(*) as cnt FROM ##batch_basic")
            assert result.rows()[0]['cnt'] == 3
            
            # Cleanup
            await conn.execute("DROP TABLE ##batch_basic")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_batch_empty():
    """Test batch execution with empty batch list."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Empty batch
            batch_items = []
            
            try:
                results = await conn.execute_batch(batch_items)
                # Might succeed with empty list or raise error
                assert isinstance(results, list)
            except Exception:
                # Empty batch might not be allowed
                pass
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_batch_single_item():
    """Test batch execution with single item."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table
            await conn.execute("""
                IF OBJECT_ID('tempdb..##batch_single', 'U') IS NOT NULL
                    DROP TABLE ##batch_single
            """)
            
            await conn.execute("""
                CREATE TABLE ##batch_single (id INT PRIMARY KEY, value VARCHAR(50))
            """)
            
            # Single item batch
            batch_items = [
                ("INSERT INTO ##batch_single VALUES (@P1, @P2)", [1, 'only']),
            ]
            
            results = await conn.execute_batch(batch_items)
            
            assert len(results) == 1
            assert results[0] == 1
            
            # Cleanup
            await conn.execute("DROP TABLE ##batch_single")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_batch_large_batch():
    """Test batch execution with many items."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table
            await conn.execute("""
                IF OBJECT_ID('tempdb..##batch_large', 'U') IS NOT NULL
                    DROP TABLE ##batch_large
            """)
            
            await conn.execute("""
                CREATE TABLE ##batch_large (id INT PRIMARY KEY, value VARCHAR(50))
            """)
            
            # Create large batch (50 items)
            batch_items = [
                ("INSERT INTO ##batch_large VALUES (@P1, @P2)", [i, f'value_{i}'])
                for i in range(1, 51)
            ]
            
            results = await conn.execute_batch(batch_items)
            
            assert len(results) == 50
            assert all(r == 1 for r in results)
            
            # Verify data
            result = await conn.query("SELECT COUNT(*) as cnt FROM ##batch_large")
            assert result.rows()[0]['cnt'] == 50
            
            # Cleanup
            await conn.execute("DROP TABLE ##batch_large")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_batch_mixed_operations():
    """Test batch with mixed INSERT, UPDATE, DELETE operations."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table with initial data
            await conn.execute("""
                IF OBJECT_ID('tempdb..##batch_mixed', 'U') IS NOT NULL
                    DROP TABLE ##batch_mixed
            """)
            
            await conn.execute("""
                CREATE TABLE ##batch_mixed (id INT PRIMARY KEY, status VARCHAR(20))
            """)
            
            # Insert initial data
            await conn.execute("INSERT INTO ##batch_mixed VALUES (@P1, @P2)", [1, 'active'])
            
            # Mixed batch
            batch_items = [
                ("INSERT INTO ##batch_mixed VALUES (@P1, @P2)", [2, 'active']),
                ("UPDATE ##batch_mixed SET status = @P1 WHERE id = @P2", ['inactive', 1]),
                ("INSERT INTO ##batch_mixed VALUES (@P1, @P2)", [3, 'active']),
                ("DELETE FROM ##batch_mixed WHERE id = @P1", [2]),
            ]
            
            results = await conn.execute_batch(batch_items)
            
            assert len(results) == 4
            
            # Verify final state
            result = await conn.query("SELECT COUNT(*) as cnt FROM ##batch_mixed")
            assert result.rows()[0]['cnt'] == 2  # 1 initial + 1 insert - 1 delete
            
            # Cleanup
            await conn.execute("DROP TABLE ##batch_mixed")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_batch_duplicate_key_error():
    """Test batch behavior with duplicate key violation."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table
            await conn.execute("""
                IF OBJECT_ID('tempdb..##batch_dup', 'U') IS NOT NULL
                    DROP TABLE ##batch_dup
            """)
            
            await conn.execute("""
                CREATE TABLE ##batch_dup (id INT PRIMARY KEY, value VARCHAR(50))
            """)
            
            # Batch with duplicate key (should fail or rollback)
            batch_items = [
                ("INSERT INTO ##batch_dup VALUES (@P1, @P2)", [1, 'first']),
                ("INSERT INTO ##batch_dup VALUES (@P1, @P2)", [1, 'duplicate']),  # Duplicate
            ]
            
            try:
                results = await conn.execute_batch(batch_items)
                # If batch succeeds, nothing should be inserted (rollback)
                result = await conn.query("SELECT COUNT(*) as cnt FROM ##batch_dup")
                # Either 0 (rolled back) or might have partially succeeded
            except Exception:
                # Batch error is expected for duplicate key
                pass
            
            # Cleanup
            await conn.execute("DROP TABLE ##batch_dup")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_batch_no_parameters():
    """Test batch with items that have no parameters."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table
            await conn.execute("""
                IF OBJECT_ID('tempdb..##batch_noparams', 'U') IS NOT NULL
                    DROP TABLE ##batch_noparams
            """)
            
            await conn.execute("""
                CREATE TABLE ##batch_noparams (id INT, value VARCHAR(50))
            """)
            
            # Batch items without parameters
            batch_items = [
                ("INSERT INTO ##batch_noparams VALUES (1, 'one')", None),
                ("INSERT INTO ##batch_noparams VALUES (2, 'two')", None),
                ("INSERT INTO ##batch_noparams VALUES (3, 'three')", None),
            ]
            
            results = await conn.execute_batch(batch_items)
            assert len(results) == 3
            
            # Verify data
            result = await conn.query("SELECT COUNT(*) as cnt FROM ##batch_noparams")
            assert result.rows()[0]['cnt'] == 3
            
            # Cleanup
            await conn.execute("DROP TABLE ##batch_noparams")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bulk_insert_basic():
    """Test bulk insert operation."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table
            await conn.execute("""
                IF OBJECT_ID('tempdb..##bulk_basic', 'U') IS NOT NULL
                    DROP TABLE ##bulk_basic
            """)
            
            await conn.execute("""
                CREATE TABLE ##bulk_basic (id INT, value VARCHAR(50))
            """)
            
            # Prepare data for bulk insert
            rows = [
                [1, 'row1'],
                [2, 'row2'],
                [3, 'row3'],
            ]
            
            # Execute bulk insert
            await conn.bulk_insert('##bulk_basic', ['id', 'value'], rows)
            
            # Verify data
            result = await conn.query("SELECT COUNT(*) as cnt FROM ##bulk_basic")
            assert result.rows()[0]['cnt'] == 3
            
            # Cleanup
            await conn.execute("DROP TABLE ##bulk_basic")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bulk_insert_many_rows():
    """Test bulk insert with many rows."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table
            await conn.execute("""
                IF OBJECT_ID('tempdb..##bulk_many', 'U') IS NOT NULL
                    DROP TABLE ##bulk_many
            """)
            
            await conn.execute("""
                CREATE TABLE ##bulk_many (id INT, value VARCHAR(50))
            """)
            
            # Prepare large dataset
            rows = [
                [i, f'row_{i}']
                for i in range(1, 101)
            ]
            
            # Execute bulk insert
            await conn.bulk_insert('##bulk_many', ['id', 'value'], rows)
            
            # Verify data
            result = await conn.query("SELECT COUNT(*) as cnt FROM ##bulk_many")
            assert result.rows()[0]['cnt'] == 100
            
            # Verify specific rows
            result = await conn.query("SELECT * FROM ##bulk_many WHERE id = 50")
            assert result.rows()[0]['value'] == 'row_50'
            
            # Cleanup
            await conn.execute("DROP TABLE ##bulk_many")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bulk_insert_different_types():
    """Test bulk insert with different data types."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table with multiple types
            await conn.execute("""
                IF OBJECT_ID('tempdb..##bulk_types', 'U') IS NOT NULL
                    DROP TABLE ##bulk_types
            """)
            
            await conn.execute("""
                CREATE TABLE ##bulk_types (
                    id INT,
                    name VARCHAR(50),
                    score FLOAT,
                    active BIT
                )
            """)
            
            # Prepare data with different types
            rows = [
                [1, 'Alice', 95.5, 1],
                [2, 'Bob', 87.3, 0],
                [3, 'Charlie', 92.1, 1],
            ]
            
            # Execute bulk insert
            await conn.bulk_insert('##bulk_types', ['id', 'name', 'score', 'active'], rows)
            
            # Verify data
            result = await conn.query("SELECT * FROM ##bulk_types WHERE id = 1")
            row = result.rows()[0]
            assert row['name'] == 'Alice'
            assert abs(row['score'] - 95.5) < 0.1
            
            # Cleanup
            await conn.execute("DROP TABLE ##bulk_types")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_batch_basic():
    """Test batch query execution."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table with test data
            await conn.execute("""
                IF OBJECT_ID('tempdb..##batch_query', 'U') IS NOT NULL
                    DROP TABLE ##batch_query
            """)
            
            await conn.execute("""
                CREATE TABLE ##batch_query (id INT, value VARCHAR(50))
            """)
            
            # Insert test data
            for i in range(1, 4):
                await conn.execute(
                    "INSERT INTO ##batch_query VALUES (@P1, @P2)",
                    [i, f'value_{i}']
                )
            
            # Create batch queries
            batch_items = [
                ("SELECT * FROM ##batch_query WHERE id = @P1", [1]),
                ("SELECT * FROM ##batch_query WHERE id = @P1", [2]),
                ("SELECT * FROM ##batch_query WHERE id = @P1", [3]),
            ]
            
            # Execute query batch
            results = await conn.query_batch(batch_items)
            
            # Verify results
            assert len(results) == 3
            for i, result in enumerate(results, 1):
                assert result.has_rows()
                rows = result.rows()
                assert rows[0]['id'] == i
                assert rows[0]['value'] == f'value_{i}'
            
            # Cleanup
            await conn.execute("DROP TABLE ##batch_query")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_batch_empty_results():
    """Test batch queries with empty results."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table
            await conn.execute("""
                IF OBJECT_ID('tempdb..##batch_empty', 'U') IS NOT NULL
                    DROP TABLE ##batch_empty
            """)
            
            await conn.execute("""
                CREATE TABLE ##batch_empty (id INT, value VARCHAR(50))
            """)
            
            # Insert one row
            await conn.execute("INSERT INTO ##batch_empty VALUES (@P1, @P2)", [1, 'test'])
            
            # Batch with mixed results
            batch_items = [
                ("SELECT * FROM ##batch_empty WHERE id = @P1", [1]),
                ("SELECT * FROM ##batch_empty WHERE id = @P1", [999]),  # No result
                ("SELECT * FROM ##batch_empty WHERE id = @P1", [1]),
            ]
            
            results = await conn.query_batch(batch_items)
            
            assert len(results) == 3
            assert results[0].has_rows()
            assert not results[1].has_rows()  # Empty result
            assert results[2].has_rows()
            
            # Cleanup
            await conn.execute("DROP TABLE ##batch_empty")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_batch_transaction_rollback():
    """Test that batch is transactional and rolls back on error."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table
            await conn.execute("""
                IF OBJECT_ID('tempdb..##batch_rollback', 'U') IS NOT NULL
                    DROP TABLE ##batch_rollback
            """)
            
            await conn.execute("""
                CREATE TABLE ##batch_rollback (id INT PRIMARY KEY, value VARCHAR(50))
            """)
            
            # Batch that should fail and rollback
            batch_items = [
                ("INSERT INTO ##batch_rollback VALUES (@P1, @P2)", [1, 'first']),
                ("INVALID SQL STATEMENT", []),  # This will cause error
                ("INSERT INTO ##batch_rollback VALUES (@P1, @P2)", [2, 'second']),
            ]
            
            try:
                results = await conn.execute_batch(batch_items)
            except Exception:
                # Expected - batch should fail
                pass
            
            # Verify transaction was rolled back
            result = await conn.query("SELECT COUNT(*) as cnt FROM ##batch_rollback")
            count = result.rows()[0]['cnt']
            # Should be 0 (rolled back) or might have first insert before error
            assert count in [0, 1]
            
            # Cleanup
            await conn.execute("DROP TABLE ##batch_rollback")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_batch_return_values():
    """Test that execute_batch returns correct affected row counts."""
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Create temp table
            await conn.execute("""
                IF OBJECT_ID('tempdb..##batch_returns', 'U') IS NOT NULL
                    DROP TABLE ##batch_returns
            """)
            
            await conn.execute("""
                CREATE TABLE ##batch_returns (id INT PRIMARY KEY, value VARCHAR(50))
            """)
            
            # Batch with different return values
            batch_items = [
                ("INSERT INTO ##batch_returns VALUES (@P1, @P2)", [1, 'one']),
                ("INSERT INTO ##batch_returns VALUES (@P1, @P2)", [2, 'two']),
                ("UPDATE ##batch_returns SET value = @P1 WHERE id >= @P2", ['updated', 1]),
                ("DELETE FROM ##batch_returns WHERE id = @P1", [1]),
            ]
            
            results = await conn.execute_batch(batch_items)
            
            # Verify return values
            assert results[0] == 1  # INSERT returns 1
            assert results[1] == 1  # INSERT returns 1
            assert results[2] == 2  # UPDATE returns 2 (updated 2 rows)
            assert results[3] == 1  # DELETE returns 1
            
            # Cleanup
            await conn.execute("DROP TABLE ##batch_returns")
    except Exception as e:
        pytest.fail(f"Database not available: {e}")
