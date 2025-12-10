"""
Tests for batch operations in FastMSSQL

This module tests the new batch querying capabilities including:
- Batch query execution
- Batch command execution
- Bulk insert operations
- Performance characteristics
- Error handling

Run with: python -m pytest tests/test_batch_operations.py -v
"""

import pytest
import time
from datetime import datetime, timedelta
import random
import os

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("FastMSSQL wrapper not available", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = os.getenv(
    "FASTMSSQL_TEST_CONNECTION_STRING",
)

class TestBatchQueries:
    """Test batch query execution functionality."""
    
    @pytest.mark.asyncio
    async def test_basic_batch_queries(self):
        """Test basic batch query execution."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Define batch queries
                queries = [
                    ("SELECT 1 as test_value", None),
                    ("SELECT 2 as test_value", None),
                    ("SELECT @P1 as param_value", [42])
                ]
                
                # Execute batch
                results = await conn.query_batch(queries)
                
                # Verify results
                assert len(results) == 3
                
                # Check each result
                rows1 = results[0].rows()
                assert len(rows1) == 1
                assert rows1[0]['test_value'] == 1
                
                rows2 = results[1].rows()
                assert len(rows2) == 1
                assert rows2[0]['test_value'] == 2
                
                rows3 = results[2].rows()
                assert len(rows3) == 1
                assert rows3[0]['param_value'] == 42
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_batch_queries_with_parameters(self):
        """Test batch queries with various parameter types."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                now = datetime.now()
                
                queries = [
                    ("SELECT @P1 as string_val, @P2 as int_val", ["test_string", 123]),
                    ("SELECT @P1 as float_val, @P2 as bool_val", [3.14, True]),
                    ("SELECT @P1 as date_val", [now.isoformat()]),  # Convert datetime to string
                    ("SELECT @P1 as null_val", [None])
                ]
                
                results = await conn.query_batch(queries)
                assert len(results) == 4
                
                # Verify parameter handling
                rows1 = results[0].rows()
                assert len(rows1) == 1
                assert rows1[0]['string_val'] == "test_string"
                assert rows1[0]['int_val'] == 123
                
                rows2 = results[1].rows()
                assert len(rows2) == 1
                assert abs(rows2[0]['float_val'] - 3.14) < 0.001
                
                rows3 = results[2].rows()
                assert len(rows3) == 1
                assert rows3[0]['date_val'] is not None
                
                rows4 = results[3].rows()
                assert len(rows4) == 1
                assert rows4[0]['null_val'] is None
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_empty_batch_queries(self):
        """Test batch execution with empty query list."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                results = await conn.query_batch([])
                assert len(results) == 0
                
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_batch_query_error_handling(self):
        """Test error handling in batch queries."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Include an invalid query in the batch
                queries = [
                    ("SELECT 1 as valid", None),
                    ("SELECT * FROM non_existent_table_xyz", None),  # This should fail
                    ("SELECT 2 as also_valid", None)
                ]
                
                with pytest.raises(Exception):
                    await conn.query_batch(queries)
                    
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestBatchCommands:
    """Test batch command execution functionality."""
    
    @pytest.mark.asyncio
    async def test_basic_batch_commands(self):
        """Test basic batch command execution."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Create test table
                await conn.execute("DROP TABLE IF EXISTS batch_test")
                await conn.execute("""
                    CREATE TABLE batch_test (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        name NVARCHAR(50),
                        value INT
                    )
                """)
                
                try:
                    # Define batch commands
                    commands = [
                        ("INSERT INTO batch_test (name, value) VALUES (@P1, @P2)", ["test1", 10]),
                        ("INSERT INTO batch_test (name, value) VALUES (@P1, @P2)", ["test2", 20]),
                        ("INSERT INTO batch_test (name, value) VALUES (@P1, @P2)", ["test3", 30]),
                        ("UPDATE batch_test SET value = value * 2 WHERE name = @P1", ["test1"])
                    ]
                    
                    # Execute batch
                    results = await conn.execute_batch(commands)
                    
                    # Verify results
                    assert len(results) == 4
                    assert results[0] == 1  # INSERT affected 1 row
                    assert results[1] == 1  # INSERT affected 1 row
                    assert results[2] == 1  # INSERT affected 1 row
                    assert results[3] == 1  # UPDATE affected 1 row
                    
                    # Verify data was inserted correctly
                    verify_result = await conn.query("SELECT COUNT(*) as count FROM batch_test")
                    count_rows = verify_result.rows()
                    assert len(count_rows) == 1
                    assert count_rows[0]['count'] == 3
                    
                finally:
                    # Cleanup
                    await conn.execute("DROP TABLE IF EXISTS batch_test")
                    
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_batch_commands_mixed_operations(self):
        """Test batch commands with mixed operation types."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Create test tables
                await conn.execute("DROP TABLE IF EXISTS batch_test1")
                await conn.execute("DROP TABLE IF EXISTS batch_test2")
                
                await conn.execute("""
                    CREATE TABLE batch_test1 (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        name NVARCHAR(50)
                    )
                """)
                
                await conn.execute("""
                    CREATE TABLE batch_test2 (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        description NVARCHAR(100)
                    )
                """)
                
                try:
                    # Mixed operations
                    commands = [
                        ("INSERT INTO batch_test1 (name) VALUES (@P1)", ["item1"]),
                        ("INSERT INTO batch_test2 (description) VALUES (@P1)", ["desc1"]),
                        ("UPDATE batch_test1 SET name = @P1 WHERE name = @P2", ["updated_item1", "item1"]),
                        ("DELETE FROM batch_test2 WHERE description = @P1", ["desc1"])
                    ]
                    
                    results = await conn.execute_batch(commands)
                    
                    assert len(results) == 4
                    assert all(result >= 0 for result in results)  # All should affect >= 0 rows
                    
                finally:
                    # Cleanup
                    await conn.execute("DROP TABLE IF EXISTS batch_test1")
                    await conn.execute("DROP TABLE IF EXISTS batch_test2")
                    
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestBulkInsert:
    """Test bulk insert functionality."""
    
    @pytest.mark.asyncio
    async def test_basic_bulk_insert(self):
        """Test basic bulk insert operation."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Create test table
                await conn.execute("DROP TABLE IF EXISTS bulk_test")
                await conn.execute("""
                    CREATE TABLE bulk_test (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        name NVARCHAR(50),
                        age INT,
                        salary DECIMAL(10,2)
                    )
                """)
                
                try:
                    # Prepare bulk data
                    columns = ["name", "age", "salary"]
                    data_rows = [
                        ["Alice", 25, 50000.00],
                        ["Bob", 30, 60000.00],
                        ["Charlie", 35, 70000.00],
                        ["Diana", 28, 55000.00],
                        ["Eve", 32, 65000.00]
                    ]
                    
                    # Perform bulk insert
                    rows_inserted = await conn.bulk_insert("bulk_test", columns, data_rows)
                    
                    # Verify insertion
                    assert rows_inserted == 5
                    
                    # Verify data
                    verify_result = await conn.query("SELECT COUNT(*) as count FROM bulk_test")
                    count_rows = verify_result.rows()
                    assert len(count_rows) == 1
                    assert count_rows[0]['count'] == 5
                    
                    # Verify specific data
                    alice_result = await conn.query("SELECT * FROM bulk_test WHERE name = @P1", ["Alice"])
                    alice_rows = alice_result.rows()
                    assert len(alice_rows) == 1
                    assert alice_rows[0]['age'] == 25
                    assert alice_rows[0]['salary'] == 50000.00
                    
                finally:
                    # Cleanup
                    await conn.execute("DROP TABLE IF EXISTS bulk_test")
                    
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_large_bulk_insert(self):
        """Test bulk insert with larger dataset."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Create test table
                await conn.execute("DROP TABLE IF EXISTS bulk_large_test")
                await conn.execute("""
                    CREATE TABLE bulk_large_test (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        name NVARCHAR(50),
                        value INT,
                        created_date DATETIME
                    )
                """)
                
                try:
                    # Generate larger dataset
                    columns = ["name", "value", "created_date"]
                    data_rows = []
                    
                    for i in range(1000):  # 1000 rows
                        data_rows.append([
                            f"item_{i:04d}",
                            random.randint(1, 1000),
                            (datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S")  # SQL Server compatible format
                        ])
                    
                    # Measure performance
                    start_time = time.time()
                    rows_inserted = await conn.bulk_insert("bulk_large_test", columns, data_rows)
                    insert_time = time.time() - start_time
                    
                    # Verify insertion
                    assert rows_inserted == 1000
                    print(f"Bulk insert performance: {rows_inserted / insert_time:.0f} rows/second")
                    
                    # Verify count
                    verify_result = await conn.query("SELECT COUNT(*) as count FROM bulk_large_test")
                    count_rows = verify_result.rows()
                    assert count_rows[0]['count'] == 1000
                    
                finally:
                    # Cleanup
                    await conn.execute("DROP TABLE IF EXISTS bulk_large_test")
                    
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_bulk_insert_error_handling(self):
        """Test error handling in bulk insert operations."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Test with non-existent table
                columns = ["name", "value"]
                data_rows = [["test", 123]]
                
                with pytest.raises(Exception):
                    await conn.bulk_insert("non_existent_table", columns, data_rows)
                
                # Test with mismatched columns/data
                await conn.execute("DROP TABLE IF EXISTS bulk_error_test")
                await conn.execute("""
                    CREATE TABLE bulk_error_test (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        name NVARCHAR(50)
                    )
                """)
                
                try:
                    # Wrong number of columns
                    with pytest.raises(ValueError):
                        await conn.bulk_insert("bulk_error_test", ["name"], [["test", "extra_value"]])
                    
                finally:
                    await conn.execute("DROP TABLE IF EXISTS bulk_error_test")
                    
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestBatchPerformance:
    """Test performance characteristics of batch operations."""
    
    @pytest.mark.asyncio
    async def test_batch_vs_individual_queries(self):
        """Compare performance of batch vs individual queries."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Number of queries to test
                num_queries = 10
                
                # Define test queries
                test_queries = [
                    ("SELECT @P1 as query_num", [i])
                    for i in range(num_queries)
                ]
                
                # Test batch execution
                start_time = time.time()
                batch_results = await conn.query_batch(test_queries)
                batch_time = time.time() - start_time
                
                # Verify batch results
                assert len(batch_results) == num_queries
                for i, result in enumerate(batch_results):
                    rows = result.rows()
                    assert rows[0]['query_num'] == i
                
                # Test individual execution
                start_time = time.time()
                individual_results = []
                for i in range(num_queries):
                    result = await conn.query("SELECT @P1 as query_num", [i])
                    individual_results.append(result)
                individual_time = time.time() - start_time
                
                # Verify individual results
                assert len(individual_results) == num_queries
                for i, result in enumerate(individual_results):
                    rows = result.rows()
                    assert rows[0]['query_num'] == i
                
                # Performance comparison (batch should be faster or similar)
                print(f"Batch time: {batch_time:.3f}s, Individual time: {individual_time:.3f}s")
                if individual_time > batch_time:
                    improvement = ((individual_time - batch_time) / individual_time) * 100
                    print(f"Performance improvement: {improvement:.1f}%")
                    
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_batch_vs_individual_commands(self):
        """Compare performance of batch vs individual commands."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Create test table
                await conn.execute("DROP TABLE IF EXISTS perf_test")
                await conn.execute("""
                    CREATE TABLE perf_test (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        value INT
                    )
                """)
                
                try:
                    num_commands = 20
                    
                    # Test batch execution
                    batch_commands = [
                        ("INSERT INTO perf_test (value) VALUES (@P1)", [i])
                        for i in range(num_commands)
                    ]
                    
                    start_time = time.time()
                    batch_results = await conn.execute_batch(batch_commands)
                    batch_time = time.time() - start_time
                    
                    assert len(batch_results) == num_commands
                    assert sum(batch_results) == num_commands  # Each insert affects 1 row
                    
                    # Clear table for individual test
                    await conn.execute("DELETE FROM perf_test")
                    
                    # Test individual execution
                    start_time = time.time()
                    individual_results = []
                    for i in range(num_commands):
                        result = await conn.execute("INSERT INTO perf_test (value) VALUES (@P1)", [i])
                        individual_results.append(result)
                    individual_time = time.time() - start_time
                    
                    assert len(individual_results) == num_commands
                    assert sum(individual_results) == num_commands
                    
                    # Performance comparison
                    print(f"Batch commands time: {batch_time:.3f}s, Individual commands time: {individual_time:.3f}s")
                    if individual_time > batch_time:
                        improvement = ((individual_time - batch_time) / individual_time) * 100
                        print(f"Performance improvement: {improvement:.1f}%")
                    
                finally:
                    await conn.execute("DROP TABLE IF EXISTS perf_test")
                    
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestBatchEdgeCases:
    """Test edge cases and error conditions for batch operations."""
    
    @pytest.mark.asyncio
    async def test_batch_with_invalid_parameters(self):
        """Test batch operations with invalid parameter formats."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Test invalid query format
                with pytest.raises(ValueError):
                    await conn.query_batch([
                        "SELECT 1",  # Missing tuple format
                    ])
                
                # Test invalid parameter count in tuple
                with pytest.raises(ValueError):
                    await conn.query_batch([
                        ("SELECT 1", None, "extra_param"),  # Too many elements in tuple
                    ])
                    
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
    
    @pytest.mark.asyncio
    async def test_bulk_insert_edge_cases(self):
        """Test bulk insert edge cases."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Create test table
                await conn.execute("DROP TABLE IF EXISTS edge_test")
                await conn.execute("""
                    CREATE TABLE edge_test (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        name NVARCHAR(50)
                    )
                """)
                
                try:
                    # Test empty data
                    result = await conn.bulk_insert("edge_test", ["name"], [])
                    assert result == 0
                    
                    # Test single row
                    result = await conn.bulk_insert("edge_test", ["name"], [["single"]])
                    assert result == 1
                    
                    # Test data with special characters and Unicode
                    special_data = [
                        ["Test with 'quotes'"],
                        ["Test with \"double quotes\""],
                        ["Test with unicode: ñáéíóú"],
                        ["Test with emoji: 🚀"],
                        ["Test with newline\ncharacter"]
                    ]
                    
                    result = await conn.bulk_insert("edge_test", ["name"], special_data)
                    assert result == 5
                    
                finally:
                    await conn.execute("DROP TABLE IF EXISTS edge_test")
                    
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
class TestBatchIntegration:
    """Integration tests for batch operations in real-world scenarios."""
    
    @pytest.mark.asyncio
    async def test_etl_pipeline_simulation(self):
        """Simulate an ETL pipeline using batch operations."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Setup: Create source and target tables
                await conn.execute("DROP TABLE IF EXISTS etl_source")
                await conn.execute("DROP TABLE IF EXISTS etl_target")
                
                await conn.execute("""
                    CREATE TABLE etl_source (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        raw_data NVARCHAR(100),
                        status NVARCHAR(20) DEFAULT 'pending'
                    )
                """)
                
                await conn.execute("""
                    CREATE TABLE etl_target (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        processed_data NVARCHAR(100),
                        processing_date DATETIME
                    )
                """)
                
                try:
                    # Step 1: Load source data using bulk insert
                    source_columns = ["raw_data"]
                    source_data = [
                        [f"raw_record_{i}"] for i in range(100)
                    ]
                    
                    loaded_rows = await conn.bulk_insert("etl_source", source_columns, source_data)
                    assert loaded_rows == 100
                    
                    # Step 2: Extract data using batch queries
                    extract_queries = [
                        ("SELECT * FROM etl_source WHERE status = @P1", ["pending"]),
                        ("SELECT COUNT(*) as total FROM etl_source", None)
                    ]
                    
                    extract_results = await conn.query_batch(extract_queries)
                    pending_data = extract_results[0].rows()
                    total_count_rows = extract_results[1].rows()
                    
                    assert len(pending_data) == 100
                    assert total_count_rows[0]['total'] == 100
                    
                    # Step 3: Transform and load using bulk insert
                    target_columns = ["processed_data", "processing_date"]
                    target_data = [
                        [f"processed_{row['raw_data']}", datetime.now().strftime("%Y-%m-%d %H:%M:%S")]  # SQL Server compatible format
                        for row in pending_data
                    ]
                    
                    processed_rows = await conn.bulk_insert("etl_target", target_columns, target_data)
                    assert processed_rows == 100
                    
                    # Step 4: Update source status using batch commands
                    update_commands = [
                        ("UPDATE etl_source SET status = @P1 WHERE id = @P2", ["processed", row['id']])
                        for row in pending_data[:50]  # Update first 50
                    ]
                    
                    update_results = await conn.execute_batch(update_commands)
                    assert len(update_results) == 50
                    assert sum(update_results) == 50
                    
                    # Verify final state
                    verify_queries = [
                        ("SELECT COUNT(*) as processed_count FROM etl_source WHERE status = @P1", ["processed"]),
                        ("SELECT COUNT(*) as target_count FROM etl_target", None)
                    ]
                    
                    verify_results = await conn.query_batch(verify_queries)
                    processed_count_rows = verify_results[0].rows()
                    target_count_rows = verify_results[1].rows()
                    
                    assert processed_count_rows[0]['processed_count'] == 50
                    assert target_count_rows[0]['target_count'] == 100
                    
                finally:
                    # Cleanup
                    await conn.execute("DROP TABLE IF EXISTS etl_source")
                    await conn.execute("DROP TABLE IF EXISTS etl_target")
                    
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

# --- Setup Utility ---
async def setup_test_table_and_data(conn, table_name="test_fetch_table", num_rows=5):
    """Drops, creates, and populates a test table."""
    await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    await conn.execute(f"""
        CREATE TABLE {table_name} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name NVARCHAR(50)
        )
    """)
    
    # Create an INSERT statement with the specified number of rows
    values = []
    names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack']
    for i in range(num_rows):
        values.append(f"('{names[i]}')")
    
    insert_query = f"INSERT INTO {table_name} (name) VALUES {', '.join(values)}"
    await conn.execute(insert_query)

@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetchone_only():
    """Test sequential fetchone calls and exhaustion of the result set."""
    TABLE_NAME = "test_fetchone_table"
    NUM_ROWS = 3 # Use a smaller, specific number of rows
    
    async with Connection(TEST_CONNECTION_STRING) as conn:
        await setup_test_table_and_data(conn, TABLE_NAME, NUM_ROWS)
        QUERY = f"SELECT id, name FROM {TABLE_NAME} ORDER BY id"
        
        result = await conn.query(QUERY)

        # 1. Fetch the first row
        row1 = result.fetchone()
        assert row1 is not None
        assert row1['name'] == 'Alice'

        # 2. Fetch the second row
        row2 = result.fetchone()
        assert row2 is not None
        assert row2['name'] == 'Bob'

        # 3. Fetch the third (last) row
        row3 = result.fetchone()
        assert row3 is not None
        assert row3['name'] == 'Charlie'

        # 4. Attempt to fetch after all rows are consumed (should be None)
        row_none_1 = result.fetchone()
        assert row_none_1 is None

        # 5. Repeated attempt to fetch after exhaustion
        row_none_2 = result.fetchone()
        assert row_none_2 is None
        
        # Clean up
        await conn.execute(f"DROP TABLE {TABLE_NAME}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetchmany_size_variations():
    """Test fetchmany with sizes: < remaining, = remaining, > remaining, and zero rows."""
    TABLE_NAME = "test_fetchmany_table"
    NUM_ROWS = 7
    
    async with Connection(TEST_CONNECTION_STRING) as conn:
        await setup_test_table_and_data(conn, TABLE_NAME, NUM_ROWS)
        QUERY = f"SELECT id, name FROM {TABLE_NAME} ORDER BY id"
        
        result = await conn.query(QUERY)
        # Total rows: 7

        # 1. fetchmany(3): Less than remaining (7)
        rows_3 = result.fetchmany(3) 
        assert len(rows_3) == 3
        assert rows_3[0]['name'] == 'Alice'
        assert rows_3[2]['name'] == 'Charlie'
        # Remaining: 4 rows ('Diana', 'Eve', 'Frank', 'Grace')

        # 2. fetchmany(2): Progressing the cursor
        rows_2 = result.fetchmany(2)
        assert len(rows_2) == 2
        assert rows_2[0]['name'] == 'Diana'
        assert rows_2[1]['name'] == 'Eve'
        # Remaining: 2 rows ('Frank', 'Grace')

        # 3. fetchmany(5): Greater than remaining (2)
        rows_greater = result.fetchmany(5)
        assert len(rows_greater) == 2
        assert rows_greater[0]['name'] == 'Frank'
        assert rows_greater[1]['name'] == 'Grace'
        # Remaining: 0 rows

        # 4. fetchmany(1): After exhaustion (should be empty list)
        rows_empty = result.fetchmany(1)
        assert rows_empty == []

        # 5. fetchmany(0): Should return an empty list immediately
        rows_zero = result.fetchmany(0)
        assert rows_zero == []
        
        # Clean up
        await conn.execute(f"DROP TABLE {TABLE_NAME}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetchall_and_exhaustion():
    """Test fetchall, ensuring it retrieves all rows and clears the cursor."""
    TABLE_NAME = "test_fetchall_table"
    NUM_ROWS = 4
    
    async with Connection(TEST_CONNECTION_STRING) as conn:
        await setup_test_table_and_data(conn, TABLE_NAME, NUM_ROWS)
        QUERY = f"SELECT id, name FROM {TABLE_NAME} ORDER BY id"
        
        result = await conn.query(QUERY)

        # 1. Test fetchall
        all_rows = result.fetchall()
        assert len(all_rows) == NUM_ROWS
        assert all_rows[0]['name'] == 'Alice'
        assert all_rows[NUM_ROWS - 1]['name'] == 'Diana'

        # 2. Test fetchone after fetchall (Should return None)
        row_none = result.fetchone()
        assert row_none is None

        # 3. Test fetchmany after fetchall (Should return empty list)
        rows_empty = result.fetchmany(2)
        assert rows_empty == []
        
        # Clean up
        await conn.execute(f"DROP TABLE {TABLE_NAME}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetching_edge_cases():
    """Test behavior on an empty result set and mixed fetch operations."""
    TABLE_NAME = "test_fetch_edge_table"
    NUM_ROWS = 5
    
    async with Connection(TEST_CONNECTION_STRING) as conn:
        await setup_test_table_and_data(conn, TABLE_NAME, NUM_ROWS)
        
        # --- Case A: Empty Result Set ---
        EMPTY_QUERY = f"SELECT id, name FROM {TABLE_NAME} WHERE id < 0" # Guaranteed zero rows
        result_empty = await conn.query(EMPTY_QUERY)

        # 1. fetchone on empty
        assert result_empty.fetchone() is None
        
        # 2. fetchmany on empty
        assert result_empty.fetchmany(10) == []

        # 3. fetchall on empty
        assert result_empty.fetchall() == [] # Note: Should still be empty even after fetchone/fetchmany

        # --- Case B: Mixing fetch methods ---
        MIX_QUERY = f"SELECT id, name FROM {TABLE_NAME} ORDER BY id"
        result_mix = await conn.query(MIX_QUERY)
        
        # 4. fetchone (Alice)
        assert result_mix.fetchone()['name'] == 'Alice'
        
        # 5. fetchmany (Bob, Charlie)
        rows_mix = result_mix.fetchmany(2)
        assert len(rows_mix) == 2
        assert rows_mix[0]['name'] == 'Bob'
        
        # 6. fetchall on remaining (Diana, Eve)
        remaining_rows = result_mix.fetchall()
        assert len(remaining_rows) == 2
        assert remaining_rows[0]['name'] == 'Diana'
        assert remaining_rows[1]['name'] == 'Eve'
        
        # Clean up
        await conn.execute(f"DROP TABLE {TABLE_NAME}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_fetchone_fetchmany_fetchall_comprehensive():
    """Test fetchone, fetchmany, and fetchall methods for query results, including edge cases."""
    TABLE_NAME = "test_fetch_methods_comprehensive"
    try:
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # --- Setup: Create Table and Insert Data ---
            await conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            await conn.execute(f"""
                CREATE TABLE {TABLE_NAME} (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(50)
                )
            """)
            
            # Insert 5 rows of test data
            await conn.execute(f"""
                INSERT INTO {TABLE_NAME} (name) VALUES 
                ('Alice'), ('Bob'), ('Charlie'), ('Diana'), ('Eve')
            """)
            
            # Query setup for fetching tests
            QUERY = f"SELECT id, name FROM {TABLE_NAME} ORDER BY id"
            
            # --- Test fetchone, fetchmany, and subsequent fetch calls ---
            
            result = await conn.query(QUERY)
            
            # 1. Test fetchone 
            row1 = result.fetchone()
            assert row1 is not None
            assert row1['name'] == 'Alice'

            # 2. Test fetchmany with size less than remaining rows (size=2)
            rows_2 = result.fetchmany(2)
            assert len(rows_2) == 2
            assert rows_2[0]['name'] == 'Bob' # The 2nd row
            assert rows_2[1]['name'] == 'Charlie' # The 3rd row

            # Remaining rows are 'Diana', 'Eve' (2 rows left)

            # 3. Test fetchmany with size greater than remaining rows (size=5)
            rows_remaining = result.fetchmany(5)
            assert len(rows_remaining) == 2 # Should only return the 2 remaining rows
            assert rows_remaining[0]['name'] == 'Diana'
            assert rows_remaining[1]['name'] == 'Eve'

            # 4. Test fetchone after all rows are consumed (Should return None)
            row_none_after_all = result.fetchone()
            assert row_none_after_all is None

            # 5. Test fetchmany after all rows are consumed (Should return empty list)
            rows_empty_after_all = result.fetchmany(10)
            assert rows_empty_after_all == []

            # --- Test fetchall ---

            # 6. Test fetchall on a fresh query
            result_full = await conn.query(QUERY)
            all_rows = result_full.fetchall()
            assert len(all_rows) == 5
            assert all_rows[0]['name'] == 'Alice'
            assert all_rows[4]['name'] == 'Eve' # Check the last row

            # 7. Test fetchone/fetchmany after fetchall (Should be empty)
            row_none_after_fetchall = result_full.fetchone()
            assert row_none_after_fetchall is None
            rows_empty_after_fetchall = result_full.fetchmany(1)
            assert rows_empty_after_fetchall == []

            # --- Test on an empty result set (No rows in table) ---

            # 8. Setup: Query a table with no matching data
            result_empty = await conn.query(f"SELECT id, name FROM {TABLE_NAME} WHERE 1=0") 
            
            # 9. Test fetchone on empty result
            row_empty_1 = result_empty.fetchone()
            assert row_empty_1 is None

            # 10. Test fetchmany on empty result
            rows_empty_2 = result_empty.fetchmany(5)
            assert rows_empty_2 == []

            # 11. Test fetchall on empty result
            result_empty_2 = await conn.query(f"SELECT id, name FROM {TABLE_NAME} WHERE 1=0")
            all_rows_empty = result_empty_2.fetchall()
            assert all_rows_empty == []

            # --- Clean up ---
            await conn.execute(f"DROP TABLE {TABLE_NAME}")

    except Exception as e:
        pytest.fail(f"Database not available or test failed unexpectedly: {e}")

@pytest.mark.integration
@pytest.mark.asyncio
async def test_readme_fetch_sequence_verification(test_connection_string):
    """
    Verifies the sequential synchronous fetching behavior (fetchone, fetchmany, fetchall)
    on the Result object returned by conn.query(), using the available
    'test_connection_string' fixture to establish the connection.
    """
    TABLE_NAME = "readme_fetch_test_table"
    
    # Establish connection using the available fixture
    async with Connection(test_connection_string) as conn:
        
        # Using a simple try/finally block for cleanup within the test scope
        try:
            # --- Data Setup (Simulating the README's bulk_insert) ---
            await conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            await conn.execute(f"""
                CREATE TABLE {TABLE_NAME} (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(50),
                    age INT
                )
            """)
            columns = ["name", "age"]
            data_rows = [
                ["Alice Johnson", 28],
                ["Bob Smith", 32],
                ["Carol Davis", 25],
                ["David Lee", 35],
                ["Eva Green", 29]
            ]
            # Use the correct table name
            await conn.bulk_insert(TABLE_NAME, columns, data_rows)

            # Query setup: ORDER BY age DESC (Expected order: David, Bob, Eva, Alice, Carol)
            QUERY = f"SELECT name, age FROM {TABLE_NAME} ORDER BY age DESC"
            
            # Execute query to get the Result object
            result = await conn.query(QUERY)
            
            # --- 1. fetchone() ---
            row1 = result.fetchone()
            assert row1 is not None
            assert row1['name'] == 'David Lee'
            
            # --- 2. fetchmany(2) ---
            rows_batch = result.fetchmany(2)
            assert len(rows_batch) == 2
            assert rows_batch[0]['name'] == 'Bob Smith'
            assert rows_batch[1]['name'] == 'Eva Green'
            
            # --- 3. fetchall() ---
            remaining_rows = result.fetchall()
            assert len(remaining_rows) == 2
            assert remaining_rows[0]['name'] == 'Alice Johnson'
            assert remaining_rows[1]['name'] == 'Carol Davis'
            
            # --- 4. Exhaustion Checks ---
            assert result.fetchone() is None
            assert result.fetchmany(10) == []

            # --- 5. Verify execute_batch logic (simple check) ---
            commands = [
                (f"UPDATE {TABLE_NAME} SET age = age + 1 WHERE name = @P1", ["Alice Johnson"]),
            ]
            affected_counts = await conn.execute_batch(commands)
            assert affected_counts[0] == 1
                
        finally:
            # Clean up
            await conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")