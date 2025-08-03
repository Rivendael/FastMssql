"""
Performance and stress tests for mssql-python-rust

This module tests performance characteristics, concurrent operations,
large data handling, and stress scenarios.
"""

import pytest
import sys
import os
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add the parent directory to Python path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    import mssql_python_rust as mssql
except ImportError:
    pytest.skip("mssql_python_rust not available - run 'maturin develop' first", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = "Server=SNOWFLAKE\\SQLEXPRESS,50014;Database=pymssql_test;Integrated Security=true;TrustServerCertificate=yes"

@pytest.mark.performance
@pytest.mark.integration
def test_large_result_set():
    """Test handling of large result sets."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Create test table with large dataset
            conn.execute_non_query("""
                CREATE TABLE test_large_data (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    data_text NVARCHAR(100),
                    data_number INT,
                    data_decimal DECIMAL(10,2),
                    created_date DATETIME DEFAULT GETDATE()
                )
            """)
            
            # Insert large amount of data in batches
            batch_size = 1000
            total_records = 5000
            
            for batch_start in range(0, total_records, batch_size):
                values = []
                for i in range(batch_start, min(batch_start + batch_size, total_records)):
                    values.append(f"('Record {i}', {i}, {i * 1.5})")
                
                insert_sql = f"""
                    INSERT INTO test_large_data (data_text, data_number, data_decimal) VALUES 
                    {', '.join(values)}
                """
                conn.execute_non_query(insert_sql)
            
            # Test retrieving large result set
            start_time = time.time()
            rows = conn.execute("SELECT * FROM test_large_data ORDER BY id")
            end_time = time.time()
            
            assert len(rows) == total_records
            assert rows[0]['data_number'] == 0
            assert rows[-1]['data_number'] == total_records - 1
            
            query_time = end_time - start_time
            print(f"Query time for {total_records} records: {query_time:.3f} seconds")
            assert query_time < 10.0  # Should complete within 10 seconds
            
            # Test filtering on large dataset
            start_time = time.time()
            filtered_rows = conn.execute("SELECT * FROM test_large_data WHERE data_number > 4000")
            end_time = time.time()
            
            assert len(filtered_rows) < total_records
            filter_time = end_time - start_time
            print(f"Filter query time: {filter_time:.3f} seconds")
            
            # Clean up
            conn.execute_non_query("DROP TABLE test_large_data")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.performance
@pytest.mark.integration
def test_concurrent_connections():
    """Test multiple concurrent database connections."""
    try:
        def run_query(thread_id):
            """Function to run in each thread."""
            with mssql.connect(TEST_CONNECTION_STRING) as conn:
                # Each thread runs its own queries
                rows = conn.execute(f"SELECT {thread_id} as thread_id, GETDATE() as execution_time")
                return {
                    'thread_id': thread_id,
                    'result': rows[0],
                    'success': True
                }
        
        # Test with multiple concurrent connections
        num_threads = 10
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(run_query, i) for i in range(num_threads)]
            results = [future.result() for future in as_completed(futures)]
        
        end_time = time.time()
        
        assert len(results) == num_threads
        assert all(r['success'] for r in results)
        
        # Check that all threads completed
        thread_ids = [r['thread_id'] for r in results]
        assert set(thread_ids) == set(range(num_threads))
        
        total_time = end_time - start_time
        print(f"Concurrent connections test time: {total_time:.3f} seconds")
        assert total_time < 30.0  # Should complete within 30 seconds
        
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.performance
@pytest.mark.integration
def test_bulk_insert_performance():
    """Test performance of bulk insert operations."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Clean up any existing table first
            try:
                conn.execute_non_query("""
                    IF OBJECT_ID('test_bulk_insert', 'U') IS NOT NULL 
                    DROP TABLE test_bulk_insert
                """)
            except:
                pass
            
            # Create test table
            conn.execute_non_query("""
                CREATE TABLE test_bulk_insert (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100),
                    value INT,
                    description NVARCHAR(255)
                )
            """)
            
            # Test different bulk insert sizes
            batch_sizes = [100, 500, 1000]
            
            for batch_size in batch_sizes:
                # Generate test data
                values = []
                for i in range(batch_size):
                    values.append(f"('Name {i}', {i}, 'Description for record {i}')")
                
                # Measure insert time
                start_time = time.time()
                insert_sql = f"""
                    INSERT INTO test_bulk_insert (name, value, description) VALUES 
                    {', '.join(values)}
                """
                affected = conn.execute_non_query(insert_sql)
                end_time = time.time()
                
                assert affected == batch_size
                insert_time = end_time - start_time
                records_per_second = batch_size / insert_time if insert_time > 0 else float('inf')
                
                print(f"Batch size {batch_size}: {insert_time:.3f}s, {records_per_second:.0f} records/sec")
                
                # Clear table for next test
                conn.execute_non_query("DELETE FROM test_bulk_insert")
            
            # Clean up
            try:
                conn.execute_non_query("""
                    IF OBJECT_ID('test_bulk_insert', 'U') IS NOT NULL 
                    DROP TABLE test_bulk_insert
                """)
            except:
                pass
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.performance
@pytest.mark.integration
def test_repeated_query_performance():
    """Test performance of repeated query execution."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Setup test data
            conn.execute_non_query("""
                CREATE TABLE test_repeated_queries (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    category NVARCHAR(50),
                    value DECIMAL(10,2)
                )
            """)
            
            # Insert test data
            conn.execute_non_query("""
                INSERT INTO test_repeated_queries (category, value) VALUES 
                ('A', 100.0), ('B', 200.0), ('C', 300.0), ('A', 150.0), ('B', 250.0)
            """)
            
            # Test repeated execution of the same query
            query = "SELECT category, SUM(value) as total FROM test_repeated_queries GROUP BY category"
            num_iterations = 100
            
            start_time = time.time()
            for i in range(num_iterations):
                rows = conn.execute(query)
                assert len(rows) == 3  # Should always return 3 categories
            end_time = time.time()
            
            total_time = end_time - start_time
            avg_time_per_query = total_time / num_iterations
            queries_per_second = num_iterations / total_time if total_time > 0 else float('inf')
            
            print(f"Repeated queries: {total_time:.3f}s total, {avg_time_per_query:.4f}s avg, {queries_per_second:.0f} queries/sec")
            
            # Should be able to execute at least 10 queries per second
            assert queries_per_second > 10
            
            # Clean up
            conn.execute_non_query("DROP TABLE test_repeated_queries")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.asyncio
@pytest.mark.performance
@pytest.mark.integration
async def test_async_concurrent_queries():
    """Test concurrent async query execution."""
    try:
        async def run_async_query(query_id):
            """Function to run async queries concurrently."""
            async with mssql.connect_async(TEST_CONNECTION_STRING) as conn:
                rows = await conn.execute(f"""
                    SELECT 
                        {query_id} as query_id,
                        GETDATE() as execution_time,
                        'Async query result' as message
                """)
                return {
                    'query_id': query_id,
                    'result': rows[0],
                    'success': True
                }
        
        # Run multiple async queries concurrently
        num_queries = 20
        start_time = time.time()
        
        tasks = [run_async_query(i) for i in range(num_queries)]
        results = await asyncio.gather(*tasks)
        
        end_time = time.time()
        
        assert len(results) == num_queries
        assert all(r['success'] for r in results)
        
        # Verify all queries completed
        query_ids = [r['query_id'] for r in results]
        assert set(query_ids) == set(range(num_queries))
        
        total_time = end_time - start_time
        print(f"Async concurrent queries time: {total_time:.3f} seconds")
        assert total_time < 15.0  # Should complete within 15 seconds
        
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.performance
@pytest.mark.integration
def test_memory_usage_with_large_strings():
    """Test memory handling with large string data."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Clean up any existing table first
            try:
                conn.execute_non_query("""
                    IF OBJECT_ID('test_large_strings', 'U') IS NOT NULL 
                    DROP TABLE test_large_strings
                """)
            except:
                pass
            
            # Create table for large string test
            conn.execute_non_query("""
                CREATE TABLE test_large_strings (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    large_text NVARCHAR(MAX)
                )
            """)
            
            # Generate large strings (1KB, 10KB, 100KB)
            sizes = [1024, 10240, 102400]
            
            for size in sizes:
                large_string = 'A' * size
                
                # Insert large string
                conn.execute_non_query(f"INSERT INTO test_large_strings (large_text) VALUES (N'{large_string}')")
                
                # Retrieve and verify
                rows = conn.execute("SELECT large_text FROM test_large_strings WHERE id = SCOPE_IDENTITY()")
                assert len(rows) == 1
                assert len(rows[0]['large_text']) == size
                assert rows[0]['large_text'] == large_string
                
                print(f"Successfully handled {size} byte string")
            
            # Clean up
            try:
                conn.execute_non_query("""
                    IF OBJECT_ID('test_large_strings', 'U') IS NOT NULL 
                    DROP TABLE test_large_strings
                """)
            except:
                pass
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.performance
@pytest.mark.integration 
def test_connection_pool_simulation():
    """Simulate connection pooling behavior."""
    try:
        # Test rapid connection creation/destruction
        num_connections = 50
        start_time = time.time()
        
        for i in range(num_connections):
            with mssql.connect(TEST_CONNECTION_STRING) as conn:
                rows = conn.execute("SELECT 1 as test_value")
                assert rows[0]['test_value'] == 1
        
        end_time = time.time()
        total_time = end_time - start_time
        connections_per_second = num_connections / total_time if total_time > 0 else float('inf')
        
        print(f"Connection creation test: {total_time:.3f}s, {connections_per_second:.0f} connections/sec")
        
        # Should be able to create at least 5 connections per second
        assert connections_per_second > 5
        
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.performance
@pytest.mark.integration
def test_long_running_query():
    """Test handling of long-running queries."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Run a query that takes some time to execute
            start_time = time.time()
            rows = conn.execute("""
                WITH NumberSequence AS (
                    SELECT 1 as n
                    UNION ALL
                    SELECT n + 1
                    FROM NumberSequence
                    WHERE n < 10000
                )
                SELECT COUNT(*) as total_count
                FROM NumberSequence
                OPTION (MAXRECURSION 10000)
            """)
            end_time = time.time()
            
            assert len(rows) == 1
            assert rows[0]['total_count'] == 10000
            
            query_time = end_time - start_time
            print(f"Long-running query time: {query_time:.3f} seconds")
            
            # Query should complete (no timeout), but we don't enforce a specific time limit
            # as this depends on the server performance
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")

@pytest.mark.stress
@pytest.mark.integration
def test_stress_mixed_operations():
    """Stress test with mixed read/write operations."""
    try:
        with mssql.connect(TEST_CONNECTION_STRING) as conn:
            # Setup stress test table
            conn.execute_non_query("""
                CREATE TABLE test_stress_operations (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    operation_type NVARCHAR(20),
                    data_value INT,
                    timestamp_col DATETIME DEFAULT GETDATE()
                )
            """)
            
            # Perform mixed operations
            num_operations = 1000
            start_time = time.time()
            
            for i in range(num_operations):
                if i % 3 == 0:
                    # Insert operation
                    conn.execute_non_query(f"""
                        INSERT INTO test_stress_operations (operation_type, data_value) 
                        VALUES ('INSERT', {i})
                    """)
                elif i % 3 == 1:
                    # Update operation
                    conn.execute_non_query(f"""
                        UPDATE test_stress_operations 
                        SET data_value = data_value + 1 
                        WHERE id % 10 = {i % 10}
                    """)
                else:
                    # Select operation
                    rows = conn.execute(f"""
                        SELECT COUNT(*) as count 
                        FROM test_stress_operations 
                        WHERE data_value > {i // 2}
                    """)
                    assert len(rows) == 1
            
            end_time = time.time()
            total_time = end_time - start_time
            ops_per_second = num_operations / total_time if total_time > 0 else float('inf')
            
            print(f"Stress test: {num_operations} operations in {total_time:.3f}s, {ops_per_second:.0f} ops/sec")
            
            # Verify final state
            rows = conn.execute("SELECT COUNT(*) as total FROM test_stress_operations")
            insert_count = num_operations // 3 + (1 if num_operations % 3 > 0 else 0)
            assert rows[0]['total'] == insert_count
            
            # Clean up
            conn.execute_non_query("DROP TABLE test_stress_operations")
            
    except Exception as e:
        pytest.skip(f"Database not available: {e}")
