#!/usr/bin/env python3
"""
Async usage example for mssql-python-rust

This example demonstrates how to use the async features of the library.
"""

import asyncio
import os
import sys
import time

# Add the parent directory to Python path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    import mssql_python_rust as mssql
except ImportError as e:
    print(f"Error importing mssql_python_rust: {e}")
    print("Make sure you've built the extension with 'maturin develop'")
    sys.exit(1)

async def basic_async_example(connection_string: str):
    """Basic async connection and query example."""
    print("\n=== Basic Async Example ===")
    
    # Using async context manager
    async with mssql.connect_async(connection_string) as conn:
        # Execute a simple query
        rows = await conn.execute("SELECT @@VERSION as sql_version")
        for row in rows:
            print(f"SQL Server Version: {row['sql_version'][:100]}...")
        
        # Get current time
        result = await conn.execute("SELECT GETDATE() as current_time")
        if result:
            print(f"Current time: {result[0]['current_time']}")

async def concurrent_queries_example(connection_string: str):
    """Example showing concurrent query execution."""
    print("\n=== Concurrent Queries Example ===")
    
    queries = [
        "SELECT 'Query 1' as name, 42 as value",
        "SELECT 'Query 2' as name, 84 as value", 
        "SELECT 'Query 3' as name, 126 as value",
        "SELECT 'Query 4' as name, 168 as value",
        "SELECT 'Query 5' as name, 210 as value"
    ]
    
    async def execute_single_query(query: str, query_num: int):
        async with mssql.connect_async(connection_string) as conn:
            start_time = time.time()
            rows = await conn.execute(query)
            end_time = time.time()
            duration = end_time - start_time
            if rows:
                result = rows[0]
                print(f"Query {query_num}: {result['name']} = {result['value']} (took {duration:.3f}s)")
    
    # Execute all queries concurrently
    start_time = time.time()
    tasks = [execute_single_query(query, i+1) for i, query in enumerate(queries)]
    await asyncio.gather(*tasks)
    end_time = time.time()
    
    total_duration = end_time - start_time
    print(f"All 5 queries completed concurrently in {total_duration:.3f}s")

async def performance_comparison(connection_string: str, num_queries: int = 20):
    """Compare sync vs async performance."""
    print(f"\n=== Performance Comparison: {num_queries} queries ===")
    
    # Sync version
    print("Running synchronous queries...")
    start_time = time.time()
    with mssql.connect(connection_string) as conn:
        for i in range(num_queries):
            rows = conn.execute(f"SELECT {i} as query_number")
    sync_duration = time.time() - start_time
    
    # Async version (sequential)
    print("Running asynchronous queries (sequential)...")
    start_time = time.time()
    async with mssql.connect_async(connection_string) as conn:
        for i in range(num_queries):
            rows = await conn.execute(f"SELECT {i} as query_number")
    async_sequential_duration = time.time() - start_time
    
    # Async version (concurrent)
    print("Running asynchronous queries (concurrent)...")
    async def single_query(i: int):
        async with mssql.connect_async(connection_string) as conn:
            return await conn.execute(f"SELECT {i} as query_number")
    
    start_time = time.time()
    tasks = [single_query(i) for i in range(num_queries)]
    await asyncio.gather(*tasks)
    async_concurrent_duration = time.time() - start_time
    
    print(f"Synchronous:           {sync_duration:.3f}s ({num_queries/sync_duration:.1f} queries/sec)")
    print(f"Async (sequential):    {async_sequential_duration:.3f}s ({num_queries/async_sequential_duration:.1f} queries/sec)")
    print(f"Async (concurrent):    {async_concurrent_duration:.3f}s ({num_queries/async_concurrent_duration:.1f} queries/sec)")
    print(f"Async concurrent speedup: {sync_duration/async_concurrent_duration:.1f}x faster")

async def error_handling_example(connection_string: str):
    """Example of async error handling."""
    print("\n=== Async Error Handling Example ===")
    
    try:
        async with mssql.connect_async(connection_string) as conn:
            # This will cause an error
            rows = await conn.execute("SELECT * FROM non_existent_table")
    except Exception as e:
        print(f"Expected error caught: {e}")

async def bulk_operations_example(connection_string: str):
    """Example of bulk async operations."""
    print("\n=== Bulk Async Operations Example ===")
    
    # Create multiple connections and execute different operations
    operations = [
        "SELECT 'Operation A' as name, GETDATE() as timestamp",
        "SELECT 'Operation B' as name, @@SPID as process_id",
        "SELECT 'Operation C' as name, DB_NAME() as database_name",
        "SELECT 'Operation D' as name, USER_NAME() as user_name",
        "SELECT 'Operation E' as name, @@SERVERNAME as server_name"
    ]
    
    async def execute_operation(operation: str):
        async with mssql.connect_async(connection_string) as conn:
            return await conn.execute(operation)
    
    # Execute all operations concurrently
    results = await asyncio.gather(*[execute_operation(op) for op in operations])
    
    for i, result in enumerate(results):
        if result:
            row = result[0]
            print(f"{row['name']}: {list(row.values())[1]}")

async def main():
    """Main async function."""
    
    # Connection string - adjust as needed for your environment
    connection_string = "Server=localhost;Database=master;Integrated Security=true"
    
    print(f"mssql-python-rust Async Examples")
    print(f"Version: {mssql.version()}")
    print(f"Connecting to: {connection_string}")
    
    try:
        # Test basic connectivity first
        print("\n=== Async Connectivity Test ===")
        async with mssql.connect_async(connection_string) as conn:
            rows = await conn.execute("SELECT 'Async connection successful' as status")
            if rows:
                print(f"Status: {rows[0]['status']}")
        
        # Run various async examples
        await basic_async_example(connection_string)
        await concurrent_queries_example(connection_string)
        await performance_comparison(connection_string, 10)
        await error_handling_example(connection_string)
        await bulk_operations_example(connection_string)
        
        print("\n=== All async examples completed successfully! ===")
        
    except Exception as e:
        print(f"Async examples failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure SQL Server is running")
        print("2. Check your connection string")
        print("3. Verify network connectivity")
        print("4. Check authentication method")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
