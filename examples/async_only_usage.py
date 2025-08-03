#!/usr/bin/env python3
"""
Async usage example for mssql-python-rust

This example demonstrates asynchronous usage patterns with the new async-only API.
"""

import asyncio
import os
import sys
import time

# Add the parent directory to Python path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    from mssql_python_rust import Connection
except ImportError as e:
    print(f"Error importing mssql_python_rust: {e}")
    print("Make sure you've built the extension with 'maturin develop'")
    sys.exit(1)

async def async_examples(connection_string: str):
    """Modern asynchronous examples using the new async-only API."""
    print("\n" + "="*50)
    print("ASYNCHRONOUS EXAMPLES")
    print("="*50)
    
    try:
        # Example 1: Basic async query
        print("\n=== Async Example 1: Basic Async Query ===")
        async with Connection(connection_string) as conn:
            rows = await conn.execute("SELECT @@VERSION as sql_version")
            for row in rows:
                print(f"SQL Server Version: {row['sql_version'][:100]}...")
        
        # Example 2: Concurrent queries
        print("\n=== Async Example 2: Concurrent Queries ===")
        queries = [
            ("Query A", "SELECT 'Async Query A' as name, 1 as value"),
            ("Query B", "SELECT 'Async Query B' as name, 2 as value"),
            ("Query C", "SELECT 'Async Query C' as name, 3 as value"),
        ]
        
        async def execute_single_query(name: str, query: str):
            async with Connection(connection_string) as conn:
                start_time = time.time()
                rows = await conn.execute(query)
                duration = time.time() - start_time
                if rows:
                    result = rows[0]
                    print(f"{name}: {result['name']} = {result['value']} (took {duration:.3f}s)")
                    return result
        
        # Execute all queries concurrently
        start_time = time.time()
        tasks = [execute_single_query(name, query) for name, query in queries]
        results = await asyncio.gather(*tasks)
        total_duration = time.time() - start_time
        print(f"All queries completed concurrently in {total_duration:.3f}s")
        
        # Example 3: Async with error handling
        print("\n=== Async Example 3: Error Handling ===")
        try:
            async with Connection(connection_string) as conn:
                rows = await conn.execute("SELECT * FROM non_existent_table")
        except Exception as e:
            print(f"Expected error caught: {e}")
        
        # Example 4: Multiple operations on same connection
        print("\n=== Async Example 4: Multiple Operations ===")
        async with Connection(connection_string) as conn:
            # Multiple queries using the same connection
            queries = [
                "SELECT GETDATE() as current_time",
                "SELECT 'Hello' as greeting, 'World' as target",
                "SELECT 42 as answer, 3.14159 as pi"
            ]
            
            for i, query in enumerate(queries, 1):
                print(f"\nQuery {i}: {query}")
                rows = await conn.execute(query)
                for row in rows:
                    print(f"  Result: {dict(row)}")
        
        # Example 5: Performance with concurrent connections
        print("\n=== Async Example 5: Performance Comparison ===")
        await performance_comparison(connection_string, 5)
        
        print("\n=== Async examples completed! ===")
        
    except Exception as e:
        print(f"Async examples failed: {e}")
        return False
    
    return True

async def performance_comparison(connection_string: str, num_queries: int):
    """Compare sequential vs concurrent async performance."""
    print(f"Comparing sequential vs concurrent async performance with {num_queries} queries...")
    
    # Async version (sequential)
    print("Running asynchronous queries (sequential)...")
    start_time = time.time()
    async with Connection(connection_string) as conn:
        for i in range(num_queries):
            rows = await conn.execute(f"SELECT {i} as query_number, GETDATE() as timestamp")
    async_sequential_duration = time.time() - start_time
    
    # Async version (concurrent)
    print("Running asynchronous queries (concurrent)...")
    async def single_query(i: int):
        async with Connection(connection_string) as conn:
            return await conn.execute(f"SELECT {i} as query_number, GETDATE() as timestamp")
    
    start_time = time.time()
    tasks = [single_query(i) for i in range(num_queries)]
    await asyncio.gather(*tasks)
    async_concurrent_duration = time.time() - start_time
    
    print(f"Results:")
    print(f"  Async (sequential):    {async_sequential_duration:.3f}s ({num_queries/async_sequential_duration:.1f} queries/sec)")
    print(f"  Async (concurrent):    {async_concurrent_duration:.3f}s ({num_queries/async_concurrent_duration:.1f} queries/sec)")
    if async_concurrent_duration > 0:
        speedup = async_sequential_duration / async_concurrent_duration
        print(f"  Concurrent speedup: {speedup:.1f}x faster")

async def main():
    """Main function demonstrating async patterns."""
    
    # Connection string - adjust as needed for your environment
    connection_string = "Server=localhost;Database=master;Integrated Security=true"
    
    print("mssql-python-rust Async-Only Examples")
    print("New simplified async-only API")
    
    try:
        # Run asynchronous examples
        async_success = await async_examples(connection_string)
        
        if async_success:
            print("\n" + "="*50)
            print("ALL EXAMPLES COMPLETED SUCCESSFULLY!")
            print("="*50)
            print("\nKey features of the new async-only API:")
            print("1. Clean async context manager: async with Connection(conn_str)")
            print("2. Intuitive method names: await conn.execute(sql)")
            print("3. No confusing _async suffixes when already in async context")
            print("4. Perfect for concurrent operations and async web frameworks")
            return 0
        
        return 1
        
    except Exception as e:
        print(f"Examples failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure SQL Server is running")
        print("2. Check your connection string")
        print("3. Verify network connectivity")
        print("4. Check authentication method")
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
