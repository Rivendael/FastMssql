#!/usr/bin/env python3
"""
Mixed sync/async usage example for mssql-python-rust

This example demonstrates both synchronous and asynchronous usage patterns.
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

def sync_examples(connection_string: str):
    """Traditional synchronous examples."""
    print("\n" + "="*50)
    print("SYNCHRONOUS EXAMPLES")
    print("="*50)
    
    print(f"\nmssql-python-rust version: {mssql.version()}")
    print(f"Connecting to: {connection_string}")
    
    try:
        # Example 1: Using context manager (recommended)
        print("\n=== Sync Example 1: Basic Query with Context Manager ===")
        with mssql.connect(connection_string) as conn:
            # Execute a simple query
            rows = conn.execute("SELECT @@VERSION as sql_version")
            for row in rows:
                print(f"SQL Server Version: {row['sql_version'][:100]}...")
        
        # Example 2: Manual connection management
        print("\n=== Sync Example 2: Manual Connection Management ===")
        conn = mssql.connect(connection_string)
        try:
            conn.connect()
            result = conn.execute("SELECT GETDATE() as current_time")
            if result:
                print(f"Current time: {result[0]['current_time']}")
        finally:
            conn.disconnect()
        
        # Example 3: Multiple queries
        print("\n=== Sync Example 3: Multiple Queries ===")
        queries = [
            "SELECT 'Hello' as greeting, 'World' as target",
            "SELECT 42 as answer, 3.14159 as pi",
            "SELECT 1 as one, 2 as two, 3 as three"
        ]
        
        with mssql.connect(connection_string) as conn:
            for i, query in enumerate(queries, 1):
                print(f"\nQuery {i}: {query}")
                rows = conn.execute(query)
                for row in rows:
                    print(f"  Result: {dict(row.to_dict())}")
        
        print("\n=== Sync examples completed! ===")
        
    except Exception as e:
        print(f"Sync examples failed: {e}")
        return False
    
    return True

async def async_examples(connection_string: str):
    """Modern asynchronous examples."""
    print("\n" + "="*50)
    print("ASYNCHRONOUS EXAMPLES")
    print("="*50)
    
    try:
        # Example 1: Basic async query
        print("\n=== Async Example 1: Basic Async Query ===")
        async with mssql.connect_async(connection_string) as conn:
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
            async with mssql.connect_async(connection_string) as conn:
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
            async with mssql.connect_async(connection_string) as conn:
                rows = await conn.execute("SELECT * FROM non_existent_table")
        except Exception as e:
            print(f"Expected error caught: {e}")
        
        # Example 4: Performance comparison
        print("\n=== Async Example 4: Performance Comparison ===")
        await performance_comparison(connection_string, 5)
        
        print("\n=== Async examples completed! ===")
        
    except Exception as e:
        print(f"Async examples failed: {e}")
        return False
    
    return True

async def performance_comparison(connection_string: str, num_queries: int):
    """Compare sync vs async performance."""
    print(f"Comparing sync vs async performance with {num_queries} queries...")
    
    # Sync version
    print("Running synchronous queries...")
    start_time = time.time()
    with mssql.connect(connection_string) as conn:
        for i in range(num_queries):
            rows = conn.execute(f"SELECT {i} as query_number, GETDATE() as timestamp")
    sync_duration = time.time() - start_time
    
    # Async version (sequential)
    print("Running asynchronous queries (sequential)...")
    start_time = time.time()
    async with mssql.connect_async(connection_string) as conn:
        for i in range(num_queries):
            rows = await conn.execute(f"SELECT {i} as query_number, GETDATE() as timestamp")
    async_sequential_duration = time.time() - start_time
    
    # Async version (concurrent)
    print("Running asynchronous queries (concurrent)...")
    async def single_query(i: int):
        async with mssql.connect_async(connection_string) as conn:
            return await conn.execute(f"SELECT {i} as query_number, GETDATE() as timestamp")
    
    start_time = time.time()
    tasks = [single_query(i) for i in range(num_queries)]
    await asyncio.gather(*tasks)
    async_concurrent_duration = time.time() - start_time
    
    print(f"Results:")
    print(f"  Synchronous:           {sync_duration:.3f}s ({num_queries/sync_duration:.1f} queries/sec)")
    print(f"  Async (sequential):    {async_sequential_duration:.3f}s ({num_queries/async_sequential_duration:.1f} queries/sec)")
    print(f"  Async (concurrent):    {async_concurrent_duration:.3f}s ({num_queries/async_concurrent_duration:.1f} queries/sec)")
    if async_concurrent_duration > 0:
        speedup = sync_duration / async_concurrent_duration
        print(f"  Async concurrent speedup: {speedup:.1f}x faster")

async def main():
    """Main function demonstrating both sync and async patterns."""
    
    # Connection string - adjust as needed for your environment
    connection_string = "Server=localhost;Database=master;Integrated Security=true"
    
    print("mssql-python-rust Mixed Sync/Async Examples")
    print(f"Version: {mssql.version()}")
    
    try:
        # Run synchronous examples
        sync_success = sync_examples(connection_string)
        
        if sync_success:
            # Run asynchronous examples
            async_success = await async_examples(connection_string)
            
            if async_success:
                print("\n" + "="*50)
                print("ALL EXAMPLES COMPLETED SUCCESSFULLY!")
                print("="*50)
                print("\nKey takeaways:")
                print("1. Sync API: Simple, blocking, good for sequential operations")
                print("2. Async API: Non-blocking, excellent for concurrent operations")
                print("3. Use async when you have multiple independent database operations")
                print("4. Use async when integrating with async web frameworks")
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
