#!/usr/bin/env python3
"""
Async usage example for mssql-python-rust

This example demonstrates the new simplified async-only API.
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

async def basic_async_example(connection_string: str):
    """Basic async connection and query example."""
    print("\n=== Basic Async Example ===")
    
    # Using async context manager with the new simplified API
    async with Connection(connection_string) as conn:
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
    
    async def execute_single_query(query_name: str, sql: str):
        """Execute a single query with timing."""
        start_time = time.time()
        async with Connection(connection_string) as conn:
            rows = await conn.execute(sql)
            duration = time.time() - start_time
            result = rows[0] if rows else None
            print(f"{query_name}: {result['name']} = {result['value']} (took {duration:.3f}s)")
            return result
    
    # Execute all queries concurrently
    print("Executing queries concurrently...")
    start_time = time.time()
    
    tasks = [
        execute_single_query(f"Task {i+1}", query) 
        for i, query in enumerate(queries)
    ]
    
    results = await asyncio.gather(*tasks)
    total_duration = time.time() - start_time
    
    print(f"All {len(queries)} queries completed in {total_duration:.3f}s")

async def multiple_operations_example(connection_string: str):
    """Example showing multiple operations on the same connection."""
    print("\n=== Multiple Operations Example ===")
    
    async with Connection(connection_string) as conn:
        # Execute multiple queries using the same connection
        operations = [
            ("Server info", "SELECT @@SERVERNAME as server_name, @@VERSION as version"),
            ("Database info", "SELECT DB_NAME() as database_name, USER_NAME() as user_name"),
            ("System time", "SELECT GETDATE() as current_time, GETUTCDATE() as utc_time"),
            ("Math operations", "SELECT 1+1 as addition, 10*5 as multiplication, POWER(2,3) as power")
        ]
        
        for name, query in operations:
            print(f"\n{name}:")
            rows = await conn.execute(query)
            for row in rows:
                for key, value in row.items():
                    print(f"  {key}: {value}")

async def error_handling_example(connection_string: str):
    """Example showing proper error handling."""
    print("\n=== Error Handling Example ===")
    
    # Example 1: Invalid query
    try:
        async with Connection(connection_string) as conn:
            await conn.execute("SELECT * FROM non_existent_table")
    except Exception as e:
        print(f"Caught expected error: {e}")
    
    # Example 2: Connection with invalid connection string
    try:
        async with Connection("Invalid=connection;string") as conn:
            await conn.execute("SELECT 1")
    except Exception as e:
        print(f"Caught connection error: {e}")
    
    # Example 3: Proper error handling with cleanup
    connection_opened = False
    try:
        async with Connection(connection_string) as conn:
            connection_opened = True
            print("Connection opened successfully")
            
            # This will fail
            await conn.execute("INVALID SQL SYNTAX")
            
    except Exception as e:
        print(f"Error occurred, but connection was properly cleaned up: {e}")
    finally:
        if connection_opened:
            print("Connection cleanup handled automatically by context manager")

async def performance_comparison_example(connection_string: str):
    """Example showing performance benefits of concurrent execution."""
    print("\n=== Performance Comparison Example ===")
    
    num_queries = 5
    
    # Sequential execution
    print(f"Executing {num_queries} queries sequentially...")
    start_time = time.time()
    async with Connection(connection_string) as conn:
        for i in range(num_queries):
            await conn.execute(f"SELECT {i} as query_id, GETDATE() as timestamp")
    sequential_duration = time.time() - start_time
    
    # Concurrent execution
    print(f"Executing {num_queries} queries concurrently...")
    async def single_query(query_id: int):
        async with Connection(connection_string) as conn:
            return await conn.execute(f"SELECT {query_id} as query_id, GETDATE() as timestamp")
    
    start_time = time.time()
    tasks = [single_query(i) for i in range(num_queries)]
    await asyncio.gather(*tasks)
    concurrent_duration = time.time() - start_time
    
    print(f"Results:")
    print(f"  Sequential: {sequential_duration:.3f}s ({num_queries/sequential_duration:.1f} queries/sec)")
    print(f"  Concurrent: {concurrent_duration:.3f}s ({num_queries/concurrent_duration:.1f} queries/sec)")
    
    if concurrent_duration > 0:
        speedup = sequential_duration / concurrent_duration
        print(f"  Speedup: {speedup:.1f}x faster with concurrent execution")

async def main():
    """Main function demonstrating various async patterns."""
    
    # Connection string - adjust as needed for your environment
    connection_string = "Server=localhost;Database=master;Integrated Security=true"
    
    print("mssql-python-rust Async Usage Examples")
    print("Demonstrating the new simplified async-only API")
    
    try:
        await basic_async_example(connection_string)
        await concurrent_queries_example(connection_string)
        await multiple_operations_example(connection_string)
        await error_handling_example(connection_string)
        await performance_comparison_example(connection_string)
        
        print("\n" + "="*50)
        print("ALL ASYNC EXAMPLES COMPLETED SUCCESSFULLY!")
        print("="*50)
        print("\nKey advantages of the async-only API:")
        print("1. Simplified syntax: async with Connection(conn_str)")
        print("2. Intuitive method names: await conn.execute(sql)")
        print("3. No confusing _async suffixes")
        print("4. Perfect for high-performance concurrent operations")
        print("5. Ideal for modern async Python applications")
        
        return 0
        
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
