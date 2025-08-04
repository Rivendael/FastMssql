#!/usr/bin/env python3
"""
Basic usage example for mssql-python-rust

This example demonstrates how to connect to SQL Server and execute queries using the new async-only API.
"""

import asyncio
import os
import sys

# Add the parent directory to Python path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    from fastmssql import Connection
except ImportError as e:
    print(f"Error importing mssql: {e}")
    print("Make sure you've built the extension with 'maturin develop'")
    sys.exit(1)

async def main():
    """Main example function."""
    
    # Connection string - adjust as needed for your environment
    # For Windows Authentication:
    connection_string = "Server=localhost;Database=master;Integrated Security=true"
    
    # For SQL Server Authentication:
    # connection_string = "Server=localhost;Database=master;User Id=sa;Password=YourPassword"
    
    print("mssql-python-rust - Async-Only API Examples")
    print(f"Connecting to: {connection_string}")
    
    try:
        # Example 1: Basic Query with Async Context Manager
        print("\n=== Example 1: Basic Query with Async Context Manager ===")
        async with Connection(connection_string) as conn:
            # Execute a simple query
            rows = await conn.execute("SELECT @@VERSION as sql_version")
            for row in rows:
                print(f"SQL Server Version: {row['sql_version'][:100]}...")
        
        # Example 2: Multiple queries on same connection
        print("\n=== Example 2: Multiple Queries ===")
        queries = [
            "SELECT 'Hello' as greeting, 'World' as target",
            "SELECT 42 as answer, 3.14159 as pi",
            "SELECT 1 as one, 2 as two, 3 as three"
        ]
        
        async with Connection(connection_string) as conn:
            for i, query in enumerate(queries, 1):
                print(f"\nQuery {i}: {query}")
                rows = await conn.execute(query)
                for row in rows:
                    print(f"  Result: {dict(row)}")
        
        # Example 3: Error handling
        print("\n=== Example 3: Error Handling ===")
        try:
            async with Connection(connection_string) as conn:
                # This will cause an error
                rows = await conn.execute("SELECT * FROM non_existent_table")
        except Exception as e:
            print(f"Expected error caught: {e}")
        
        # Example 4: Non-query operations (INSERT, UPDATE, DELETE)
        print("\n=== Example 4: Non-Query Operations ===")
        async with Connection(connection_string) as conn:
            # This would normally be used for CREATE TABLE, INSERT, etc.
            # For demo purposes, we'll use a SELECT that doesn't return rows
            rows_affected = await conn.execute("SELECT 1 WHERE 1=0")
            print(f"Rows affected: {rows_affected}")
        
        # Example 5: Concurrent queries
        print("\n=== Example 5: Concurrent Queries ===")
        async def single_query(query_id: int, query: str):
            async with Connection(connection_string) as conn:
                rows = await conn.execute(f"SELECT {query_id} as id, '{query}' as query")
                return rows[0] if rows else None
        
        # Execute multiple queries concurrently
        tasks = [
            single_query(1, "First query"),
            single_query(2, "Second query"),
            single_query(3, "Third query")
        ]
        
        results = await asyncio.gather(*tasks)
        for result in results:
            if result:
                print(f"Concurrent result: ID={result['id']}, Query='{result['query']}'")
    
    except Exception as e:
        print(f"Connection failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure SQL Server is running")
        print("2. Check your connection string")
        print("3. Verify network connectivity")
        print("4. Check authentication method")
        return 1
    
    print("\n=== All examples completed successfully! ===")
    print("\nKey benefits of the async-only API:")
    print("1. Clean and intuitive: async with Connection(conn_str)")
    print("2. Natural method names: await conn.execute(sql)")
    print("3. Perfect for concurrent operations")
    print("4. Ideal for async web frameworks")
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
