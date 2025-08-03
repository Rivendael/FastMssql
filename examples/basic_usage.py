#!/usr/bin/env python3
"""
Basic usage example for mssql-python-rust

This example demonstrates how to connect to SQL Server and execute queries.
"""

import os
import sys

# Add the parent directory to Python path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    import mssql_python_rust as mssql
except ImportError as e:
    print(f"Error importing mssql_python_rust: {e}")
    print("Make sure you've built the extension with 'maturin develop'")
    sys.exit(1)

def main():
    """Main example function."""
    
    # Connection string - adjust as needed for your environment
    # For Windows Authentication:
    connection_string = "Server=localhost;Database=master;Integrated Security=true"
    
    # For SQL Server Authentication:
    # connection_string = "Server=localhost;Database=master;User Id=sa;Password=YourPassword"
    
    print(f"mssql-python-rust version: {mssql.version()}")
    print(f"Connecting to: {connection_string}")
    
    try:
        # Example 1: Using context manager (recommended)
        print("\n=== Example 1: Basic Query with Context Manager ===")
        with mssql.connect(connection_string) as conn:
            # Execute a simple query
            rows = conn.execute("SELECT @@VERSION as sql_version")
            for row in rows:
                print(f"SQL Server Version: {row['sql_version'][:100]}...")
        
        # Example 2: Manual connection management
        print("\n=== Example 2: Manual Connection Management ===")
        conn = mssql.connect(connection_string)
        try:
            conn.connect()
            result = conn.execute("SELECT GETDATE() as current_time")
            if result:
                print(f"Current time: {result[0]['current_time']}")
        finally:
            conn.disconnect()
        
        # Example 3: Multiple queries
        print("\n=== Example 3: Multiple Queries ===")
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
                    print(f"  Result: {row.to_dict()}")
        
        # Example 4: Error handling
        print("\n=== Example 4: Error Handling ===")
        try:
            with mssql.connect(connection_string) as conn:
                # This will cause an error
                rows = conn.execute("SELECT * FROM non_existent_table")
        except Exception as e:
            print(f"Expected error caught: {e}")
        
        # Example 5: Using Query objects
        print("\n=== Example 5: Using Query Objects ===")
        try:
            query = mssql.Query("SELECT 'Hello from query object!' as message")
            
            with mssql.connect(connection_string) as conn:
                rows = query.execute(conn)
                for row in rows:
                    print(f"Query object result: {row['message']}")
        except Exception as e:
            print(f"Query object example failed: {e}")
            print("(Query objects may not be fully implemented yet)")
    
    except Exception as e:
        print(f"Connection failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure SQL Server is running")
        print("2. Check your connection string")
        print("3. Verify network connectivity")
        print("4. Check authentication method")
        return 1
    
    print("\n=== All examples completed successfully! ===")
    return 0

if __name__ == "__main__":
    sys.exit(main())
