"""
Example demonstrating parameterized queries in pymssql-rs

This example shows how to safely execute SQL queries with parameters
to prevent SQL injection attacks.
"""

import asyncio
import os

# Try to import the module - adjust import based on your environment
try:
    import mssql_python_rust as mssql
except ImportError:
    try:
        import sys
        sys.path.append('../python')
        import mssql
    except ImportError:
        print("Please build the module with 'maturin develop' or install it")
        exit(1)


async def main():
    # Connection string - update with your SQL Server details
    connection_string = os.environ.get(
        "MSSQL_CONNECTION_STRING",
        "Server=localhost;Database=test;Integrated Security=true"
    )
    
    # Create a connection with optimized pool settings
    pool_config = mssql.PoolConfig(max_size=5, min_idle=1)
    
    async with mssql.connect(connection_string, pool_config) as conn:
        print("Connected to SQL Server")
        
        # Example 1: Using Query class for parameterized queries
        print("\n=== Example 1: Using Query class ===")
        
        # Create a parameterized query
        query = mssql.Query("SELECT ?, ? + ? AS sum, ? AS message")
        query.add_parameter(42)
        query.add_parameter(10)
        query.add_parameter(5)
        query.add_parameter("Hello, World!")
        
        # Execute the query
        result = await query.execute(conn)
        print(f"Query executed: {query}")
        
        for row in result.rows():
            print(f"Row: {row.to_dict()}")
        
        # Example 2: Using Connection.execute_with_params
        print("\n=== Example 2: Direct parameterized execution ===")
        
        # This demonstrates the direct connection method
        result2 = await conn.execute_with_params(
            "SELECT ? AS id, ? AS name, ? AS active",
            [1, "John Doe", True]
        )
        
        for row in result2.rows():
            print(f"Row: {row.to_dict()}")
        
        # Example 3: Multiple parameter types
        print("\n=== Example 3: Multiple parameter types ===")
        
        query3 = mssql.Query("""
            SELECT 
                ? AS null_value,
                ? AS boolean_value,
                ? AS integer_value,
                ? AS float_value,
                ? AS string_value,
                ? AS binary_value
        """)
        
        # Set parameters with different types
        query3.set_parameters([
            None,                    # NULL
            True,                    # BOOLEAN
            12345,                   # INTEGER
            3.14159,                 # FLOAT
            "Test String",           # STRING
            b"Binary Data",          # BYTES
        ])
        
        result3 = await query3.execute(conn)
        for row in result3.rows():
            print(f"Row with mixed types: {row.to_dict()}")
        
        # Example 4: INSERT with parameters (if you have a test table)
        print("\n=== Example 4: INSERT with parameters (commented) ===")
        print("# Uncomment and modify table name if you have a test table")
        
        # Uncomment these lines if you have a test table to insert into:
        """
        insert_query = mssql.Query("INSERT INTO test_table (name, age, active) VALUES (?, ?, ?)")
        insert_query.set_parameters(["Alice", 30, True])
        
        try:
            affected_rows = await insert_query.execute_non_query(conn)
            print(f"Inserted {affected_rows} rows")
        except Exception as e:
            print(f"Insert failed (expected if table doesn't exist): {e}")
        """
        
        print("\nExample completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
