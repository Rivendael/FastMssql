#!/usr/bin/env python3
"""
Example demonstrating parameterized queries with pymssql-rs

This example shows how to use parameterized queries for safe SQL execution,
protecting against SQL injection attacks.
"""

import asyncio
import sys
import os

# Add the python module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

import mssql

async def main():
    # Connection string - modify for your environment
    conn_string = "Server=localhost;Database=test;Integrated Security=true"
    
    try:
        # Create connection with connection pooling
        pool_config = mssql.PoolConfig.development()
        
        async with mssql.connect(conn_string, pool_config) as conn:
            print("Connected to SQL Server!")
            
            # Example 1: Using Connection.execute_with_params()
            print("\n=== Example 1: Connection.execute_with_params() ===")
            
            # Create a test table (if it doesn't exist)
            await conn.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'users')
                BEGIN
                    CREATE TABLE users (
                        id INT IDENTITY(1,1) PRIMARY KEY,
                        name NVARCHAR(100) NOT NULL,
                        email NVARCHAR(100) NOT NULL,
                        age INT,
                        created_at DATETIME2 DEFAULT GETDATE()
                    )
                END
            """)
            print("Table 'users' ready")
            
            # Insert users with parameterized queries (safe from SQL injection)
            users_to_insert = [
                ("Alice Johnson", "alice@example.com", 25),
                ("Bob Smith", "bob@example.com", 30),
                ("Charlie Brown", "charlie@example.com", 35),
                ("Diana Prince", "diana@example.com", 28)
            ]
            
            for name, email, age in users_to_insert:
                result = await conn.execute_with_params(
                    "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                    [name, email, age]
                )
                print(f"Inserted user '{name}' - affected rows: {result.affected_rows}")
            
            # Query with parameters
            min_age = 28
            result = await conn.execute_with_params(
                "SELECT id, name, email, age FROM users WHERE age >= ? ORDER BY age",
                [min_age]
            )
            
            print(f"\nUsers aged {min_age} or older:")
            for row in result.rows():
                print(f"  ID: {row['id']}, Name: {row['name']}, Email: {row['email']}, Age: {row['age']}")
            
            # Example 2: Using Query class for reusable parameterized queries
            print("\n=== Example 2: Query class ===")
            
            # Create a reusable parameterized query
            user_search_query = mssql.Query("SELECT * FROM users WHERE name LIKE ? OR email LIKE ?")
            
            # Execute with different parameters
            search_term = "%Alice%"
            user_search_query.set_parameters([search_term, search_term])
            
            print(f"Query: {user_search_query.sql}")
            print(f"Parameters: {user_search_query.parameters}")
            
            result = await user_search_query.execute(conn)
            print(f"\nSearch results for '{search_term}':")
            for row in result.rows():
                print(f"  {row['name']} - {row['email']}")
            
            # Example 3: Different parameter types
            print("\n=== Example 3: Different parameter types ===")
            
            # Test different types of parameters
            test_query = mssql.Query("""
                SELECT 
                    ? as string_param,
                    ? as int_param,
                    ? as float_param,
                    ? as bool_param,
                    ? as null_param,
                    GETDATE() as current_time
            """)
            
            test_query.set_parameters([
                "Hello World",    # String
                42,              # Integer
                3.14159,         # Float
                True,            # Boolean
                None             # NULL
            ])
            
            result = await test_query.execute(conn)
            if result.rows():
                row = result.rows()[0]
                print("Parameter type test results:")
                print(f"  String: {row['string_param']}")
                print(f"  Integer: {row['int_param']}")
                print(f"  Float: {row['float_param']}")
                print(f"  Boolean: {row['bool_param']}")
                print(f"  NULL: {row['null_param']}")
                print(f"  Current time: {row['current_time']}")
            
            # Example 4: Batch operations with parameters
            print("\n=== Example 4: Batch operations ===")
            
            # Update multiple users
            update_query = mssql.Query("UPDATE users SET age = age + ? WHERE name = ?")
            
            updates = [
                (1, "Alice Johnson"),  # Add 1 year to Alice
                (2, "Bob Smith"),      # Add 2 years to Bob
            ]
            
            for age_increment, name in updates:
                update_query.set_parameters([age_increment, name])
                result = await update_query.execute(conn)
                print(f"Updated {name} - affected rows: {result.affected_rows}")
            
            # Show updated results
            result = await conn.execute("SELECT name, age FROM users ORDER BY name")
            print("\nUpdated user ages:")
            for row in result.rows():
                print(f"  {row['name']}: {row['age']} years old")
            
            # Example 5: Using convenience methods
            print("\n=== Example 5: Convenience methods ===")
            
            # Get scalar value (single value)
            count = await conn.execute_scalar_with_params(
                "SELECT COUNT(*) FROM users WHERE age > ?", 
                [25]
            )
            print(f"Users older than 25: {count}")
            
            # Get results as dictionaries
            young_users = await conn.execute_dict_with_params(
                "SELECT name, age FROM users WHERE age <= ? ORDER BY age DESC",
                [30]
            )
            print(f"Users aged 30 or younger:")
            for user in young_users:
                print(f"  {user['name']}: {user['age']}")
            
            # Cleanup - remove test data
            cleanup_result = await conn.execute("DELETE FROM users WHERE name IN ('Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince')")
            print(f"\nCleanup: Removed {cleanup_result.affected_rows} test users")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("Parameterized Queries Example")
    print("=" * 40)
    print("This example demonstrates safe parameterized queries")
    print("that protect against SQL injection attacks.")
    print()
    
    asyncio.run(main())
