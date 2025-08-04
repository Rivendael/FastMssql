"""
Example usage of mssql-python-rust with enhanced wrapper classes

This demonstrates the benefits of the Python wrapper classes over direct Rust access.
"""

import asyncio
import mssql

async def example_usage():
    """Demonstrate enhanced Python API with better type hints and IDE support."""
    
    connection_string = "Server=localhost;Database=TestDB;Integrated Security=true"
    
    # Create connection with pool configuration
    pool_config = mssql.PoolConfig(
        max_size=20,
        min_idle=5,
        max_lifetime_secs=3600,  # 1 hour
        idle_timeout_secs=300,   # 5 minutes
        connection_timeout_secs=30
    )
    
    # Method 1: Using Connection class with connection string (recommended)
    async with mssql.Connection(connection_string, pool_config) as conn:
        print("=== Using Connection() class with connection string ===")
        
        # Execute DDL
        await conn.execute("CREATE TABLE IF NOT EXISTS users (id INT, name VARCHAR(50), age INT)")
        
        # Execute DML with clear result types
        insert_result = await conn.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
        print(f"Inserted {insert_result.affected_rows} rows")
        
        # Execute SELECT with enhanced results
        select_result = await conn.execute("SELECT * FROM users WHERE age > 20")
        print(f"Found {len(select_result)} rows:")
        
        # Iterate over typed Row objects
        for row in select_result.rows():
            # IDE will provide autocompletion for these methods
            print(f"Row as dict: {row.to_dict()}")
            print(f"Row as tuple: {row.to_tuple()}")
            print(f"Name: {row['name']}, Age: {row['age']}")
        
        # Execute scalar query (get first value from first row)
        count_result = await conn.execute("SELECT COUNT(*) FROM users")
        count = count_result.rows()[0][0] if count_result.rows() else 0
        print(f"Total users: {count}")
    
    # Method 2: Using Connection class with individual connection parameters (new feature!)
    async with mssql.Connection(
        server="localhost",
        database="TestDB", 
        trusted_connection=True,
        pool_config=pool_config
    ) as conn:
        print("\n=== Using Connection() class with individual connection parameters ===")
        
        # Same API, just different connection method
        result = await conn.execute("SELECT COUNT(*) FROM users")
        count = result.rows()[0][0] if result.rows() else 0
        print(f"User count: {count}")
    
    # Method 3: Using Connection class directly (alternative syntax)
    async with mssql.Connection(connection_string=connection_string, pool_config=pool_config) as conn:
        print("\n=== Using Connection() class with keyword arguments ===")
        
        # Same API, just different instantiation style
        result = await conn.execute("SELECT COUNT(*) FROM users")
        count = result.rows()[0][0] if result.rows() else 0
        print(f"User count: {count}")
        
        # Execute scalar query (get average age)
        avg_result = await conn.execute("SELECT AVG(CAST(age AS FLOAT)) FROM users")
        avg_age = avg_result.rows()[0][0] if avg_result.rows() else 0
        print(f"Average age: {avg_age}")
    
    # Method 4: Alternative patterns showing different ways to use Connection
    print("\n=== Additional Connection patterns ===")
    
    # Pattern A: Create connection and manage manually (not recommended for production)
    conn = mssql.Connection(connection_string, pool_config)
    await conn.connect()
    try:
        result = await conn.execute("SELECT COUNT(*) FROM users")
        count = result.rows()[0][0] if result.rows() else 0
        print(f"Manual connection - User count: {count}")
    finally:
        await conn.disconnect()
    
    # Pattern B: Using Connection with individual parameters and explicit keywords
    async with mssql.Connection(
        connection_string=None,  # Explicitly don't use connection string
        server="localhost",
        database="TestDB",
        trusted_connection=True,
        pool_config=pool_config
    ) as conn:
        result = await conn.execute("SELECT AVG(CAST(age AS FLOAT)) FROM users")
        avg_age = result.rows()[0][0] if result.rows() else 0
        print(f"Individual params - Average age: {avg_age}")
    
    async with mssql.Connection(connection_string, pool_config) as conn:
        users = await conn.execute("SELECT * FROM users ORDER BY name")
        print("All users (as dictionaries):")
        for user in users.rows():
            print(f"  {user}")

async def compare_apis():
    """Compare the enhanced API vs direct Rust API."""
    
    connection_string = "Server=localhost;Database=TestDB;Integrated Security=true"
    
    print("=== Enhanced Python API (with type hints and wrappers) ===")
    async with mssql.Connection(connection_string) as conn:
        result = await conn.execute("SELECT 'Hello' as message, 42 as number")
        
        # Enhanced API provides rich type information
        for row in result.rows():
            # IDE autocomplete works here
            data = row.to_dict()
            print(f"Message: {data['message']}, Number: {data['number']}")
            
            # Type-safe access
            print(f"Row length: {len(row)}")
            print(f"First column: {row[0]}")
    
    print("\n=== Direct Rust API (minimal wrapping) ===")
    # You can still access the lower-level API when needed
    async with mssql.Connection(connection_string) as conn:
        result = await conn.execute("SELECT 'Hello' as message, 42 as number")
        
        # Enhanced API provides better type information
        for row in result.rows():
            data = row.to_dict()
            print(f"Message: {data['message']}, Number: {data['number']}")

if __name__ == "__main__":
    # Run examples
    asyncio.run(example_usage())
    asyncio.run(compare_apis())
