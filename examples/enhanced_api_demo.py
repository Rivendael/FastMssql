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
        max_lifetime=3600,  # 1 hour
        idle_timeout=300,   # 5 minutes
        connection_timeout=30
    )
    
    # Method 1: Using connection manager function (recommended)
    async with mssql.connect(connection_string, pool_config) as conn:
        print("=== Using connect() function ===")
        
        # Execute DDL
        await conn.execute("CREATE TABLE IF NOT EXISTS users (id INT, name VARCHAR(50), age INT)")
        
        # Execute DML with clear result types
        insert_result = await conn.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)")
        print(f"Inserted {insert_result.affected_rows} rows")
        
        # Execute SELECT with enhanced results
        select_result = await conn.execute("SELECT * FROM users WHERE age > 20")
        print(f"Found {len(select_result)} rows:")
        
        # Iterate over typed Row objects
        for row in select_result.rows:
            # IDE will provide autocompletion for these methods
            print(f"Row as dict: {row.to_dict()}")
            print(f"Row as tuple: {row.to_tuple()}")
            print(f"Name: {row['name']}, Age: {row['age']}")
        
        # Execute scalar query
        count = await conn.execute_scalar("SELECT COUNT(*) FROM users")
        print(f"Total users: {count}")
        
        # Get results as dictionaries (convenience method)
        users_dict = await conn.execute_dict("SELECT name, age FROM users ORDER BY age")
        for user in users_dict:
            print(f"{user['name']} is {user['age']} years old")
    
    # Method 2: Using Connection class directly
    async with mssql.Connection(connection_string, pool_config) as conn:
        print("\n=== Using Connection() class directly ===")
        
        # Same API, just different instantiation
        result = await conn.execute("SELECT COUNT(*) FROM users")
        print(f"User count: {result.rows[0][0]}")
        
        # Execute scalar query
        avg_age = await conn.execute_scalar("SELECT AVG(CAST(age AS FLOAT)) FROM users")
        print(f"Average age: {avg_age}")
    
    # Method 3: One-off queries (for simple operations)
    print("\n=== Using convenience functions ===")
    
    # Execute with automatic connection management
    result = await mssql.execute_async(
        connection_string, 
        "SELECT COUNT(*) FROM users",
        pool_config
    )
    print(f"User count: {result.rows[0][0]}")
    
    # Get scalar value directly
    avg_age = await mssql.execute_scalar_async(
        connection_string,
        "SELECT AVG(CAST(age AS FLOAT)) FROM users",
        pool_config
    )
    print(f"Average age: {avg_age}")
    
    # Get results as dictionaries
    users = await mssql.execute_dict_async(
        connection_string,
        "SELECT * FROM users ORDER BY name",
        pool_config
    )
    print("All users:")
    for user in users:
        print(f"  {user}")

async def compare_apis():
    """Compare the enhanced API vs direct Rust API."""
    
    connection_string = "Server=localhost;Database=TestDB;Integrated Security=true"
    
    print("=== Enhanced Python API (with type hints and wrappers) ===")
    async with mssql.connect(connection_string) as conn:
        result = await conn.execute("SELECT 'Hello' as message, 42 as number")
        
        # Enhanced API provides rich type information
        for row in result.rows:
            # IDE autocomplete works here
            data = row.to_dict()
            print(f"Message: {data['message']}, Number: {data['number']}")
            
            # Type-safe access
            print(f"Row length: {len(row)}")
            print(f"First column: {row[0]}")
    
    print("\n=== Direct Rust API (minimal wrapping) ===")
    # You can still access the lower-level API when needed
    async with mssql.connect(connection_string) as conn:
        # Convert to proper async connection for consistency
        result = await conn.execute("SELECT 'Hello' as message, 42 as number")
        
        # Enhanced API provides better type information
        for row in result.rows:
            data = row.to_dict()
            print(f"Message: {data['message']}, Number: {data['number']}")

if __name__ == "__main__":
    # Run examples
    asyncio.run(example_usage())
    asyncio.run(compare_apis())
