#!/usr/bin/env python3
"""
Comprehensive FastMSSQL Library Usage Examples
This file demonstrates all the features and capabilities of the FastMSSQL library,
a high-performance Microsoft SQL Server driver for Python built with Rust.

Features demonstrated:
- Basic connection management with async context managers
- SELECT queries with query() method
- Data modification with execute() method
- Parameterized queries for security and performance
- Connection pooling configuration
- SSL/TLS configuration
- Error handling patterns
- Batch operations
- Transaction handling
- Performance optimization tips
"""

import asyncio
from fastmssql import Connection, PoolConfig, SslConfig, EncryptionLevel


async def basic_usage_example():
    """
    Basic usage example showing the fundamental operations.
    """
    print("🔹 Basic Usage Example")
    print("-" * 40)
    
    # Using async context manager - automatically handles connect/disconnect
    async with Connection("Server=localhost;Database=TestDB;User Id=testuser;Password=testpass;") as conn:
        
        # === SELECT QUERIES - Use query() method ===
        print("📖 SELECT Operations:")
        
        # Simple SELECT query
        result = await conn.query("SELECT TOP 5 * FROM users")
        rows = result.rows()
        for row in rows:
            print(f"  User: {row.get('name', 'N/A')}, Age: {row.get('age', 'N/A')}")
        
        # Parameterized SELECT query
        result = await conn.query(
            "SELECT * FROM users WHERE age > @P1 AND city = @P2",
            [25, "New York"]
        )
        users = result.rows()
        print(f"  Found {len(users)} users in New York over 25")
        
        # === DATA MODIFICATION - Use execute() method ===
        print("\n🔧 Data Modification Operations:")
        
        # INSERT operation
        affected = await conn.execute(
            "INSERT INTO users (name, email, age, city) VALUES (@P1, @P2, @P3, @P4)",
            ["John Doe", "john.doe@example.com", 30, "Chicago"]
        )
        print(f"  Inserted {affected} row(s)")
        
        # UPDATE operation
        affected = await conn.execute(
            "UPDATE users SET age = @P1 WHERE name = @P2",
            [31, "John Doe"]
        )
        print(f"  Updated {affected} row(s)")
        
        # DELETE operation
        affected = await conn.execute(
            "DELETE FROM users WHERE age > @P1",
            [100]
        )
        print(f"  Deleted {affected} row(s)")


async def connection_configuration_example():
    """
    Example showing different ways to configure connections.
    """
    print("\n🔹 Connection Configuration Examples")
    print("-" * 40)
    
    # Method 1: Connection string
    print("📡 Method 1: Connection String")
    async with Connection("Server=myserver.database.windows.net;Database=mydb;User Id=myuser;Password=mypass;") as conn:
        result = await conn.query("SELECT @@VERSION as version")
        rows = result.rows()
        for row in rows:
            print(f"  SQL Server Version: {row['version'][:50]}...")
    
    # Method 2: Individual parameters
    print("\n📡 Method 2: Individual Parameters")
    async with Connection(
        server="localhost",
        database="TestDB",
        username="testuser",
        password="testpass"
    ) as conn:
        result = await conn.query("SELECT DB_NAME() as current_db")
        rows = result.rows()
        for row in rows:
            print(f"  Current Database: {row['current_db']}")
    


async def advanced_configuration_example():
    """
    Example showing advanced configuration with connection pooling and SSL.
    """
    print("\n🔹 Advanced Configuration Example")
    print("-" * 40)
    
    # Configure connection pool
    pool_config = PoolConfig(
        max_connections=20,
        min_connections=2,
        acquire_timeout_seconds=30,
        idle_timeout_seconds=600
    )
    
    # Configure SSL/TLS
    ssl_config = SslConfig(
        encryption_level=EncryptionLevel.Required,
        trust_server_certificate=False,
        # certificate_path="/path/to/cert.pem"  # Optional certificate path
    )
    
    print("🔒 Using advanced configuration:")
    print(f"  Pool: {pool_config.min_connections}-{pool_config.max_connections} connections")
    print(f"  SSL: {ssl_config.encryption_level}")
    
    async with Connection(
        server="localhost",
        database="TestDB",
        username="testuser",
        password="testpass",
        pool_config=pool_config,
        ssl_config=ssl_config
    ) as conn:
        
        # Test the connection
        result = await conn.query("SELECT @@SERVERNAME as server_name, DB_NAME() as database_name")
        rows = result.rows()
        for row in rows:
            print(f"  Connected to: {row['server_name']}.{row['database_name']}")


async def parameter_types_example():
    """
    Example showing different parameter types and how to use them.
    """
    print("\n🔹 Parameter Types Example")
    print("-" * 40)
    
    async with Connection("Server=localhost;Database=TestDB;User Id=testuser;Password=testpass;") as conn:
        
        # Various parameter types
        print("📝 Testing different parameter types:")
        
        # String parameters
        result = await conn.query(
            "SELECT @P1 as string_param, @P2 as unicode_param",
            ["Hello World", "Unicode: ñáéíóú🚀"]
        )
        rows = result.rows()
        for row in rows:
            print(f"  Strings: {row['string_param']}, {row['unicode_param']}")
        
        # Numeric parameters
        result = await conn.query(
            "SELECT @P1 as int_param, @P2 as float_param, @P3 as decimal_param",
            [42, 3.14159, 99.99]
        )
        rows = result.rows()
        for row in rows:
            print(f"  Numbers: {row['int_param']}, {row['float_param']}, {row['decimal_param']}")
        
        # Boolean and None parameters
        result = await conn.query(
            "SELECT @P1 as bool_param, @P2 as null_param",
            [True, None]
        )
        rows = result.rows()
        for row in rows:
            print(f"  Special: {row['bool_param']}, {row['null_param']}")
        
        # Date/Time parameters (as strings)
        result = await conn.query(
            "SELECT @P1 as date_param, @P2 as datetime_param",
            ["2024-01-15", "2024-01-15 14:30:00"]
        )
        rows = result.rows()
        for row in rows:
            print(f"  Dates: {row['date_param']}, {row['datetime_param']}")


async def batch_operations_example():
    """
    Example showing efficient batch operations.
    """
    print("\n🔹 Batch Operations Example")
    print("-" * 40)
    
    async with Connection("Server=localhost;Database=TestDB;User Id=testuser;Password=testpass;") as conn:
        
        # Create a temporary table for testing
        await conn.execute("""
            IF OBJECT_ID('tempdb..#batch_test') IS NOT NULL
                DROP TABLE #batch_test
                
            CREATE TABLE #batch_test (
                id INT IDENTITY(1,1) PRIMARY KEY,
                name NVARCHAR(100),
                value DECIMAL(10,2),
                created_date DATETIME2 DEFAULT GETDATE()
            )
        """)
        print("✅ Created temporary table for batch testing")
        
        # Batch insert using multiple individual executes
        print("📦 Inserting batch data...")
        users_data = [
            ("Alice Johnson", 1000.50),
            ("Bob Smith", 2500.75),
            ("Carol Williams", 3200.25),
            ("David Brown", 1800.00),
            ("Eve Davis", 4100.30)
        ]
        
        inserted_count = 0
        for name, value in users_data:
            affected = await conn.execute(
                "INSERT INTO #batch_test (name, value) VALUES (@P1, @P2)",
                [name, value]
            )
            inserted_count += affected
        
        print(f"✅ Inserted {inserted_count} records")
        
        # Verify the batch insert
        result = await conn.query("SELECT COUNT(*) as total FROM #batch_test")
        rows = result.rows()
        for row in rows:
            print(f"📊 Total records in table: {row['total']}")
        
        # Batch update
        affected = await conn.execute(
            "UPDATE #batch_test SET value = value * 1.1 WHERE value < @P1",
            [2000.00]
        )
        print(f"💰 Updated {affected} records with 10% increase")


async def error_handling_example():
    """
    Example showing proper error handling patterns.
    """
    print("\n🔹 Error Handling Example")
    print("-" * 40)
    
    try:
        async with Connection("Server=localhost;Database=TestDB;User Id=testuser;Password=testpass;") as conn:
            
            # Example 1: SQL syntax error
            print("🚨 Testing SQL syntax error handling:")
            try:
                await conn.query("SELCT * FROM invalid_syntax")  # Intentional typo
            except Exception as e:
                print(f"  ✅ Caught SQL syntax error: {type(e).__name__}")
            
            # Example 2: Invalid table name
            print("\n🚨 Testing invalid table error handling:")
            try:
                await conn.query("SELECT * FROM non_existent_table_12345")
            except Exception as e:
                print(f"  ✅ Caught table error: {type(e).__name__}")
            
            # Example 3: Parameter mismatch
            print("\n🚨 Testing parameter mismatch error handling:")
            try:
                await conn.query("SELECT @P1, @P2", [1])  # Missing second parameter
            except Exception as e:
                print(f"  ✅ Caught parameter error: {type(e).__name__}")
            
            print("\n✅ All error handling tests completed successfully")
    
    except Exception as e:
        print(f"❌ Connection error: {e}")


async def performance_tips_example():
    """
    Example demonstrating performance optimization techniques.
    """
    print("\n🔹 Performance Optimization Tips")
    print("-" * 40)
    
    async with Connection("Server=localhost;Database=TestDB;User Id=testuser;Password=testpass;") as conn:
        
        print("⚡ Performance Tips:")
        print("1. Use parameterized queries (always!)")
        print("2. Use appropriate connection pool settings")
        print("3. Use result.rows() to get all results efficiently")
        print("4. Batch operations when possible")
        print("5. Use specific column names instead of SELECT *")
        
        # Example: Efficient large result set processing
        print("\n📊 Processing large result set efficiently:")
        
        # Create test data
        await conn.execute("""
            IF OBJECT_ID('tempdb..#perf_test') IS NOT NULL
                DROP TABLE #perf_test
                
            CREATE TABLE #perf_test (
                id INT IDENTITY(1,1) PRIMARY KEY,
                data NVARCHAR(50)
            )
        """)
        
        # Insert test data
        for i in range(10):
            await conn.execute(
                "INSERT INTO #perf_test (data) VALUES (@P1)",
                [f"Test data row {i+1}"]
            )
        
        # Efficient processing: stream results instead of loading all into memory
        print("  📈 Processing results efficiently:")
        result = await conn.query("SELECT id, data FROM #perf_test ORDER BY id")
        rows = result.rows()
        row_count = len(rows)
        
        for i, row in enumerate(rows):
            if i < 3:  # Show first 3 rows
                print(f"    Row {row['id']}: {row['data']}")
        
        print(f"  ✅ Processed {row_count} rows efficiently")


async def ddl_operations_example():
    """
    Example showing DDL (Data Definition Language) operations.
    """
    print("\n🔹 DDL Operations Example")
    print("-" * 40)
    
    async with Connection("Server=localhost;Database=TestDB;User Id=testuser;Password=testpass;") as conn:
        
        # Create table
        print("🏗️ Creating table...")
        await conn.execute("""
            IF OBJECT_ID('demo_products') IS NOT NULL
                DROP TABLE demo_products
                
            CREATE TABLE demo_products (
                product_id INT IDENTITY(1,1) PRIMARY KEY,
                product_name NVARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                category_id INT,
                created_date DATETIME2 DEFAULT GETDATE(),
                is_active BIT DEFAULT 1
            )
        """)
        print("✅ Table 'demo_products' created")
        
        # Create index
        await conn.execute("""
            CREATE INDEX IX_demo_products_category 
            ON demo_products (category_id)
        """)
        print("✅ Index created")
        
        # Insert sample data
        products = [
            ("Laptop Pro", 1299.99, 1),
            ("Wireless Mouse", 29.99, 2),
            ("USB Cable", 9.99, 2),
            ("Monitor 24inch", 299.99, 1)
        ]
        
        for name, price, category in products:
            await conn.execute(
                "INSERT INTO demo_products (product_name, price, category_id) VALUES (@P1, @P2, @P3)",
                [name, price, category]
            )
        
        print("✅ Sample data inserted")
        
        print("\n📊 Product Inventory:")
        result = await conn.query("""
            SELECT product_id, product_name, price, 
                   FORMAT(price, 'C') as formatted_price,
                   created_date
            FROM demo_products 
            ORDER BY price DESC
        """)
        rows = result.rows()
        for row in rows:
            print(f"  {row['product_name']}: {row['formatted_price']} (ID: {row['product_id']})")
        
        # Clean up
        await conn.execute("DROP TABLE demo_products")
        print("\n🧹 Cleanup completed")


async def main():
    """
    Main function that runs all examples.
    """
    print("🚀 FastMSSQL Comprehensive Examples")
    print("=" * 50)
    print("High-Performance Microsoft SQL Server Driver for Python")
    print("Built with Rust for maximum performance and safety")
    print("=" * 50)
    
    examples = [
        ("Basic Usage", basic_usage_example),
        ("Connection Configuration", connection_configuration_example),
        ("Advanced Configuration", advanced_configuration_example),
        ("Parameter Types", parameter_types_example),
        ("Batch Operations", batch_operations_example),
        ("Error Handling", error_handling_example),
        ("Performance Tips", performance_tips_example),
        ("DDL Operations", ddl_operations_example),
    ]
    
    print("\n📋 Available Examples:")
    for i, (name, _) in enumerate(examples, 1):
        print(f"  {i}. {name}")
    
    print("\n" + "=" * 50)
    print("NOTE: These examples require a running SQL Server instance.")
    print("Update connection strings to match your environment.")
    print("=" * 50)
    
    # Uncomment the following lines to run examples (requires real database)
    # for name, example_func in examples:
    #     try:
    #         await example_func()
    #     except Exception as e:
    #         print(f"\n❌ Error in {name}: {e}")
    #         print("   (This is expected without a real database connection)")
    
    print("\n✅ Example definitions loaded successfully!")
    print("💡 Uncomment the example execution code to run with a real database.")


if __name__ == "__main__":
    # Run the examples
    asyncio.run(main())
