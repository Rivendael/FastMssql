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
- Batch operations (query_batch, execute_batch)
- High-performance bulk inserts
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
    Example showing efficient batch operations including bulk inserts.
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
        
        # === BULK INSERT EXAMPLE ===
        print("\n📦 Bulk Insert Operation:")
        
        # Prepare bulk data (much more efficient than individual inserts)
        columns = ["name", "value"]
        bulk_data = [
            ["Alice Johnson", 1000.50],
            ["Bob Smith", 2500.75],
            ["Carol Williams", 3200.25],
            ["David Brown", 1800.00],
            ["Eve Davis", 4100.30],
            ["Frank Miller", 2750.80],
            ["Grace Wilson", 3900.15],
            ["Henry Taylor", 1650.40],
            ["Ivy Anderson", 4500.90],
            ["Jack Thompson", 2200.60]
        ]
        
        # Perform bulk insert - much faster than individual INSERT statements
        rows_inserted = await conn.bulk_insert("#batch_test", columns, bulk_data)
        print(f"✅ Bulk inserted {rows_inserted} records in one operation")
        
        # === BATCH QUERIES EXAMPLE ===
        print("\n📊 Batch Query Operations:")
        
        # Execute multiple queries in a single round-trip
        batch_queries = [
            ("SELECT COUNT(*) as total_records FROM #batch_test", None),
            ("SELECT AVG(value) as avg_value FROM #batch_test", None),
            ("SELECT MAX(value) as max_value, MIN(value) as min_value FROM #batch_test", None),
            ("SELECT COUNT(*) as high_value_count FROM #batch_test WHERE value > @P1", [3000.00])
        ]
        
        results = await conn.query_batch(batch_queries)
        
        # Process batch results
        total_records = results[0].rows()[0]['total_records']
        avg_value = results[1].rows()[0]['avg_value']
        max_min = results[2].rows()[0]
        high_value_count = results[3].rows()[0]['high_value_count']
        
        print(f"  📈 Total Records: {total_records}")
        print(f"  📈 Average Value: ${avg_value:.2f}")
        print(f"  📈 Value Range: ${max_min['min_value']:.2f} - ${max_min['max_value']:.2f}")
        print(f"  📈 High Value Records (>$3000): {high_value_count}")
        
        # === BATCH COMMANDS EXAMPLE ===
        print("\n🔧 Batch Command Operations:")
        
        # Execute multiple commands in a single round-trip
        batch_commands = [
            ("UPDATE #batch_test SET value = value * 1.1 WHERE value < @P1", [2000.00]),  # 10% increase for lower values
            ("INSERT INTO #batch_test (name, value) VALUES (@P1, @P2)", ["Bonus Record", 5000.00]),
            ("UPDATE #batch_test SET created_date = DATEADD(day, -1, created_date) WHERE name LIKE @P1", ["%Bonus%"])
        ]
        
        affected_counts = await conn.execute_batch(batch_commands)
        
        print(f"  🔄 Updated {affected_counts[0]} records with value increase")
        print(f"  ➕ Inserted {affected_counts[1]} bonus record")
        print(f"  📅 Updated {affected_counts[2]} record dates")
        
        # Verify final state
        verification_result = await conn.query("""
            SELECT 
                COUNT(*) as final_count,
                AVG(value) as final_avg_value,
                MAX(value) as final_max_value
            FROM #batch_test
        """)
        
        final_stats = verification_result.rows()[0]
        print(f"\n📊 Final Statistics:")
        print(f"  Total Records: {final_stats['final_count']}")
        print(f"  Average Value: ${final_stats['final_avg_value']:.2f}")
        print(f"  Maximum Value: ${final_stats['final_max_value']:.2f}")
        
        # Show top records
        top_records_result = await conn.query("""
            SELECT TOP 3 name, value, created_date 
            FROM #batch_test 
            ORDER BY value DESC
        """)
        
        print(f"\n🏆 Top 3 Records by Value:")
        for record in top_records_result.rows():
            print(f"  {record['name']}: ${record['value']:.2f}")
        
        print("\n� Batch Operations Benefits:")
        print("  • Reduced network round-trips")
        print("  • Better performance for bulk operations")
        print("  • Atomic execution for related operations")
        print("  • Optimal resource utilization")


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


async def bulk_insert_example():
    """
    Dedicated example for high-performance bulk insert operations.
    """
    print("\n🔹 High-Performance Bulk Insert Example")
    print("-" * 40)
    
    async with Connection("Server=localhost;Database=TestDB;User Id=testuser;Password=testpass;") as conn:
        
        # Create a table optimized for bulk inserts
        await conn.execute("""
            IF OBJECT_ID('sales_data') IS NOT NULL
                DROP TABLE sales_data
                
            CREATE TABLE sales_data (
                id INT IDENTITY(1,1) PRIMARY KEY,
                product_code NVARCHAR(20),
                product_name NVARCHAR(100),
                quantity INT,
                unit_price DECIMAL(10,2),
                sale_date DATE,
                customer_id INT,
                total_amount AS (quantity * unit_price) PERSISTED
            )
        """)
        print("✅ Created sales_data table for bulk insert demonstration")
        
        # Generate sample sales data (simulating a data import scenario)
        print("📈 Generating sample sales data...")
        
        import random
        from datetime import date, timedelta
        
        products = [
            ("PRD001", "Wireless Headphones", 299.99),
            ("PRD002", "Bluetooth Speaker", 89.99),
            ("PRD003", "USB-C Cable", 19.99),
            ("PRD004", "Power Bank", 49.99),
            ("PRD005", "Phone Case", 24.99),
            ("PRD006", "Screen Protector", 12.99),
            ("PRD007", "Car Charger", 34.99),
            ("PRD008", "Wireless Mouse", 39.99),
            ("PRD009", "Keyboard", 79.99),
            ("PRD010", "Monitor Stand", 159.99)
        ]
        
        # Prepare bulk data for insert
        columns = ["product_code", "product_name", "quantity", "unit_price", "sale_date", "customer_id"]
        sales_data = []
        
        # Generate 1000 sales records
        base_date = date.today() - timedelta(days=30)
        for i in range(1000):
            product_code, product_name, price = random.choice(products)
            quantity = random.randint(1, 5)
            sale_date = (base_date + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
            customer_id = random.randint(1000, 9999)
            
            sales_data.append([product_code, product_name, quantity, price, sale_date, customer_id])
        
        print(f"📊 Prepared {len(sales_data)} sales records for bulk insert")
        
        # Perform bulk insert with timing
        import time
        start_time = time.time()
        
        rows_inserted = await conn.bulk_insert("sales_data", columns, sales_data)
        
        insert_time = time.time() - start_time
        
        print(f"🚀 Bulk inserted {rows_inserted} records in {insert_time:.3f} seconds")
        print(f"⚡ Performance: {rows_inserted / insert_time:.0f} records/second")
        
        # Verify and analyze the inserted data
        print("\n📊 Data Analysis:")
        
        analysis_queries = [
            ("SELECT COUNT(*) as total_sales FROM sales_data", None),
            ("SELECT COUNT(DISTINCT product_code) as unique_products FROM sales_data", None),
            ("SELECT COUNT(DISTINCT customer_id) as unique_customers FROM sales_data", None),
            ("SELECT SUM(total_amount) as total_revenue FROM sales_data", None),
            ("SELECT AVG(total_amount) as avg_order_value FROM sales_data", None)
        ]
        
        analysis_results = await conn.query_batch(analysis_queries)
        
        total_sales = analysis_results[0].rows()[0]['total_sales']
        unique_products = analysis_results[1].rows()[0]['unique_products']
        unique_customers = analysis_results[2].rows()[0]['unique_customers']
        total_revenue = analysis_results[3].rows()[0]['total_revenue']
        avg_order_value = analysis_results[4].rows()[0]['avg_order_value']
        
        print(f"  📈 Total Sales Records: {total_sales:,}")
        print(f"  📦 Unique Products: {unique_products}")
        print(f"  👥 Unique Customers: {unique_customers}")
        print(f"  💰 Total Revenue: ${total_revenue:,.2f}")
        print(f"  🛒 Average Order Value: ${avg_order_value:.2f}")
        
        # Show top-selling products
        top_products_result = await conn.query("""
            SELECT 
                product_name,
                SUM(quantity) as total_quantity_sold,
                SUM(total_amount) as total_product_revenue,
                COUNT(*) as number_of_sales
            FROM sales_data
            GROUP BY product_code, product_name
            ORDER BY total_product_revenue DESC
        """)
        
        print(f"\n🏆 Top-Selling Products by Revenue:")
        for i, product in enumerate(top_products_result.rows()[:5], 1):
            print(f"  {i}. {product['product_name']}: ${product['total_product_revenue']:,.2f} "
                  f"({product['total_quantity_sold']} units, {product['number_of_sales']} sales)")
        
        # Performance comparison note
        print(f"\n💡 Performance Note:")
        print(f"  Individual INSERT statements would require {len(sales_data)} round-trips")
        print(f"  Bulk insert completed in 1 round-trip - significant performance gain!")
        print(f"  Estimated time savings: ~{(len(sales_data) * 0.01):.1f} seconds for individual inserts")
        
        # Cleanup
        await conn.execute("DROP TABLE sales_data")
        print("\n🧹 Cleanup completed")


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
        ("High-Performance Bulk Insert", bulk_insert_example),
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
