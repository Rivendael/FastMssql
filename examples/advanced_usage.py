#!/usr/bin/env python3
"""
Advanced usage example for mssql-python-rust

This example demonstrates advanced features like transactions, bulk operations, and async patterns.
"""

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

def performance_test(connection_string: str, num_queries: int = 100):
    """Test query performance."""
    print(f"\n=== Performance Test: {num_queries} queries ===")
    
    start_time = time.time()
    
    with mssql.connect(connection_string) as conn:
        for i in range(num_queries):
            rows = conn.execute(f"SELECT {i} as query_number, GETDATE() as timestamp")
            # Process the first row
            if rows:
                row = rows[0]
                # Access data to ensure it's processed
                _ = row['query_number']
    
    end_time = time.time()
    duration = end_time - start_time
    queries_per_sec = num_queries / duration
    
    print(f"Executed {num_queries} queries in {duration:.3f} seconds")
    print(f"Performance: {queries_per_sec:.1f} queries/second")

def bulk_insert_simulation(connection_string: str):
    """Simulate bulk insert operations."""
    print("\n=== Bulk Insert Simulation ===")
    
    # Create a temporary table for testing
    create_table_sql = """
    IF OBJECT_ID('tempdb..#test_bulk', 'U') IS NOT NULL
        DROP TABLE #test_bulk
    
    CREATE TABLE #test_bulk (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(100),
        value FLOAT,
        created_date DATETIME2 DEFAULT GETDATE()
    )
    """
    
    try:
        with mssql.connect(connection_string) as conn:
            # Create the table
            conn.execute_non_query(create_table_sql)
            print("Created temporary table #test_bulk")
            
            # Insert multiple rows
            insert_count = 50
            start_time = time.time()
            
            for i in range(insert_count):
                insert_sql = f"""
                INSERT INTO #test_bulk (name, value) 
                VALUES ('Record_{i}', {i * 3.14159})
                """
                conn.execute_non_query(insert_sql)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Query the results
            rows = conn.execute("SELECT COUNT(*) as total_count FROM #test_bulk")
            total_count = rows[0]['total_count'] if rows else 0
            
            print(f"Inserted {insert_count} rows in {duration:.3f} seconds")
            print(f"Table now contains {total_count} rows")
            
            # Sample some data
            sample_rows = conn.execute("""
                SELECT TOP 5 id, name, value, created_date 
                FROM #test_bulk 
                ORDER BY id
            """)
            
            print("\nSample data:")
            for row in sample_rows:
                print(f"  ID: {row['id']}, Name: {row['name']}, Value: {row['value']:.2f}")
                
    except Exception as e:
        print(f"Bulk insert test failed: {e}")

def connection_pooling_simulation(connection_string: str):
    """Simulate connection pooling behavior."""
    print("\n=== Connection Management Test ===")
    
    # Test multiple sequential connections
    print("Testing sequential connections...")
    for i in range(5):
        try:
            with mssql.connect(connection_string) as conn:
                rows = conn.execute(f"SELECT {i} as connection_number")
                if rows:
                    print(f"Connection {i}: {rows[0]['connection_number']}")
        except Exception as e:
            print(f"Connection {i} failed: {e}")
    
    # Test connection reuse
    print("\nTesting connection reuse...")
    try:
        conn = mssql.connect(connection_string)
        conn.connect()
        
        for i in range(3):
            rows = conn.execute(f"SELECT {i} as reuse_test")
            if rows:
                print(f"Reuse test {i}: {rows[0]['reuse_test']}")
        
        conn.disconnect()
        print("Connection reuse test completed")
        
    except Exception as e:
        print(f"Connection reuse test failed: {e}")

def data_type_test(connection_string: str):
    """Test different SQL Server data types."""
    print("\n=== Data Type Test ===")
    
    test_queries = [
        ("Integer types", "SELECT CAST(42 as INT) as int_val, CAST(999999999 as BIGINT) as bigint_val"),
        ("Float types", "SELECT CAST(3.14159 as FLOAT) as float_val, CAST(2.71828 as REAL) as real_val"),
        ("String types", "SELECT 'Hello' as varchar_val, N'Unicode: 你好' as nvarchar_val"),
        ("Date types", "SELECT GETDATE() as datetime_val, CAST('2024-01-01' as DATE) as date_val"),
        ("Boolean type", "SELECT CAST(1 as BIT) as true_val, CAST(0 as BIT) as false_val"),
        ("NULL values", "SELECT NULL as null_val, 'Not NULL' as not_null_val"),
    ]
    
    try:
        with mssql.connect(connection_string) as conn:
            for test_name, query in test_queries:
                print(f"\n{test_name}:")
                rows = conn.execute(query)
                if rows:
                    row = rows[0]
                    for col_name in row.columns():
                        value = row.get(col_name)
                        print(f"  {col_name}: {value} (type: {type(value)})")
                        
    except Exception as e:
        print(f"Data type test failed: {e}")

def main():
    """Main function for advanced examples."""
    
    # Connection string - adjust as needed
    connection_string = "Server=localhost;Database=master;Integrated Security=true"
    
    print(f"mssql-python-rust Advanced Examples")
    print(f"Version: {mssql.version()}")
    
    try:
        # Test basic connectivity first
        print("\n=== Connectivity Test ===")
        with mssql.connect(connection_string) as conn:
            rows = conn.execute("SELECT 'Connection successful' as status")
            if rows:
                print(f"Status: {rows[0]['status']}")
        
        # Run various tests
        data_type_test(connection_string)
        performance_test(connection_string, 50)
        bulk_insert_simulation(connection_string)
        connection_pooling_simulation(connection_string)
        
        print("\n=== All advanced examples completed! ===")
        
    except Exception as e:
        print(f"Advanced examples failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
