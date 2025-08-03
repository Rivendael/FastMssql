#!/usr/bin/env python3
"""
Advanced usage example for mssql-python-rust

This example demonstrates advanced async patterns, performance testing, and concurrent operations.
"""

import asyncio
import os
import sys
import time
from typing import List, Dict, Any

# Add the parent directory to Python path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    from mssql_python_rust import Connection
except ImportError as e:
    print(f"Error importing mssql_python_rust: {e}")
    print("Make sure you've built the extension with 'maturin develop'")
    sys.exit(1)

async def performance_test(connection_string: str, num_queries: int = 100):
    """Test async query performance."""
    print(f"\n=== Performance Test: {num_queries} queries ===")
    
    start_time = time.time()
    
    async with Connection(connection_string) as conn:
        for i in range(num_queries):
            rows = await conn.execute(f"SELECT {i} as query_number, GETDATE() as timestamp")
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

async def concurrent_performance_test(connection_string: str, num_queries: int = 100):
    """Test concurrent query performance."""
    print(f"\n=== Concurrent Performance Test: {num_queries} queries ===")
    
    async def single_query(query_id: int):
        async with Connection(connection_string) as conn:
            return await conn.execute(f"SELECT {query_id} as query_number, GETDATE() as timestamp")
    
    start_time = time.time()
    
    # Execute all queries concurrently
    tasks = [single_query(i) for i in range(num_queries)]
    results = await asyncio.gather(*tasks)
    
    end_time = time.time()
    duration = end_time - start_time
    queries_per_sec = num_queries / duration
    
    print(f"Executed {num_queries} concurrent queries in {duration:.3f} seconds")
    print(f"Performance: {queries_per_sec:.1f} queries/second")
    
    return len([r for r in results if r])

async def batch_operations_example(connection_string: str):
    """Demonstrate batch operations using async patterns."""
    print("\n=== Batch Operations Example ===")
    
    # Simulate batch processing with multiple operations
    operations = [
        "SELECT 'Operation 1' as name, 100 as value",
        "SELECT 'Operation 2' as name, 200 as value",
        "SELECT 'Operation 3' as name, 300 as value",
        "SELECT 'Operation 4' as name, 400 as value",
        "SELECT 'Operation 5' as name, 500 as value"
    ]
    
    async def process_batch(batch_operations: List[str]):
        """Process a batch of operations on a single connection."""
        async with Connection(connection_string) as conn:
            results = []
            for i, operation in enumerate(batch_operations):
                result = await conn.execute(operation)
                if result:
                    results.append({
                        'batch_item': i + 1,
                        'name': result[0]['name'],
                        'value': result[0]['value']
                    })
            return results
    
    start_time = time.time()
    results = await process_batch(operations)
    duration = time.time() - start_time
    
    print(f"Processed {len(operations)} operations in batch:")
    for result in results:
        print(f"  {result['batch_item']}: {result['name']} = {result['value']}")
    
    print(f"Batch processing took {duration:.3f} seconds")

async def connection_pooling_simulation(connection_string: str):
    """Simulate connection pooling behavior."""
    print("\n=== Connection Pooling Simulation ===")
    
    async def worker(worker_id: int, num_tasks: int):
        """Worker that performs multiple database tasks."""
        results = []
        for task_id in range(num_tasks):
            async with Connection(connection_string) as conn:
                result = await conn.execute(
                    f"SELECT {worker_id} as worker_id, {task_id} as task_id, "
                    f"GETDATE() as timestamp, @@SPID as connection_id"
                )
                if result:
                    results.append({
                        'worker_id': worker_id,
                        'task_id': task_id,
                        'connection_id': result[0]['connection_id'],
                        'timestamp': result[0]['timestamp']
                    })
                
                # Small delay to simulate work
                await asyncio.sleep(0.01)
        
        return results
    
    # Run multiple workers concurrently
    num_workers = 5
    tasks_per_worker = 3
    
    start_time = time.time()
    worker_tasks = [
        worker(worker_id, tasks_per_worker) 
        for worker_id in range(num_workers)
    ]
    
    all_results = await asyncio.gather(*worker_tasks)
    duration = time.time() - start_time
    
    # Flatten results
    flattened_results = [result for worker_results in all_results for result in worker_results]
    
    print(f"Completed {len(flattened_results)} tasks across {num_workers} workers in {duration:.3f}s")
    
    # Analyze connection usage
    unique_connections = set(r['connection_id'] for r in flattened_results)
    print(f"Used {len(unique_connections)} unique database connections")
    
    return flattened_results

async def error_recovery_example(connection_string: str):
    """Demonstrate error handling and recovery patterns."""
    print("\n=== Error Recovery Example ===")
    
    async def operation_with_retry(operation_name: str, sql: str, max_retries: int = 3):
        """Execute an operation with retry logic."""
        for attempt in range(max_retries):
            try:
                async with Connection(connection_string) as conn:
                    result = await conn.execute(sql)
                    print(f"✅ {operation_name} succeeded on attempt {attempt + 1}")
                    return result
            except Exception as e:
                print(f"❌ {operation_name} failed on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.1)  # Brief delay before retry
                else:
                    print(f"🚫 {operation_name} failed after {max_retries} attempts")
                    raise
    
    # Test successful operation
    await operation_with_retry(
        "Valid Query", 
        "SELECT 'Success!' as message, GETDATE() as timestamp"
    )
    
    # Test operation that will fail
    try:
        await operation_with_retry(
            "Invalid Query", 
            "SELECT * FROM non_existent_table_xyz"
        )
    except Exception:
        print("Expected failure handled gracefully")

async def data_processing_pipeline(connection_string: str):
    """Demonstrate a data processing pipeline using async patterns."""
    print("\n=== Data Processing Pipeline Example ===")
    
    # Step 1: Extract data
    async def extract_data():
        """Extract data from the database."""
        async with Connection(connection_string) as conn:
            return await conn.execute("""
                SELECT 
                    1 as id, 'Alice' as name, 25 as age, 'Engineer' as role
                UNION ALL SELECT 
                    2 as id, 'Bob' as name, 30 as age, 'Manager' as role
                UNION ALL SELECT 
                    3 as id, 'Carol' as name, 28 as age, 'Designer' as role
            """)
    
    # Step 2: Transform data
    async def transform_data(raw_data: List[Dict[str, Any]]):
        """Transform the extracted data."""
        transformed = []
        for row in raw_data:
            transformed.append({
                'employee_id': row['id'],
                'full_name': row['name'],
                'age_group': 'Young' if row['age'] < 30 else 'Experienced',
                'department': row['role'],
                'processed_at': time.time()
            })
        return transformed
    
    # Step 3: Load/validate data
    async def validate_data(transformed_data: List[Dict[str, Any]]):
        """Validate the transformed data."""
        valid_records = []
        for record in transformed_data:
            # Simulate async validation (e.g., checking against another database)
            await asyncio.sleep(0.01)
            if record['employee_id'] and record['full_name']:
                valid_records.append(record)
        return valid_records
    
    # Execute the pipeline
    print("Starting data processing pipeline...")
    
    # Extract
    start_time = time.time()
    raw_data = await extract_data()
    print(f"✅ Extracted {len(raw_data)} records")
    
    # Transform
    transformed_data = await transform_data(raw_data)
    print(f"✅ Transformed {len(transformed_data)} records")
    
    # Validate
    valid_data = await validate_data(transformed_data)
    print(f"✅ Validated {len(valid_data)} records")
    
    duration = time.time() - start_time
    print(f"Pipeline completed in {duration:.3f} seconds")
    
    # Display results
    for record in valid_data:
        print(f"  Employee {record['employee_id']}: {record['full_name']} ({record['age_group']}) - {record['department']}")

async def main():
    """Main function demonstrating advanced async patterns."""
    
    # Connection string - adjust as needed for your environment
    connection_string = "Server=localhost;Database=master;Integrated Security=true"
    
    print("mssql-python-rust Advanced Usage Examples")
    print("Demonstrating advanced async patterns and performance")
    
    try:
        # Performance tests
        await performance_test(connection_string, 50)
        await concurrent_performance_test(connection_string, 50)
        
        # Advanced patterns
        await batch_operations_example(connection_string)
        await connection_pooling_simulation(connection_string)
        await error_recovery_example(connection_string)
        await data_processing_pipeline(connection_string)
        
        print("\n" + "="*60)
        print("ALL ADVANCED EXAMPLES COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("\nAdvanced async patterns demonstrated:")
        print("1. Sequential vs concurrent performance comparison")
        print("2. Batch processing with single connection")
        print("3. Connection pooling simulation")
        print("4. Error handling and retry patterns")
        print("5. Data processing pipelines")
        print("6. High-performance concurrent operations")
        
        return 0
        
    except Exception as e:
        print(f"Advanced examples failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure SQL Server is running")
        print("2. Check your connection string")
        print("3. Verify network connectivity")
        print("4. Check authentication method")
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
