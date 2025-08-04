#!/usr/bin/env python3
"""
Advanced connection pool configuration example for pymssql-rs.

This example demonstrates how to configure and monitor the bb8 connection pool
for different scenarios and workloads.
"""

import asyncio
import time
import concurrent.futures
from mssql_python_rust import Connection, PoolConfig

# Connection string - adjust as needed for your environment
CONNECTION_STRING = "Server=SNOWFLAKE\\SQLEXPRESS,50014;Database=pymssql_test;Integrated Security=true;TrustServerCertificate=yes"

def basic_pool_config_example():
    """Demonstrates basic pool configuration options with context managers."""
    print("=== Basic Pool Configuration Example ===")
    
    # Create a custom pool configuration
    pool_config = PoolConfig(
        max_size=5,           # Maximum 5 connections
        min_idle=1,           # Keep at least 1 idle connection
        max_lifetime_secs=600,    # Connections live max 10 minutes
        idle_timeout_secs=180,    # Idle connections timeout after 3 minutes
        connection_timeout_secs=15  # Wait max 15 seconds for a connection
    )
    
    print(f"Pool Config: {pool_config}")
    
    # Create connection with custom pool config using context manager
    with Connection(CONNECTION_STRING, pool_config) as conn:
        print(f"Connected: {conn.is_connected()}")
        
        # Check initial pool stats
        stats = conn.pool_stats()
        print(f"Initial pool stats: {stats}")
        
        # Execute some queries to see pool in action
        for i in range(3):
            result = conn.execute("SELECT GETDATE() as current_time")
            stats = conn.pool_stats()
            print(f"After query {i+1}: {stats}")
            time.sleep(0.5)
    
    print("Connection automatically closed")

def predefined_configurations_example():
    """Demonstrates using predefined pool configurations with context managers."""
    print("\n=== Predefined Configurations Example ===")
    
    # Test different predefined configurations
    configs = {
        "High Throughput": PoolConfig.high_throughput(),
        "Low Resource": PoolConfig.low_resource(),
        "Development": PoolConfig.development()
    }
    
    for name, config in configs.items():
        print(f"\n{name} Configuration:")
        print(f"  {config}")
        
        try:
            with Connection(CONNECTION_STRING, config) as conn:
                # Run a quick test
                result = conn.execute("SELECT 'Testing {}' as message".format(name))
                stats = conn.pool_stats()
                print(f"  Stats: {stats}")
                print(f"  Result: {result[0]['message']}")
                
        except Exception as e:
            print(f"  Error: {e}")

async def concurrent_load_testing():
    """Test pool behavior under concurrent load using async context manager."""
    print("\n=== Concurrent Load Testing ===")
    
    # Use high-throughput config for this test
    pool_config = PoolConfig.high_throughput()
    
    async with Connection(CONNECTION_STRING, pool_config) as conn:
        async def worker(worker_id):
            """Simulates a worker that performs database operations."""
            start_time = time.time()
            try:
                # Simulate different types of operations
                if worker_id % 3 == 0:
                    # Query operation
                    result = await conn.execute(f"SELECT {worker_id} as worker_id, GETDATE() as timestamp")
                    operation = "Query"
                    details = result[0]['worker_id']
                elif worker_id % 3 == 1:
                    # Non-query operation (if you have a test table)
                    affected = await conn.execute(f"-- Worker {worker_id} placeholder operation")
                    operation = "NonQuery"
                    details = f"affected: {affected}"
                else:
                    # Another query
                    result = await conn.execute("SELECT @@VERSION as version")
                    operation = "Version"
                    details = "version check"
                
                end_time = time.time()
                return f"Worker {worker_id:2d}: {operation:8s} - {details} (took {end_time - start_time:.3f}s)"
                
            except Exception as e:
                end_time = time.time()
                return f"Worker {worker_id:2d}: ERROR    - {e} (took {end_time - start_time:.3f}s)"
        
        # Create 20 concurrent workers
        print("Starting 20 concurrent workers...")
        start_time = time.time()
        
        tasks = [worker(i) for i in range(1, 21)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = time.time()
        
        print(f"Completed in {end_time - start_time:.3f} seconds:")
        for result in results:
            if isinstance(result, Exception):
                print(f"  Exception: {result}")
            else:
                print(f"  {result}")
        
        # Check final pool stats
        final_stats = conn.pool_stats()
        print(f"\nFinal pool stats: {final_stats}")
    
    print("Connection automatically closed")

def pool_monitoring_example():
    """Demonstrates how to monitor pool statistics over time using context managers."""
    print("\n=== Pool Monitoring Example ===")
    
    pool_config = PoolConfig(
        max_size=8,
        min_idle=2,
        idle_timeout_secs=30,  # Short timeout for demo
        connection_timeout_secs=10
    )
    
    with Connection(CONNECTION_STRING, pool_config) as conn:
        print("Monitoring pool statistics during operations...")
        
        def print_stats(label):
            stats = conn.pool_stats()
            print(f"{label:20s}: {stats}")
        
        print_stats("Initial state")
        
        # Execute queries and monitor pool changes
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            def run_query(query_id):
                time.sleep(0.1)  # Simulate some work
                result = conn.execute(f"SELECT {query_id} as id, GETDATE() as timestamp")
                return result[0]['id']
            
            # Submit multiple tasks to see pool scaling
            futures = []
            for i in range(6):
                future = executor.submit(run_query, i)
                futures.append(future)
                time.sleep(0.2)  # Stagger submissions
                print_stats(f"After submitting {i+1}")
            
            # Wait for completion
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                print_stats(f"Query {result} completed")
        
        print_stats("All queries done")
        
        # Wait a bit to see idle timeout effects
        print("Waiting 35 seconds to observe idle timeouts...")
        time.sleep(35)
        print_stats("After idle timeout")
    
    print("Connection automatically closed")

def configuration_validation_example():
    """Demonstrates configuration validation."""
    print("\n=== Configuration Validation Example ===")
    
    # Valid configuration
    try:
        config = PoolConfig(max_size=10, min_idle=5)
        print(f"Valid config: {config}")
    except Exception as e:
        print(f"Error with valid config: {e}")
    
    # Invalid configuration - min_idle > max_size
    try:
        config = PoolConfig(max_size=5, min_idle=10)
        print(f"Invalid config created: {config}")
    except Exception as e:
        print(f"Expected error with invalid config: {e}")
    
    # Invalid configuration - max_size = 0
    try:
        config = PoolConfig(max_size=0)
        print(f"Invalid config created: {config}")
    except Exception as e:
        print(f"Expected error with zero max_size: {e}")
    
    # Dynamic configuration changes
    try:
        config = PoolConfig(max_size=10, min_idle=2)
        print(f"Initial config: {config}")
        
        # Valid change
        config.max_size = 15
        print(f"After increasing max_size: {config}")
        
        # Invalid change - would make min_idle > max_size
        config.max_size = 1  # This should fail
        print(f"After invalid change: {config}")
    except Exception as e:
        print(f"Expected error with invalid dynamic change: {e}")

if __name__ == "__main__":
    # Run all examples
    basic_pool_config_example()
    predefined_configurations_example()
    
    # Run async example
    asyncio.run(concurrent_load_testing())
    
    pool_monitoring_example()
    configuration_validation_example()
    
    print("\n=== Pool Configuration Summary ===")
    print("✓ Configurable pool size (max_size)")
    print("✓ Minimum idle connections (min_idle)")
    print("✓ Connection lifetime management (max_lifetime_secs)")
    print("✓ Idle timeout handling (idle_timeout_secs)")
    print("✓ Connection timeout (connection_timeout_secs)")
    print("✓ Predefined configurations for common scenarios")
    print("✓ Real-time pool statistics monitoring")
    print("✓ Configuration validation")
    print("✓ Thread-safe concurrent access")
    print("✓ Automatic resource cleanup")
