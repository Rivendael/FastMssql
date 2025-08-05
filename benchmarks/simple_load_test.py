#!/usr/bin/env python3
"""
Simple load test using a single connection pool for accurate results.
"""

import asyncio
import time
import os
import sys

# Add the python directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python'))

from fastmssql import Connection, PoolConfig


async def simple_load_test(connection_string: str, workers: int = 10, duration: int = 15):
    """Run a simple load test with proper connection pooling."""
    
    print(f"🎯 Simple Load Test:")
    print(f"   Workers: {workers}")
    print(f"   Duration: {duration}s")
    print(f"   Query: SELECT @@VERSION")
    
    # Single connection object with high throughput performance pool config
    pool_config = PoolConfig.high_throughput()
    
    request_count = 0
    error_count = 0
    response_times = []
    
    async with Connection(connection_string, pool_config) as conn:
        async def worker(worker_id: int):
            """Worker that executes queries using the shared connection pool."""
            nonlocal request_count, error_count
            local_requests = 0
            
            end_time = time.time() + duration
            
            while time.time() < end_time:
                start_time = time.time()
                try:
                    result = await conn.execute("SELECT @@VERSION")
                    # Force consumption of results
                    if result.has_rows():
                        list(result.rows())
                    
                    response_time = time.time() - start_time
                    response_times.append(response_time)
                    local_requests += 1
                    
                except Exception as e:
                    error_count += 1
                    print(f"Worker {worker_id} error: {e}")
                
                # Small delay to prevent overwhelming
                await asyncio.sleep(0.001)
            
            request_count += local_requests
            print(f"Worker {worker_id} completed {local_requests} requests")
        
        print("Starting workers...")
        start_time = time.time()
        
        # Start all workers
        worker_tasks = [asyncio.create_task(worker(i)) for i in range(workers)]
        
        # Wait for all workers to complete
        await asyncio.gather(*worker_tasks)
        
        actual_duration = time.time() - start_time
    
    # Calculate results
    rps = request_count / actual_duration
    error_rate = (error_count / (request_count + error_count)) * 100 if (request_count + error_count) > 0 else 0
    avg_response_time = sum(response_times) / len(response_times) if response_times else 0
    
    print(f"\n📊 Results:")
    print(f"   Total Requests: {request_count:,}")
    print(f"   Errors: {error_count}")
    print(f"   Duration: {actual_duration:.2f}s")
    print(f"   RPS: {rps:.2f}")
    print(f"   Error Rate: {error_rate:.2f}%")
    print(f"   Avg Response Time: {avg_response_time*1000:.2f}ms")
    
    return rps


async def main():
    """Run simple load tests."""
    from dotenv import load_dotenv
    load_dotenv()
    # Try to get connection string from environment
    connection_string = os.getenv('TEST_CONNECTION_STRING')
    
    if not connection_string:
        print("❌ No connection string found!")
        print("Please set the MSSQL_CONNECTION_STRING environment variable.")
        print("Example:")
        print('  set MSSQL_CONNECTION_STRING="Server=localhost,1433;Database=master;User Id=sa;Password=YourPassword;TrustServerCertificate=true;"')
        print("\nOr for Windows Authentication:")
        print('  set MSSQL_CONNECTION_STRING="Server=localhost;Database=master;Integrated Security=true;TrustServerCertificate=true;"')
        return
    
    # Test different worker counts
    scenarios = [
        {"workers": 5, "duration": 15},
        {"workers": 10, "duration": 15},
        {"workers": 20, "duration": 15},
    ]
    
    results = []
    
    for scenario in scenarios:
        print("\n" + "="*50)
        rps = await simple_load_test(
            connection_string=connection_string,
            workers=scenario["workers"],
            duration=scenario["duration"]
        )
        results.append((scenario["workers"], rps))
        
        # Wait between tests
        await asyncio.sleep(2)
    
    # Summary
    print("\n" + "="*50)
    print("SUMMARY")
    print("="*50)
    print(f"{'Workers':<10} {'RPS':<10}")
    print("-" * 20)
    
    for workers, rps in results:
        print(f"{workers:<10} {rps:<10.1f}")


if __name__ == "__main__":
    asyncio.run(main())
