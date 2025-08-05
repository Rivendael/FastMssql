#!/usr/bin/env python3
"""
Simple memory usage test for fastmssql to understand its memory characteristics.
"""

import asyncio
import gc
import os
import psutil
import tracemalloc

class MemoryProfiler:
    """Simple memory profiling utility"""
    
    def __init__(self, name: str):
        self.name = name
        self.process = psutil.Process()
        self.start_memory = None
        self.peak_memory = None
        
    def __enter__(self):
        gc.collect()  # Clean up before measurement
        tracemalloc.start()
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        end_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        self.peak_memory = peak / 1024 / 1024  # MB
        self.memory_increase = end_memory - self.start_memory
        
        return {
            'memory_increase': self.memory_increase,
            'peak_memory': self.peak_memory,
            'start_memory': self.start_memory,
            'end_memory': end_memory
        }


async def test_fastmssql_memory_usage():
    """Test fastmssql memory usage patterns"""
    
    try:
        from fastmssql import Connection, PoolConfig
    except ImportError:
        print("fastmssql not available - run 'maturin develop' first")
        return
    
    connection_string = os.getenv('CONNECTION_STRING', 
                                 "Server=localhost;Database=master;User Id=sa;Password=YourPassword123")
    
    print("FastMSSQL Memory Usage Analysis")
    print("=" * 40)
    
    # Test 1: Basic connection memory overhead
    print("\nTest 1: Connection Pool Creation")
    with MemoryProfiler("Connection Pool") as profiler:
        async with Connection(connection_string) as conn:
            stats = conn.pool_stats()
            print(f"  Pool created: {stats}")
        
    result1 = profiler.__exit__(None, None, None)
    print(f"  Memory overhead: {result1['memory_increase']:.2f} MB")
    print(f"  Peak memory: {result1['peak_memory']:.2f} MB")
    
    # Test 2: Query execution memory usage
    print("\nTest 2: Query Execution (100 queries)")
    with MemoryProfiler("Query Execution") as profiler:
        async with Connection(connection_string) as conn:
            for i in range(100):
                result = await conn.execute("SELECT @@VERSION, GETDATE(), NEWID(), 'test_string_' + CAST(@i AS VARCHAR)", [i])
                # Ensure results are materialized
                for row in result:
                    _ = len(str(row))
                    
                if i % 25 == 0:
                    print(f"    Completed {i} queries...")
        
    result2 = profiler.__exit__(None, None, None)
    print(f"  Memory overhead: {result2['memory_increase']:.2f} MB")
    print(f"  Peak memory: {result2['peak_memory']:.2f} MB")
    print(f"  Per query: {result2['memory_increase'] / 100 * 1024:.2f} KB")
    
    # Test 3: Concurrent operations memory usage
    print("\nTest 3: Concurrent Operations (20 workers, 5 queries each)")
    with MemoryProfiler("Concurrent Operations") as profiler:
        async with Connection(connection_string, PoolConfig.high_throughput()) as conn:
            
            async def worker(worker_id: int):
                results = []
                for i in range(5):
                    result = await conn.execute("SELECT @worker_id as worker, @i as iteration, @@VERSION", 
                                               [worker_id, i])
                    results.extend(result)
                return results
            
            # Run 20 concurrent workers
            tasks = [worker(i) for i in range(20)]
            all_results = await asyncio.gather(*tasks)
            
            total_rows = sum(len(result) for result in all_results)
            print(f"    Processed {total_rows} total rows")
            
            stats = conn.pool_stats()
            print(f"    Final pool stats: {stats}")
        
    result3 = profiler.__exit__(None, None, None)
    print(f"  Memory overhead: {result3['memory_increase']:.2f} MB")
    print(f"  Peak memory: {result3['peak_memory']:.2f} MB")
    print(f"  Per concurrent operation: {result3['memory_increase'] / 100 * 1024:.2f} KB")
    
    # Test 4: Memory leak test (repeated operations)
    print("\nTest 4: Memory Leak Test (1000 operations)")
    with MemoryProfiler("Memory Leak Test") as profiler:
        async with Connection(connection_string) as conn:
            initial_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            for batch in range(10):  # 10 batches of 100 operations
                for i in range(100):
                    result = await conn.execute("SELECT 'batch_' + @batch + '_op_' + @i as operation", 
                                               [batch, i])
                    # Process result
                    for row in result:
                        _ = row['operation']
                
                # Check memory after each batch
                current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                print(f"    Batch {batch + 1}: {current_memory - initial_memory:.2f} MB increase")
                
                # Force garbage collection
                gc.collect()
        
    result4 = profiler.__exit__(None, None, None)
    print(f"  Total memory overhead: {result4['memory_increase']:.2f} MB")
    print(f"  Peak memory: {result4['peak_memory']:.2f} MB")
    
    # Summary
    print("\n" + "=" * 40)
    print("MEMORY USAGE SUMMARY")
    print("=" * 40)
    print(f"Connection Pool Creation:    {result1['memory_increase']:>8.2f} MB")
    print(f"100 Sequential Queries:      {result1['memory_increase']:>8.2f} MB ({result2['memory_increase']/100*1024:>6.2f} KB/query)")
    print(f"100 Concurrent Operations:   {result3['memory_increase']:>8.2f} MB")
    print(f"1000 Operations (leak test): {result4['memory_increase']:>8.2f} MB")
    print(f"Peak Memory Usage:           {max(result1['peak_memory'], result2['peak_memory'], result3['peak_memory'], result4['peak_memory']):>8.2f} MB")
    
    # Efficiency metrics
    print("\nEfficiency Metrics:")
    print(f"Memory per query (sequential): {result2['memory_increase']/100*1024:.2f} KB")
    print(f"Memory per query (concurrent): {result3['memory_increase']/100*1024:.2f} KB")
    print(f"Memory per query (stress):     {result4['memory_increase']/1000*1024:.2f} KB")
    
    if result4['memory_increase'] < 5.0:  # Less than 5MB increase for 1000 operations
        print("✅ Excellent memory efficiency - minimal memory growth")
    elif result4['memory_increase'] < 10.0:
        print("✅ Good memory efficiency")
    else:
        print("⚠️  High memory usage detected")


if __name__ == "__main__":
    asyncio.run(test_fastmssql_memory_usage())
