#!/usr/bin/env python3
"""
Comprehensive memory usage test for fastmssql to understand its memory characteristics.
Tests various scenarios including connection pooling, query execution, concurrency, and memory leaks.
"""

import asyncio
import gc
import os
import psutil
import tracemalloc
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file if present

@dataclass
class MemorySnapshot:
    """Data class to hold memory metrics"""
    timestamp: float
    rss_mb: float  # Resident Set Size
    vms_mb: float  # Virtual Memory Size
    peak_trace_mb: float  # Peak traced memory
    
    
class MemoryProfiler:
    """Enhanced memory profiling utility with detailed tracking"""
    
    def __init__(self, name: str, verbose: bool = False):
        self.name = name
        self.process = psutil.Process()
        self.start_memory = None
        self.end_memory = None
        self.peak_memory = None
        self.snapshots: List[MemorySnapshot] = []
        self.verbose = verbose
        self.start_time = None
        
    def __enter__(self):
        gc.collect()  # Clean up before measurement
        tracemalloc.start()
        self.start_time = time.perf_counter()
        mem_info = self.process.memory_info()
        self.start_memory = mem_info.rss / 1024 / 1024  # MB
        if self.verbose:
            print(f"  [START] RSS: {self.start_memory:.2f} MB, VMS: {mem_info.vms / 1024 / 1024:.2f} MB")
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.perf_counter() - self.start_time
        
        # Get traced memory right before stopping
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        mem_info = self.process.memory_info()
        self.end_memory = mem_info.rss / 1024 / 1024  # MB
        self.peak_memory = peak / 1024 / 1024  # MB
        self.memory_increase = self.end_memory - self.start_memory
        
        if self.verbose:
            print(f"  [END]   RSS: {self.end_memory:.2f} MB, VMS: {mem_info.vms / 1024 / 1024:.2f} MB")
        
        return {
            'memory_increase': self.memory_increase,
            'peak_memory': self.peak_memory,
            'start_memory': self.start_memory,
            'end_memory': self.end_memory,
            'elapsed_seconds': elapsed,
            'rss_vms_mb': mem_info.vms / 1024 / 1024
        }
    
    def take_snapshot(self, label: str = ""):
        """Take a memory snapshot during execution"""
        mem_info = self.process.memory_info()
        current, peak = tracemalloc.get_traced_memory()
        snapshot = MemorySnapshot(
            timestamp=time.time(),
            rss_mb=mem_info.rss / 1024 / 1024,
            vms_mb=mem_info.vms / 1024 / 1024,
            peak_trace_mb=peak / 1024 / 1024
        )
        self.snapshots.append(snapshot)
        if self.verbose and label:
            print(f"  [SNAP]  {label}: {snapshot.rss_mb:.2f} MB")
        return snapshot


async def test_connection_creation():
    """Test memory overhead of connection creation"""
    from fastmssql import Connection
    
    print("\n📊 Test 1: Connection Creation")
    print("-" * 50)
    
    connection_string = os.getenv('FASTMSSQL_TEST_CONNECTION_STRING')
    if not connection_string:
        print("⚠️  Skipping - FASTMSSQL_TEST_CONNECTION_STRING not set")
        return None
    
    with MemoryProfiler("Connection Creation") as profiler:
        async with Connection(connection_string) as conn:
            stats = await conn.pool_stats()
            print(f"  Pool stats: {stats}")
    
    result = profiler.__exit__(None, None, None)
    print(f"  Memory overhead: {result['memory_increase']:.2f} MB")
    print(f"  Peak traced memory: {result['peak_memory']:.2f} MB")
    print(f"  Elapsed time: {result['elapsed_seconds']:.2f}s")
    
    return result


async def test_sequential_queries(query_count: int = 100):
    """Test memory usage during sequential query execution"""
    from fastmssql import Connection
    
    print("\n📊 Test 2: Sequential Query Execution")
    print("-" * 50)
    
    connection_string = os.getenv('FASTMSSQL_TEST_CONNECTION_STRING')
    if not connection_string:
        print("⚠️  Skipping - FASTMSSQL_TEST_CONNECTION_STRING not set")
        return None
    
    with MemoryProfiler("Sequential Queries", verbose=True) as profiler:
        async with Connection(connection_string) as conn:
            for i in range(query_count):
                result = await conn.execute(
                    "SELECT @i as iteration, @@VERSION as version, NEWID() as id",
                    [i]
                )
                # Ensure results are materialized
                rows = list(result)
                for row in rows:
                    _ = len(str(row))
                
                if (i + 1) % 25 == 0:
                    profiler.take_snapshot(f"After {i + 1} queries")
                    print(f"    Completed {i + 1} queries...")
        
    result = profiler.__exit__(None, None, None)
    print(f"  Total memory overhead: {result['memory_increase']:.2f} MB")
    print(f"  Peak traced memory: {result['peak_memory']:.2f} MB")
    print(f"  Per query: {result['memory_increase'] / query_count * 1024:.3f} KB")
    print(f"  Elapsed time: {result['elapsed_seconds']:.2f}s")
    
    return result


async def test_large_result_sets(batch_size: int = 1000):
    """Test memory usage with large result sets"""
    from fastmssql import Connection
    
    print("\n📊 Test 3: Large Result Sets")
    print("-" * 50)
    
    connection_string = os.getenv('FASTMSSQL_TEST_CONNECTION_STRING')
    if not connection_string:
        print("⚠️  Skipping - FASTMSSQL_TEST_CONNECTION_STRING not set")
        return None
    
    with MemoryProfiler("Large Result Sets", verbose=True) as profiler:
        async with Connection(connection_string) as conn:
            # Test with multiple result sets of varying sizes
            for size_mult in [1, 2, 4, 8]:
                result = await conn.execute(
                    f"""
                    WITH NumberSeries AS (
                        SELECT TOP {batch_size * size_mult}
                            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as num,
                            NEWID() as guid,
                            CONVERT(VARCHAR(100), GETDATE()) as date_str,
                            'Test record ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS VARCHAR) as data
                        FROM sys.objects a
                        CROSS JOIN sys.objects b
                    )
                    SELECT * FROM NumberSeries
                    """
                )
                rows = list(result)
                print(f"    Fetched {len(rows)} rows ({size_mult}x batch size)")
                profiler.take_snapshot(f"After {size_mult}x batch")
        
    result = profiler.__exit__(None, None, None)
    print(f"  Total memory overhead: {result['memory_increase']:.2f} MB")
    print(f"  Peak traced memory: {result['peak_memory']:.2f} MB")
    print(f"  Elapsed time: {result['elapsed_seconds']:.2f}s")
    
    return result


async def test_concurrent_operations(worker_count: int = 20, queries_per_worker: int = 10):
    """Test memory usage under concurrent load"""
    from fastmssql import Connection, PoolConfig
    
    print("\n📊 Test 4: Concurrent Operations")
    print("-" * 50)
    
    connection_string = os.getenv('FASTMSSQL_TEST_CONNECTION_STRING')
    if not connection_string:
        print("⚠️  Skipping - FASTMSSQL_TEST_CONNECTION_STRING not set")
        return None
    
    with MemoryProfiler("Concurrent Operations", verbose=True) as profiler:
        async with Connection(connection_string, PoolConfig.high_throughput()) as conn:
            
            async def worker(worker_id: int):
                results = []
                for i in range(queries_per_worker):
                    result = await conn.execute(
                        "SELECT @worker_id as worker, @i as iteration, @@VERSION as version",
                        [worker_id, i]
                    )
                    results.extend(list(result))
                return results
            
            # Run concurrent workers
            tasks = [worker(i) for i in range(worker_count)]
            all_results = await asyncio.gather(*tasks)
            
            total_rows = sum(len(result) for result in all_results)
            total_operations = worker_count * queries_per_worker
            print(f"    Completed {total_operations} operations, {total_rows} total rows")
            
            stats = await conn.pool_stats()
            print(f"    Final pool stats: {stats}")
            
            profiler.take_snapshot("After all concurrent operations")
        
    result = profiler.__exit__(None, None, None)
    print(f"  Total memory overhead: {result['memory_increase']:.2f} MB")
    print(f"  Peak traced memory: {result['peak_memory']:.2f} MB")
    print(f"  Per operation: {result['memory_increase'] / (worker_count * queries_per_worker) * 1024:.3f} KB")
    print(f"  Elapsed time: {result['elapsed_seconds']:.2f}s")
    
    return result


async def test_memory_leak_detection(batch_count: int = 10, ops_per_batch: int = 100):
    """Test for memory leaks during extended operations"""
    from fastmssql import Connection
    
    print("\n📊 Test 5: Memory Leak Detection")
    print("-" * 50)
    
    connection_string = os.getenv('FASTMSSQL_TEST_CONNECTION_STRING')
    if not connection_string:
        print("⚠️  Skipping - FASTMSSQL_TEST_CONNECTION_STRING not set")
        return None
    
    batch_memories = []
    
    with MemoryProfiler("Memory Leak Test") as profiler:
        async with Connection(connection_string) as conn:
            initial_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            for batch in range(batch_count):
                batch_initial = psutil.Process().memory_info().rss / 1024 / 1024
                
                for i in range(ops_per_batch):
                    result = await conn.execute(
                        """
                        SELECT 
                            @batch as batch_num,
                            @op as operation_num,
                            NEWID() as test_guid,
                            REPLICATE('x', 1000) as padding_data
                        """,
                        [batch, i]
                    )
                    # Process results
                    for row in result:
                        _ = len(str(row))
                
                # Check memory after each batch
                batch_end = psutil.Process().memory_info().rss / 1024 / 1024
                batch_increase = batch_end - batch_initial
                batch_memories.append(batch_increase)
                
                print(f"    Batch {batch + 1:2d}: {batch_increase:7.2f} MB increase "
                      f"(total: {batch_end - initial_memory:7.2f} MB)")
                
                profiler.take_snapshot(f"After batch {batch + 1}")
                
                # Force garbage collection between batches
                gc.collect()
        
    result = profiler.__exit__(None, None, None)
    print(f"  Total memory overhead: {result['memory_increase']:.2f} MB")
    print(f"  Peak traced memory: {result['peak_memory']:.2f} MB")
    print(f"  Per operation: {result['memory_increase'] / (batch_count * ops_per_batch) * 1024:.3f} KB")
    print(f"  Elapsed time: {result['elapsed_seconds']:.2f}s")
    
    # Analyze leak pattern
    if batch_memories:
        avg_batch_increase = sum(batch_memories) / len(batch_memories)
        print(f"\n  Leak Analysis:")
        print(f"    Average memory per batch: {avg_batch_increase:.2f} MB")
        print(f"    First batch: {batch_memories[0]:.2f} MB")
        print(f"    Last batch: {batch_memories[-1]:.2f} MB")
        if batch_memories[-1] < batch_memories[0] * 0.9:
            print(f"    ✅ Memory stabilizing (good sign)")
        elif batch_memories[-1] > batch_memories[0] * 1.1:
            print(f"    ⚠️  Potential memory leak detected")
        else:
            print(f"    ✅ Stable memory usage")
    
    return result


async def test_batch_operations(batch_size: int = 50):
    """Test memory usage during batch operations"""
    from fastmssql import Connection
    
    print("\n📊 Test 6: Batch Operations")
    print("-" * 50)
    
    connection_string = os.getenv('FASTMSSQL_TEST_CONNECTION_STRING')
    if not connection_string:
        print("⚠️  Skipping - FASTMSSQL_TEST_CONNECTION_STRING not set")
        return None
    
    with MemoryProfiler("Batch Operations", verbose=True) as profiler:
        async with Connection(connection_string) as conn:
            # Create test data
            for batch_num in range(5):
                batch_data = []
                for i in range(batch_size):
                    batch_data.append([batch_num, i, f"batch_{batch_num}_item_{i}"])
                
                # Execute batch of parameterized queries
                for params in batch_data:
                    result = await conn.execute(
                        "SELECT @batch as batch, @item as item_num, @label as label",
                        params
                    )
                    _ = list(result)
                
                print(f"    Completed batch {batch_num + 1}")
                profiler.take_snapshot(f"After batch {batch_num + 1}")
        
    result = profiler.__exit__(None, None, None)
    print(f"  Total memory overhead: {result['memory_increase']:.2f} MB")
    print(f"  Peak traced memory: {result['peak_memory']:.2f} MB")
    print(f"  Per batch operation: {result['memory_increase'] / (5 * batch_size) * 1024:.3f} KB")
    print(f"  Elapsed time: {result['elapsed_seconds']:.2f}s")
    
    return result


def format_header(title: str):
    """Format a nice header for output"""
    width = 60
    print("\n" + "=" * width)
    print(f"  {title}".center(width))
    print("=" * width)


def print_summary(results: Dict[str, Optional[Dict]]):
    """Print a comprehensive summary of all tests"""
    format_header("COMPREHENSIVE MEMORY ANALYSIS SUMMARY")
    
    print("\n📈 Test Results:")
    print("-" * 60)
    
    valid_results = {k: v for k, v in results.items() if v is not None}
    
    if not valid_results:
        print("❌ No valid results to display")
        return
    
    # Display individual test results
    for test_name, result in valid_results.items():
        print(f"\n{test_name}:")
        print(f"  Memory Increase:    {result['memory_increase']:>10.2f} MB")
        print(f"  Peak Traced Memory: {result['peak_memory']:>10.2f} MB")
        print(f"  Execution Time:     {result['elapsed_seconds']:>10.2f}s")
    
    # Overall statistics
    print("\n📊 Overall Statistics:")
    print("-" * 60)
    
    if valid_results:
        total_memory = sum(r['memory_increase'] for r in valid_results.values())
        peak_memory = max(r['peak_memory'] for r in valid_results.values())
        total_time = sum(r['elapsed_seconds'] for r in valid_results.values())
        
        print(f"Total Memory Used:    {total_memory:>10.2f} MB")
        print(f"Peak Memory Usage:    {peak_memory:>10.2f} MB")
        print(f"Total Execution Time: {total_time:>10.2f}s")
        print(f"Number of Tests:      {len(valid_results):>10d}")
    
    # Performance assessment
    print("\n🎯 Performance Assessment:")
    print("-" * 60)
    
    if valid_results:
        sequential_result = results.get("Sequential Queries")
        concurrent_result = results.get("Concurrent Operations")
        
        if sequential_result and sequential_result['memory_increase'] < 50:
            print("✅ Excellent sequential query memory efficiency")
        else:
            print("⚠️  Sequential query memory usage is moderate")
        
        if concurrent_result and concurrent_result['memory_increase'] < 50:
            print("✅ Excellent concurrent operation memory efficiency")
        else:
            print("⚠️  Concurrent operation memory usage is moderate")
        
        leak_test = results.get("Memory Leak Test")
        if leak_test:
            per_op_kb = leak_test['memory_increase'] / 1000 * 1024
            if per_op_kb < 0.5:
                print(f"✅ Outstanding memory leak detection ({per_op_kb:.3f} KB/op)")
            elif per_op_kb < 1.0:
                print(f"✅ Good memory leak detection ({per_op_kb:.3f} KB/op)")
            else:
                print(f"⚠️  Monitor memory leak potential ({per_op_kb:.3f} KB/op)")
    
    print("\n" + "=" * 60 + "\n")


async def main():
    """Run all memory tests"""
    try:
        from fastmssql import Connection
    except ImportError:
        print("❌ fastmssql not available - run 'maturin develop --release' first")
        return
    
    # Check connection string
    connection_string = os.getenv('FASTMSSQL_TEST_CONNECTION_STRING')
    if not connection_string:
        print("❌ FASTMSSQL_TEST_CONNECTION_STRING environment variable not set")
        return
    
    print("\n" + "🚀 " * 20)
    format_header("FastMSSQL Comprehensive Memory Test Suite")
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Process PID: {os.getpid()}")
    print("🚀 " * 20)
    
    # Run all tests
    results = {
        "Connection Creation": await test_connection_creation(),
        "Sequential Queries": await test_sequential_queries(query_count=100),
        "Large Result Sets": await test_large_result_sets(batch_size=1000),
        "Concurrent Operations": await test_concurrent_operations(worker_count=20, queries_per_worker=10),
        "Memory Leak Test": await test_memory_leak_detection(batch_count=10, ops_per_batch=100),
        "Batch Operations": await test_batch_operations(batch_size=50),
    }
    
    # Print summary
    print_summary(results)
    print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())
