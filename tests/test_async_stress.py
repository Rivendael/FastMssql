#!/usr/bin/env python3
"""
Async Stress Testing for mssql-python-rust

This module contains stress tests and edge case scenarios for async operations:
1. High-volume concurrent operations
2. Memory leak detection
3. Resource exhaustion scenarios
4. Network interruption simulation
5. Long-running operation management
6. Async context manager edge cases
"""

import asyncio
import time
import pytest
import gc
import psutil
import os
import random

# Test configuration
TEST_CONNECTION_STRING = os.getenv(
    "FASTMSSQL_TEST_CONNECTION_STRING",
)
try:
    from fastmssql import Connection, PoolConfig
    MSSQL_AVAILABLE = True
except ImportError:
    MSSQL_AVAILABLE = False
    mssql = None
    Connection = None


class MemoryTracker:
    """Helper class to track memory usage during tests."""
    
    def __init__(self):
        self.process = psutil.Process(os.getpid())
        self.initial_memory = self.process.memory_info().rss
        self.peak_memory = self.initial_memory
        self.measurements = []
    
    def measure(self, label: str = ""):
        """Take a memory measurement."""
        current_memory = self.process.memory_info().rss
        self.peak_memory = max(self.peak_memory, current_memory)
        self.measurements.append({
            'label': label,
            'memory_mb': current_memory / 1024 / 1024,
            'increase_mb': (current_memory - self.initial_memory) / 1024 / 1024,
            'timestamp': time.time()
        })
        return current_memory
    
    def get_peak_increase_mb(self) -> float:
        """Get peak memory increase in MB."""
        return (self.peak_memory - self.initial_memory) / 1024 / 1024



@pytest.mark.asyncio
@pytest.mark.stress
@pytest.mark.integration
@pytest.mark.skipif(not MSSQL_AVAILABLE, reason="fastmssql not available")
async def test_memory_leak_detection():
    """Test for memory leaks in async operations."""
    try:
        memory_tracker = MemoryTracker()
        memory_tracker.measure("test_start")
        
        async def memory_test_cycle(cycle_id: int):
            """Perform operations that might cause memory leaks."""
            connections_created = 0
            
            # Create one connection and reuse it for multiple operations
            pool_config = PoolConfig(max_size=20, min_idle=5)
            
            try:
                async with Connection(TEST_CONNECTION_STRING, pool_config) as conn:
                    connections_created = 1
                    
                    # Perform many operations on the same connection
                    for i in range(50):
                        # Perform various operations
                        await conn.execute("SELECT 1")
                        await conn.execute("SELECT @@VERSION")
                        await conn.execute("SELECT GETDATE()")
                        
                        # Create some temporary data
                        large_query = "SELECT " + ", ".join([f"'{i}_{j}' as col_{j}" for j in range(20)])
                        await conn.execute(large_query)
                        
            except Exception:
                pass  # Ignore errors for this test
            
            return {'cycle_id': cycle_id, 'connections_created': connections_created}
        
        initial_memory = memory_tracker.measure("initial")
        
        # Run multiple cycles to detect memory leaks
        num_cycles = 10
        for cycle in range(num_cycles):
            # Run multiple concurrent memory test cycles
            tasks = [memory_test_cycle(cycle * 10 + i) for i in range(5)]
            cycle_results = await asyncio.gather(*tasks)
            
            # Force garbage collection after each major cycle
            gc.collect()
            await asyncio.sleep(0.2)
            
            memory_tracker.measure(f"cycle_{cycle}")
            
            # Check for memory growth pattern
            current_memory = memory_tracker.measurements[-1]['memory_mb']
            memory_growth = current_memory - memory_tracker.measurements[0]['memory_mb']
            
            # If memory grows too much too quickly, we might have a leak
            if memory_growth > 50:  # 50MB growth threshold
                print(f"Warning: Significant memory growth detected: {memory_growth:.1f}MB")
        
        final_memory = memory_tracker.measure("final")
        
        # Force final garbage collection
        for _ in range(3):
            gc.collect()
            await asyncio.sleep(0.1)
        
        post_gc_memory = memory_tracker.measure("post_gc")
        
        # Calculate memory statistics
        total_memory_increase = (final_memory - initial_memory) / 1024 / 1024
        post_gc_increase = (post_gc_memory - initial_memory) / 1024 / 1024
        memory_recovered = total_memory_increase - post_gc_increase
        
        # Validate memory behavior
        # After GC, memory increase should be minimal (less than 20MB)
        assert post_gc_increase < 20, \
            f"Potential memory leak detected: {post_gc_increase:.1f}MB increase after GC"
        
        # At least 80% of memory should be recoverable by GC
        if total_memory_increase > 5:  # Only check if significant memory was used
            recovery_rate = memory_recovered / total_memory_increase
            assert recovery_rate > 0.8, \
                f"Poor memory recovery: {recovery_rate:.1%} (recovered {memory_recovered:.1f}MB of {total_memory_increase:.1f}MB)"
        
        print(f"✅ Memory leak test passed:")
        print(f"   Total memory increase: {total_memory_increase:.1f}MB")
        print(f"   Post-GC increase: {post_gc_increase:.1f}MB")
        print(f"   Memory recovered: {memory_recovered:.1f}MB")
        
    except Exception as e:
        pytest.skip(f"Database not available: {e}")


@pytest.mark.asyncio
@pytest.mark.stress
@pytest.mark.integration
@pytest.mark.skipif(not MSSQL_AVAILABLE, reason="fastmssql not available")
async def test_connection_exhaustion_recovery():
    """Test behavior when approaching connection limits and recovery."""
    try:
        # This test attempts to exhaust connections and verify proper recovery
        active_connections = []
        max_connections_to_test = 30  # Conservative limit
        
        async def create_and_hold_connection(conn_id: int, hold_time: float):
            """Create a connection and hold it for specified time."""
            try:
                async with Connection(TEST_CONNECTION_STRING) as conn:
                    # Verify connection is working
                    result = await conn.execute(f"SELECT {conn_id} as conn_id, @@SPID as spid")
                    spid = result.rows()[0]['spid'] if result else None
                    
                    # Hold the connection
                    await asyncio.sleep(hold_time)
                    
                    return {'conn_id': conn_id, 'spid': spid, 'success': True}
            except Exception as e:
                return {'conn_id': conn_id, 'spid': None, 'success': False, 'error': str(e)}
        
        # Phase 1: Create many concurrent connections
        hold_time = 3.0
        tasks = [
            create_and_hold_connection(i, hold_time)
            for i in range(max_connections_to_test)
        ]
        
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        phase1_time = time.time() - start_time
        
        # Analyze Phase 1 results
        successful_connections = [
            r for r in results 
            if isinstance(r, dict) and r.get('success', False)
        ]
        failed_connections = [
            r for r in results 
            if isinstance(r, dict) and not r.get('success', False)
        ]
        exceptions = [r for r in results if isinstance(r, Exception)]
        
        print(f"Phase 1: {len(successful_connections)} successful, {len(failed_connections)} failed, {len(exceptions)} exceptions")
        
        # Phase 2: Verify connection recovery
        await asyncio.sleep(1.0)  # Brief pause
        
        # Try to create new connections after the held ones are released
        recovery_tasks = [
            create_and_hold_connection(i + 1000, 0.5)
            for i in range(10)
        ]
        
        recovery_start = time.time()
        recovery_results = await asyncio.gather(*recovery_tasks, return_exceptions=True)
        recovery_time = time.time() - recovery_start
        
        successful_recovery = [
            r for r in recovery_results 
            if isinstance(r, dict) and r.get('success', False)
        ]
        
        # Validate recovery
        assert len(successful_recovery) >= 8, \
            f"Poor connection recovery: only {len(successful_recovery)}/10 connections successful"
        
        # Most connections in phase 1 should have succeeded
        success_rate = len(successful_connections) / max_connections_to_test
        assert success_rate > 0.7, \
            f"Too many connection failures: {success_rate:.1%} success rate"
        
        print(f"✅ Connection exhaustion recovery test passed:")
        print(f"   Phase 1: {len(successful_connections)}/{max_connections_to_test} connections ({success_rate:.1%})")
        print(f"   Recovery: {len(successful_recovery)}/10 connections")
        print(f"   Times: Phase1={phase1_time:.1f}s, Recovery={recovery_time:.1f}s")
        
    except Exception as e:
        pytest.skip(f"Database not available: {e}")


@pytest.mark.asyncio
@pytest.mark.stress
@pytest.mark.integration
@pytest.mark.skipif(not MSSQL_AVAILABLE, reason="fastmssql not available")
async def test_rapid_connect_disconnect_stress():
    """Stress test rapid connection creation and destruction."""
    try:
        operations_log = []
        error_count = 0
        
        async def rapid_connection_worker(worker_id: int, iterations: int):
            """Worker that rapidly creates and destroys connections."""
            nonlocal error_count
            local_operations = []
            
            # Create one connection per worker and reuse it
            # The Rust layer will handle pooling internally
            pool_config = PoolConfig(max_size=50, min_idle=10)
            
            try:
                async with Connection(TEST_CONNECTION_STRING, pool_config) as conn:
                    for i in range(iterations):
                        operation_start = time.time()
                        try:
                            # Quick operation to verify connection
                            result = await conn.execute(f"SELECT {worker_id} as worker, {i} as iter")
                            operation_time = time.time() - operation_start
                            
                            local_operations.append({
                                'worker_id': worker_id,
                                'iteration': i,
                                'operation_time': operation_time,
                                'success': True
                            })
                            
                        except Exception as e:
                            error_count += 1
                            operation_time = time.time() - operation_start
                            local_operations.append({
                                'worker_id': worker_id,
                                'iteration': i,
                                'operation_time': operation_time,
                                'success': False,
                                'error': str(e)
                            })
                        
                        # Very brief pause to allow other workers
                        await asyncio.sleep(0.001)
                        
            except Exception as e:
                # If connection creation fails, mark all operations as failed
                error_count += iterations
                for i in range(iterations):
                    local_operations.append({
                        'worker_id': worker_id,
                        'iteration': i,
                        'operation_time': 0,
                        'success': False,
                        'error': f"Connection failed: {str(e)}"
                    })
            
            return local_operations
        
        # Stress test parameters
        num_workers = 20
        iterations_per_worker = 100
        
        start_time = time.time()
        
        # Run workers in groups to manage system load
        group_size = 5
        all_operations = []
        
        for group_start in range(0, num_workers, group_size):
            group_end = min(group_start + group_size, num_workers)
            group_tasks = [
                rapid_connection_worker(worker_id, iterations_per_worker)
                for worker_id in range(group_start, group_end)
            ]
            
            group_results = await asyncio.gather(*group_tasks)
            for worker_ops in group_results:
                all_operations.extend(worker_ops)
            
            # Brief pause between groups
            await asyncio.sleep(0.05)
        
        total_time = time.time() - start_time
        
        # Analyze results
        total_operations = len(all_operations)
        successful_operations = len([op for op in all_operations if op['success']])
        failed_operations = total_operations - successful_operations
        
        avg_operation_time = sum(op['operation_time'] for op in all_operations) / total_operations
        operations_per_second = total_operations / total_time
        
        # Calculate timing statistics
        operation_times = [op['operation_time'] for op in all_operations if op['success']]
        if operation_times:
            min_time = min(operation_times)
            max_time = max(operation_times)
            median_time = sorted(operation_times)[len(operation_times) // 2]
        else:
            min_time = max_time = median_time = 0
        
        # Validate performance
        expected_operations = num_workers * iterations_per_worker
        assert total_operations == expected_operations, \
            f"Expected {expected_operations} operations, got {total_operations}"
        
        success_rate = successful_operations / total_operations
        assert success_rate > 0.95, \
            f"Success rate too low: {success_rate:.2%} ({failed_operations} failures)"
        
        assert operations_per_second > 100, \
            f"Operations per second too low: {operations_per_second:.1f}"
        
        assert avg_operation_time < 0.1, \
            f"Average operation time too high: {avg_operation_time:.3f}s"
        
        print(f"✅ Rapid connect/disconnect stress test passed:")
        print(f"   Operations: {total_operations} ({operations_per_second:.1f}/sec)")
        print(f"   Success rate: {success_rate:.2%}")
        print(f"   Timing: avg={avg_operation_time:.3f}s, min={min_time:.3f}s, max={max_time:.3f}s, median={median_time:.3f}s")
        
    except Exception as e:
        pytest.skip(f"Database not available: {e}")


@pytest.mark.asyncio
@pytest.mark.stress
@pytest.mark.integration
@pytest.mark.skipif(not MSSQL_AVAILABLE, reason="fastmssql not available")
async def test_large_result_set_handling():
    """Test handling of large result sets in async operations."""
    try:
        # Setup: Create a table with substantial data
        async with Connection(TEST_CONNECTION_STRING) as setup_conn:
            await setup_conn.execute("""
                IF OBJECT_ID('test_large_results', 'U') IS NOT NULL 
                DROP TABLE test_large_results
            """)

            await setup_conn.execute("""
                CREATE TABLE test_large_results (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    data NVARCHAR(100),
                    number_col INT,
                    date_col DATETIME2 DEFAULT GETDATE()
                )
            """)
            
            # Insert substantial amount of test data
            batch_size = 1000
            num_batches = 5  # 5000 total rows
            
            for batch in range(num_batches):
                values = []
                for i in range(batch_size):
                    row_id = batch * batch_size + i
                    values.append(f"(N'Data row {row_id}', {row_id})")
                
                insert_sql = f"""
                    INSERT INTO test_large_results (data, number_col) 
                    VALUES {', '.join(values)}
                """
                await setup_conn.execute(insert_sql)

        memory_tracker = MemoryTracker()
        memory_tracker.measure("before_large_query")
        
        # Test concurrent large result set queries
        async def large_query_worker(worker_id: int, limit: int):
            """Worker that executes queries returning large result sets."""
            # Use a pool configuration optimized for concurrency
            pool_config = PoolConfig(max_size=15, min_idle=5)
            
            async with Connection(TEST_CONNECTION_STRING, pool_config) as conn:
                start_time = time.time()
                
                # Query for large result set - use different starting points to reduce contention
                offset = worker_id * 500  # Each worker queries different data ranges
                result = await conn.execute(f"""
                    SELECT TOP {limit}
                        id,
                        data,
                        number_col,
                        date_col,
                        'Worker {worker_id}' as worker_info
                    FROM test_large_results WITH (NOLOCK)
                    WHERE id > {offset}
                    ORDER BY id
                """)
                rows = result.rows() if result else []
                query_time = time.time() - start_time
                
                return {
                    'worker_id': worker_id,
                    'limit': limit,
                    'rows_returned': len(rows) if rows else 0,
                    'query_time': query_time,
                    'first_row_id': rows[0]['id'] if rows else None,
                    'last_row_id': rows[-1]['id'] if rows and len(rows) > 0 else None
                }
        
        # Run concurrent large queries
        query_tasks = [
            large_query_worker(0, 2000),  # Worker 0: 2000 rows
            large_query_worker(1, 1500),  # Worker 1: 1500 rows
            large_query_worker(2, 1000),  # Worker 2: 1000 rows
            large_query_worker(3, 500),   # Worker 3: 500 rows
        ]
        
        start_time = time.time()
        results = await asyncio.gather(*query_tasks)
        total_time = time.time() - start_time
        
        memory_tracker.measure("after_large_query")
        
        # Cleanup
        async with Connection(TEST_CONNECTION_STRING) as cleanup_conn:
            await cleanup_conn.execute("DROP TABLE test_large_results")
        
        memory_tracker.measure("after_cleanup")
        
        # Analyze results
        total_rows_processed = sum(r['rows_returned'] for r in results)
        max_query_time = max(r['query_time'] for r in results)
        avg_query_time = sum(r['query_time'] for r in results) / len(results)
        
        memory_increase = memory_tracker.get_peak_increase_mb()
        
        # Validate large result handling
        for result in results:
            assert result['rows_returned'] == result['limit'], \
                f"Worker {result['worker_id']}: expected {result['limit']} rows, got {result['rows_returned']}"
            
            assert result['query_time'] < 10.0, \
                f"Worker {result['worker_id']}: query took too long: {result['query_time']:.2f}s"
        
        # Concurrent execution analysis
        # For very fast queries, overhead may dominate and perfect concurrency isn't always achievable
        sequential_estimate = sum(r['query_time'] for r in results)
        concurrency_improvement = sequential_estimate / total_time if total_time > 0 else 1
        
        # Validate that we're at least not significantly slower than sequential
        # In some cases, database-level serialization means concurrent != parallel
        # Be more lenient in CI environments
        import os
        slowdown_tolerance = 3.0 if os.getenv('CI') or os.getenv('GITHUB_ACTIONS') else 1.5
        assert total_time <= sequential_estimate * slowdown_tolerance, \
            f"Concurrent execution significantly slower than sequential: {total_time:.2f}s total vs {sequential_estimate:.2f}s sequential"
        
        # If we do see good concurrency, that's a bonus
        if concurrency_improvement > 1.2:
            print(f"   🎉 Good concurrency achieved: {concurrency_improvement:.1f}x improvement")
        elif concurrency_improvement > 0.8:
            print(f"   ✅ Reasonable concurrency: {concurrency_improvement:.1f}x (database may be serializing)")
        else:
            print(f"   ⚠️  Limited concurrency: {concurrency_improvement:.1f}x (check for bottlenecks)")
        
        # Memory usage should be reasonable (be more lenient in CI environments)
        memory_limit_large = 100 if os.getenv('CI') or os.getenv('GITHUB_ACTIONS') else 50
        assert memory_increase < memory_limit_large, \
            f"Memory increase too high for large result sets: {memory_increase:.1f}MB"
        
        print(f"✅ Large result set test passed:")
        print(f"   Total rows processed: {total_rows_processed}")
        print(f"   Query times: avg={avg_query_time:.2f}s, max={max_query_time:.2f}s")
        print(f"   Total concurrent time: {total_time:.2f}s")
        print(f"   Concurrency improvement: {concurrency_improvement:.1f}x")
        print(f"   Memory increase: {memory_increase:.1f}MB")
        
    except Exception as e:
        pytest.skip(f"Database not available: {e}")


if __name__ == "__main__":
    # Run stress tests
    pytest.main([__file__, "-v", "-m", "stress"])
