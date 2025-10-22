#!/usr/bin/env python3
"""
Async Context Manager and Edge Case Testing for mssql-python-rust

This module tests edge cases and context manager behavior for async operations:
1. Context manager lifecycle edge cases
2. Exception handling within async context managers
3. Timeout and cancellation scenarios
4. Resource cleanup verification
5. Nested and complex async patterns
"""

import asyncio
import time
import pytest
from typing import List, Dict, Any
import os

# Test configuration
TEST_CONNECTION_STRING = os.getenv(
    "FASTMSSQL_TEST_CONNECTION_STRING",
)

try:
    from fastmssql import Connection
    MSSQL_AVAILABLE = True
except ImportError:
    MSSQL_AVAILABLE = False
    mssql = None
    Connection = None


class ConnectionTracker:
    """Helper class to track connection lifecycle events."""
    
    def __init__(self):
        self.events = []
        self.active_connections = set()
        self.lock = asyncio.Lock()
    
    async def log_event(self, event_type: str, connection_id: str, details: Dict[str, Any] = None):
        """Log a connection lifecycle event."""
        async with self.lock:
            event = {
                'event_type': event_type,
                'connection_id': connection_id,
                'timestamp': time.time(),
                'details': details or {}
            }
            self.events.append(event)
            
            if event_type == 'opened':
                self.active_connections.add(connection_id)
            elif event_type in ['closed', 'finally']:
                self.active_connections.discard(connection_id)
    
    def get_events_for_connection(self, connection_id: str) -> List[Dict[str, Any]]:
        """Get all events for a specific connection."""
        return [e for e in self.events if e['connection_id'] == connection_id]
    
    def get_active_connection_count(self) -> int:
        """Get current count of active connections."""
        return len(self.active_connections)


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.skipif(not MSSQL_AVAILABLE, reason="fastmssql not available")
async def test_async_context_manager_exception_handling():
    """Test async context manager behavior when exceptions occur."""
    tracker = ConnectionTracker()
    
    async def test_exception_in_context(test_id: str, should_fail: bool):
        """Test function that may raise an exception within async context."""
        connection_id = f"conn_{test_id}"
        
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                await tracker.log_event('opened', connection_id)
                
                # Execute a successful query first
                await conn.execute("SELECT 1 as test")
                await tracker.log_event('query_success', connection_id)
                
                if should_fail:
                    # This should raise an exception
                    await conn.execute("SELECT * FROM non_existent_table_xyz")
                
                await tracker.log_event('before_exit', connection_id)
                
        except Exception as e:
            await tracker.log_event('exception', connection_id, {'error': str(e)})
            return {'test_id': test_id, 'success': False, 'error': str(e)}
        finally:
            await tracker.log_event('finally', connection_id)
        
        await tracker.log_event('after_context', connection_id)
        return {'test_id': test_id, 'success': True}
    
    # Test scenarios
    test_scenarios = [
        ('success_1', False),
        ('failure_1', True),
        ('success_2', False),
        ('failure_2', True),
        ('success_3', False),
    ]
    
    # Run scenarios concurrently
    tasks = [test_exception_in_context(test_id, should_fail) for test_id, should_fail in test_scenarios]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Analyze results
    successful_tests = [r for r in results if isinstance(r, dict) and r.get('success', False)]
    failed_tests = [r for r in results if isinstance(r, dict) and not r.get('success', False)]
    
    assert len(successful_tests) == 3, f"Expected 3 successful tests, got {len(successful_tests)}"
    assert len(failed_tests) == 2, f"Expected 2 failed tests, got {len(failed_tests)}"
    
    # Verify connection lifecycle for each test
    for test_id, should_fail in test_scenarios:
        connection_id = f"conn_{test_id}"
        events = tracker.get_events_for_connection(connection_id)
        
        # Every connection should have opened and finally events
        event_types = [e['event_type'] for e in events]
        assert 'opened' in event_types, f"Connection {connection_id} missing 'opened' event"
        assert 'finally' in event_types, f"Connection {connection_id} missing 'finally' event"
        
        if should_fail:
            assert 'exception' in event_types, f"Failing connection {connection_id} missing 'exception' event"
        else:
            assert 'after_context' in event_types, f"Successful connection {connection_id} missing 'after_context' event"
    
    # Verify no connections are left hanging
    assert tracker.get_active_connection_count() == 0, "Some connections were not properly closed"
    
    print(f"✅ Async context manager exception handling test passed")
    print(f"   Successful tests: {len(successful_tests)}")
    print(f"   Failed tests (expected): {len(failed_tests)}")
    print(f"   Total events logged: {len(tracker.events)}")


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.skipif(not MSSQL_AVAILABLE, reason="fastmssql not available")
async def test_nested_async_context_managers():
    """Test nested async context managers and their interaction."""
    operation_log = []
    
    async def nested_operation_worker(worker_id: int):
        """Worker that uses nested async context managers."""
        worker_log = []
        
        # Outer context manager
        async with Connection(TEST_CONNECTION_STRING) as outer_conn:
            worker_log.append(f"Worker {worker_id}: Outer connection opened")
            
            # Execute query in outer connection
            result1 = await outer_conn.query(f"SELECT {worker_id} as worker_id, 'outer' as context")
            worker_log.append(f"Worker {worker_id}: Outer query executed")
            
            # Inner context manager (different connection)
            async with Connection(TEST_CONNECTION_STRING) as inner_conn:
                worker_log.append(f"Worker {worker_id}: Inner connection opened")
                
                # Execute query in inner connection
                result2 = await inner_conn.query(f"SELECT {worker_id} as worker_id, 'inner' as context")
                worker_log.append(f"Worker {worker_id}: Inner query executed")
                
                # Verify both connections work independently
                outer_check = await outer_conn.query("SELECT 'outer_check' as source")
                inner_check = await inner_conn.query("SELECT 'inner_check' as source")
                
                worker_log.append(f"Worker {worker_id}: Both connections verified")
            
            worker_log.append(f"Worker {worker_id}: Inner connection closed")
            
            # Execute another query in outer connection after inner is closed
            result3 = await outer_conn.query(f"SELECT {worker_id} as worker_id, 'after_inner' as context")
            worker_log.append(f"Worker {worker_id}: Final outer query executed")
        
        worker_log.append(f"Worker {worker_id}: Outer connection closed")
        
        return {
            'worker_id': worker_id,
            'log': worker_log,
            'results': [result1.rows() if result1.has_rows() else [], 
                       result2.rows() if result2.has_rows() else [], 
                       result3.rows() if result3.has_rows() else []],
            'success': True
        }
    
    # Run multiple workers with nested context managers
    num_workers = 5
    tasks = [nested_operation_worker(i) for i in range(num_workers)]
    
    start_time = time.time()
    results = await asyncio.gather(*tasks)
    total_time = time.time() - start_time
    
    # Analyze results
    successful_workers = [r for r in results if r.get('success', False)]
    assert len(successful_workers) == num_workers, f"Expected {num_workers} successful workers"
    
    # Verify each worker completed all operations
    for result in results:
        worker_id = result['worker_id']
        log = result['log']
        results_data = result['results']
        
        # Check log completeness
        assert len(log) >= 7, f"Worker {worker_id} incomplete log: {len(log)} entries"
        assert 'Outer connection opened' in log[0], f"Worker {worker_id} missing outer open"
        assert 'Outer connection closed' in log[-1], f"Worker {worker_id} missing outer close"
        
        # Check results
        assert len(results_data) == 3, f"Worker {worker_id} missing results"
        assert all(len(r) > 0 for r in results_data), f"Worker {worker_id} has empty results"
    
    print(f"✅ Nested async context managers test passed")
    print(f"   Workers: {len(successful_workers)}")
    print(f"   Total time: {total_time:.2f}s")
    print(f"   Operations per worker: 7 (all completed)")


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.skipif(not MSSQL_AVAILABLE, reason="fastmssql not available")
async def test_async_timeout_and_cancellation():
    """Test timeout behavior and task cancellation with async connections."""
    async def long_running_operation(operation_id: int, duration: int):
        """Operation that runs for a specified duration."""
        async with Connection(TEST_CONNECTION_STRING) as conn:
            # Use SQL Server's WAITFOR DELAY to simulate long operation
            result = await conn.execute(f"""
                WAITFOR DELAY '00:00:{duration:02d}';
                SELECT {operation_id} as operation_id, 'completed' as status;
            """)
            return {'operation_id': operation_id, 'result': result, 'completed': True}
    
    # Test 1: Operations that should complete within timeout
    async def test_successful_timeout():
        """Test operations that complete within reasonable time."""
        timeout_seconds = 5.0
        
        # Operations that should complete in 2 seconds each
        operations = [
            long_running_operation(i, 2) for i in range(3)
            ]
        
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*operations), 
                timeout=timeout_seconds
            )
            return {'success': True, 'results': results, 'completed_count': len(results)}
        except asyncio.TimeoutError:
            return {'success': False, 'error': 'timeout', 'completed_count': 0}
    
    # Test 2: Operations that should timeout
    async def test_timeout_behavior():
        """Test operations that exceed timeout."""
        timeout_seconds = 3.0
        
        # Operations that would take 5 seconds each (should timeout)
        operations = [
            long_running_operation(i + 100, 5) for i in range(2)
        ]
        
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*operations), 
                timeout=timeout_seconds
            )
            return {'success': False, 'error': 'should_have_timed_out', 'results': results}
        except asyncio.TimeoutError:
            return {'success': True, 'error': 'timeout_as_expected'}
    
    # Test 3: Task cancellation
    async def test_task_cancellation():
        """Test manual task cancellation."""
        # Start a long operation
        task = asyncio.create_task(long_running_operation(200, 10))
        
        # Let it run briefly
        await asyncio.sleep(1.0)
        
        # Cancel the task
        task.cancel()
        
        try:
            await task
            return {'success': False, 'error': 'cancellation_failed'}
        except asyncio.CancelledError:
            return {'success': True, 'cancelled': True}
    
    # Run all timeout tests
    start_time = time.time()
    
    timeout_test_results = await asyncio.gather(
        test_successful_timeout(),
        test_timeout_behavior(),
        test_task_cancellation(),
        return_exceptions=True
    )
    
    total_time = time.time() - start_time
    
    # Analyze results
    successful_timeout_test = timeout_test_results[0]
    timeout_behavior_test = timeout_test_results[1]
    cancellation_test = timeout_test_results[2]
    
    # Validate successful timeout test
    assert successful_timeout_test.get('success', False), \
        f"Successful timeout test failed: {successful_timeout_test}"
    assert successful_timeout_test.get('completed_count', 0) == 3, \
        f"Expected 3 completed operations, got {successful_timeout_test.get('completed_count', 0)}"
    
    # Validate timeout behavior test
    assert timeout_behavior_test.get('success', False), \
        f"Timeout behavior test failed: {timeout_behavior_test}"
    assert timeout_behavior_test.get('error') == 'timeout_as_expected', \
        f"Expected timeout, got: {timeout_behavior_test.get('error')}"
    
    # Validate cancellation test
    assert cancellation_test.get('success', False), \
        f"Cancellation test failed: {cancellation_test}"
    assert cancellation_test.get('cancelled', False), \
        f"Task was not properly cancelled: {cancellation_test}"
    
    print(f"✅ Timeout and cancellation test passed")
    print(f"   Successful timeout test: ✓")
    print(f"   Timeout behavior test: ✓")
    print(f"   Cancellation test: ✓")
    print(f"   Total test time: {total_time:.2f}s")


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.skipif(not MSSQL_AVAILABLE, reason="fastmssql not available")
async def test_async_connection_resource_cleanup():
    """Test that async connections properly clean up resources."""
    try:
        # Track connection lifecycle through operations rather than weak references
        connection_operations = []
        
        async def create_tracked_connection(conn_id: int):
            """Create a connection and track its operations."""
            operation_log = []
            
            try:
                async with Connection(TEST_CONNECTION_STRING) as conn:
                    operation_log.append(f"Connection {conn_id}: Opened")
                    
                    # Perform some operations to verify connection works
                    result1 = await conn.execute(f"SELECT {conn_id} as conn_id")
                    operation_log.append(f"Connection {conn_id}: Query 1 executed")
                    
                    result2 = await conn.execute("SELECT @@VERSION")
                    operation_log.append(f"Connection {conn_id}: Query 2 executed")
                    
                    # Test that results are valid
                    assert result1 is not None, f"Connection {conn_id}: Query 1 returned None"
                    assert result2 is not None, f"Connection {conn_id}: Query 2 returned None"
                    
                    operation_log.append(f"Connection {conn_id}: Operations validated")
                    
                # Connection should be automatically closed here
                operation_log.append(f"Connection {conn_id}: Context exited")
                
                return {
                    'conn_id': conn_id, 
                    'operations_completed': 2,
                    'operation_log': operation_log,
                    'success': True
                }
                
            except Exception as e:
                operation_log.append(f"Connection {conn_id}: Error - {str(e)}")
                return {
                    'conn_id': conn_id,
                    'operations_completed': 0,
                    'operation_log': operation_log,
                    'success': False,
                    'error': str(e)
                }
        
        # Test with multiple concurrent connections
        num_connections = 15
        start_time = time.time()
        
        # Create connections in batches to test resource management
        batch_size = 5
        all_results = []
        
        for batch_start in range(0, num_connections, batch_size):
            batch_end = min(batch_start + batch_size, num_connections)
            batch_tasks = [
                create_tracked_connection(i) 
                for i in range(batch_start, batch_end)
            ]
            
            batch_results = await asyncio.gather(*batch_tasks)
            all_results.extend(batch_results)
            
            # Small delay between batches to allow cleanup
            await asyncio.sleep(0.1)
        
        total_time = time.time() - start_time
        
        # Analyze results
        successful_connections = [r for r in all_results if r.get('success', False)]
        failed_connections = [r for r in all_results if not r.get('success', False)]
        
        # Verify all connections were successful
        assert len(successful_connections) == num_connections, \
            f"Expected {num_connections} successful connections, got {len(successful_connections)}"
        
        assert len(failed_connections) == 0, \
            f"Unexpected failed connections: {failed_connections}"
        
        # Verify all operations completed
        total_operations = sum(r['operations_completed'] for r in successful_connections)
        expected_operations = num_connections * 2
        
        assert total_operations == expected_operations, \
            f"Expected {expected_operations} operations, got {total_operations}"
        
        # Verify each connection went through proper lifecycle
        for result in successful_connections:
            log = result['operation_log']
            conn_id = result['conn_id']
            
            # Check that connection lifecycle events are present
            log_text = ' '.join(log)
            assert 'Opened' in log_text, f"Connection {conn_id}: Missing 'Opened' event"
            assert 'Context exited' in log_text, f"Connection {conn_id}: Missing 'Context exited' event"
            assert 'Operations validated' in log_text, f"Connection {conn_id}: Missing validation"
        
        # Test rapid connection creation and cleanup
        rapid_test_start = time.time()
        rapid_tasks = [create_tracked_connection(i + 1000) for i in range(10)]
        rapid_results = await asyncio.gather(*rapid_tasks)
        rapid_test_time = time.time() - rapid_test_start
        
        rapid_successful = [r for r in rapid_results if r.get('success', False)]
        assert len(rapid_successful) == 10, \
            f"Rapid test: Expected 10 successful connections, got {len(rapid_successful)}"
        
        print(f"✅ Resource cleanup test completed")
        print(f"   Batch connections created: {num_connections}")
        print(f"   All connections successful: ✓")
        print(f"   Total operations: {total_operations}")
        print(f"   Batch test time: {total_time:.2f}s")
        print(f"   Rapid connections created: {len(rapid_successful)}")
        print(f"   Rapid test time: {rapid_test_time:.2f}s")
        print(f"   Resource management: ✓")
        
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.skipif(not MSSQL_AVAILABLE, reason="fastmssql not available")
async def test_async_context_manager_with_background_tasks():
    """Test async context managers with background tasks and complex workflows."""
    try:
        background_tasks = []
        
        async def background_query_task(task_id: int, interval: float, iterations: int):
            """Background task that periodically executes queries."""
            results = []
            
            for i in range(iterations):
                try:
                    async with Connection(TEST_CONNECTION_STRING) as conn:
                        result = await conn.query(f"""
                            SELECT 
                                {task_id} as task_id,
                                {i} as iteration,
                                GETDATE() as timestamp
                        """)
                        results.append(result.rows()[0] if result and result.has_rows() else None)
                        
                except Exception as e:
                    results.append({'error': str(e)})
                
                await asyncio.sleep(interval)
            
            return {'task_id': task_id, 'results': results}
        
        async def main_workflow():
            """Main workflow that starts background tasks and performs operations."""
            # Start background tasks
            bg_task1 = asyncio.create_task(background_query_task(1, 0.5, 6))  # Every 0.5s for 3s
            bg_task2 = asyncio.create_task(background_query_task(2, 0.3, 10)) # Every 0.3s for 3s
            background_tasks.extend([bg_task1, bg_task2])
            
            # Perform main operations while background tasks run
            main_results = []
            
            for main_op in range(5):
                async with Connection(TEST_CONNECTION_STRING) as conn:
                    result = await conn.query(f"""
                        SELECT 
                            {main_op} as main_operation,
                            'main_workflow' as source,
                            GETDATE() as timestamp
                    """)
                    main_results.append(result.rows()[0] if result and result.has_rows() else None)
                
                await asyncio.sleep(0.7)  # Different timing from background tasks
            
            # Wait for background tasks to complete
            bg_results = await asyncio.gather(bg_task1, bg_task2)
            
            return {
                'main_results': main_results,
                'background_results': bg_results
            }
        
        # Run the complex workflow
        start_time = time.time()
        workflow_result = await main_workflow()
        total_time = time.time() - start_time
        
        # Analyze results
        main_results = workflow_result['main_results']
        background_results = workflow_result['background_results']
        
        # Validate main workflow
        assert len(main_results) == 5, f"Expected 5 main results, got {len(main_results)}"
        assert all(r is not None for r in main_results), "Some main results are None"
        
        # Validate background tasks
        assert len(background_results) == 2, f"Expected 2 background task results, got {len(background_results)}"
        
        task1_results = background_results[0]['results']
        task2_results = background_results[1]['results']
        
        assert len(task1_results) == 6, f"Background task 1: expected 6 results, got {len(task1_results)}"
        assert len(task2_results) == 10, f"Background task 2: expected 10 results, got {len(task2_results)}"
        
        # Check for errors in background tasks
        task1_errors = [r for r in task1_results if isinstance(r, dict) and 'error' in r]
        task2_errors = [r for r in task2_results if isinstance(r, dict) and 'error' in r]
        
        assert len(task1_errors) == 0, f"Background task 1 had errors: {task1_errors}"
        assert len(task2_errors) == 0, f"Background task 2 had errors: {task2_errors}"
        
        # Verify timing (should be approximately 3.5 seconds for main workflow)
        assert 3.0 < total_time < 5.0, f"Workflow took unexpected time: {total_time:.2f}s"
        
        print(f"✅ Background tasks with context managers test passed")
        print(f"   Main operations: {len(main_results)}")
        print(f"   Background task 1 operations: {len(task1_results)}")
        print(f"   Background task 2 operations: {len(task2_results)}")
        print(f"   Total workflow time: {total_time:.2f}s")
        
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


if __name__ == "__main__":
    # Run edge case tests
    pytest.main([__file__, "-v"])
