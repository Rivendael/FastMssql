#!/usr/bin/env python3
"""
FastMSSQL Load Testing Script

This script performs comprehensive load testing on the fastmssql library to determine:
1. Maximum concurrent connections
2. Requests per second (RPS) under different scenarios
3. Connection pool performance under stress
4. Memory usage patterns
5. Error rates under extreme load

Usage:
    python load_test.py --connection-string "Server=localhost;Database=test;..."
    python load_test.py --help
"""

import asyncio
import argparse
import time
import sys
import os
import statistics
import psutil
import gc
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import threading
import json

# Add the python directory to path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python'))

try:
    from fastmssql import Connection, PoolConfig, Parameter, Parameters
except ImportError as e:
    print(f"Error importing fastmssql: {e}")
    print("Make sure you've built the extension with 'maturin develop'")
    sys.exit(1)


@dataclass
class TestResult:
    """Results from a load test run."""
    test_name: str
    duration: float
    total_requests: int
    successful_requests: int
    failed_requests: int
    rps: float
    avg_response_time: float
    min_response_time: float
    max_response_time: float
    p50_response_time: float
    p95_response_time: float
    p99_response_time: float
    memory_usage_mb: float
    peak_memory_mb: float
    error_rate: float
    errors: List[str] = field(default_factory=list)
    response_times: List[float] = field(default_factory=list)
    timestamps: List[float] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'test_name': self.test_name,
            'duration': self.duration,
            'total_requests': self.total_requests,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'rps': self.rps,
            'avg_response_time': self.avg_response_time,
            'min_response_time': self.min_response_time,
            'max_response_time': self.max_response_time,
            'p50_response_time': self.p50_response_time,
            'p95_response_time': self.p95_response_time,
            'p99_response_time': self.p99_response_time,
            'memory_usage_mb': self.memory_usage_mb,
            'peak_memory_mb': self.peak_memory_mb,
            'error_rate': self.error_rate,
            'error_count': len(self.errors),
            'unique_errors': len(set(self.errors))
        }


class LoadTester:
    """Main load testing class for fastmssql."""
    
    def __init__(self, connection_string: str, verbose: bool = False):
        self.connection_string = connection_string
        self.verbose = verbose
        self.process = psutil.Process(os.getpid())
        self.peak_memory = 0.0
        
    def log(self, message: str):
        """Log message if verbose mode is enabled."""
        if self.verbose:
            print(f"[{time.strftime('%H:%M:%S')}] {message}")
    
    def get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        memory_mb = self.process.memory_info().rss / 1024 / 1024
        self.peak_memory = max(self.peak_memory, memory_mb)
        return memory_mb
    
    async def setup_test_data(self, conn: Connection) -> None:
        """Create test tables and data if needed."""
        self.log("Setting up test data...")
        
        # Create a simple test table for load testing
        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='load_test_table' AND xtype='U')
        CREATE TABLE load_test_table (
            id INT IDENTITY(1,1) PRIMARY KEY,
            test_data NVARCHAR(255),
            created_at DATETIME2 DEFAULT GETDATE(),
            test_number INT,
            test_value DECIMAL(10,2)
        )
        """
        
        try:
            await conn.execute(create_table_sql)
            
            # Insert some test data
            insert_sql = """
            INSERT INTO load_test_table (test_data, test_number, test_value) 
            VALUES (@P1, @P2, @P3)
            """
            
            # Insert test data in batches
            for i in range(100):
                await conn.execute(insert_sql, [f"Test data {i}", i, i * 10.5])
            
            self.log("Test data setup complete")
        except Exception as e:
            self.log(f"Test data setup failed: {e}")
    
    async def cleanup_test_data(self, conn: Connection) -> None:
        """Clean up test data after testing."""
        try:
            await conn.execute("DROP TABLE IF EXISTS load_test_table")
            self.log("Test data cleanup complete")
        except Exception as e:
            self.log(f"Test data cleanup failed: {e}")
    
    async def simple_query_worker(self, worker_id: int, conn: Connection, 
                                query: str, params: Optional[List[Any]] = None) -> Tuple[float, Optional[str]]:
        """Execute a simple query and return response time and any error."""
        start_time = time.time()
        try:
            result = await conn.execute(query, params)
            # Force consumption of results
            if result.has_rows():
                list(result.rows())
            return time.time() - start_time, None
        except Exception as e:
            return time.time() - start_time, str(e)
    
    async def concurrent_load_test(self, test_name: str, num_workers: int, 
                                 duration_seconds: int, pool_config: PoolConfig,
                                 query: str, params: Optional[List[Any]] = None) -> TestResult:
        """Run concurrent load test with specified parameters."""
        self.log(f"Starting {test_name} with {num_workers} workers for {duration_seconds}s")
        
        response_times = []
        errors = []
        timestamps = []
        successful_requests = 0
        failed_requests = 0
        start_memory = self.get_memory_usage()
        
        async with Connection(self.connection_string, pool_config) as conn:
            # Setup test data
            await self.setup_test_data(conn)
            
            start_time = time.time()
            end_time = start_time + duration_seconds
            
            async def worker(worker_id: int):
                """Worker function that executes queries until time limit."""
                nonlocal successful_requests, failed_requests
                
                while time.time() < end_time:
                    response_time, error = await self.simple_query_worker(
                        worker_id, conn, query, params
                    )
                    
                    timestamps.append(time.time())
                    response_times.append(response_time)
                    
                    if error:
                        errors.append(error)
                        failed_requests += 1
                    else:
                        successful_requests += 1
                    
                    # Small delay to prevent overwhelming
                    await asyncio.sleep(0.001)
            
            # Start all workers
            tasks = [asyncio.create_task(worker(i)) for i in range(num_workers)]
            
            # Wait for all workers to complete
            await asyncio.gather(*tasks, return_exceptions=True)
            
            actual_duration = time.time() - start_time
            total_requests = successful_requests + failed_requests
            
            # Calculate statistics
            if response_times:
                response_times.sort()
                avg_response_time = statistics.mean(response_times)
                min_response_time = min(response_times)
                max_response_time = max(response_times)
                p50_response_time = response_times[int(len(response_times) * 0.5)]
                p95_response_time = response_times[int(len(response_times) * 0.95)]
                p99_response_time = response_times[int(len(response_times) * 0.99)]
            else:
                avg_response_time = min_response_time = max_response_time = 0
                p50_response_time = p95_response_time = p99_response_time = 0
            
            rps = total_requests / actual_duration if actual_duration > 0 else 0
            error_rate = failed_requests / total_requests if total_requests > 0 else 0
            end_memory = self.get_memory_usage()
            
            # Cleanup
            await self.cleanup_test_data(conn)
        
        return TestResult(
            test_name=test_name,
            duration=actual_duration,
            total_requests=total_requests,
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            rps=rps,
            avg_response_time=avg_response_time,
            min_response_time=min_response_time,
            max_response_time=max_response_time,
            p50_response_time=p50_response_time,
            p95_response_time=p95_response_time,
            p99_response_time=p99_response_time,
            memory_usage_mb=end_memory,
            peak_memory_mb=self.peak_memory,
            error_rate=error_rate,
            errors=errors[:100],  # Keep only first 100 errors
            response_times=response_times[:1000],  # Keep sample of response times
            timestamps=timestamps[:1000]
        )
    
    async def ramp_up_test(self, max_workers: int, step_size: int, 
                          step_duration: int) -> List[TestResult]:
        """Gradually increase load to find breaking point."""
        self.log(f"Starting ramp-up test: 1 to {max_workers} workers, step size {step_size}")
        
        results = []
        
        for num_workers in range(step_size, max_workers + 1, step_size):
            pool_config = PoolConfig(
                max_size=min(num_workers * 3, 500),  # Much larger pool, scale more aggressively
                min_idle=max(1, num_workers // 4),   # Keep more connections warm
                connection_timeout_secs=10,          # Fail faster under stress
                idle_timeout_secs=60,                # Shorter idle timeout
                max_lifetime_secs=300                # Recycle connections more often
            )
            
            result = await self.concurrent_load_test(
                test_name=f"Ramp_Up_{num_workers}_Workers",
                num_workers=num_workers,
                duration_seconds=step_duration,
                pool_config=pool_config,
                query="SELECT COUNT(*) as count FROM load_test_table WHERE test_number < @P1",
                params=[50]
            )
            
            results.append(result)
            
            self.log(f"Workers: {num_workers}, RPS: {result.rps:.2f}, "
                   f"Errors: {result.error_rate:.2%}, Memory: {result.memory_usage_mb:.1f}MB")
            
            # Stop if error rate gets too high
            if result.error_rate > 0.1:  # 10% error rate
                self.log(f"Stopping ramp-up test due to high error rate: {result.error_rate:.2%}")
                break
            
            # Brief pause between tests
            await asyncio.sleep(2)
            gc.collect()  # Force garbage collection
        
        return results
    
    async def stress_test_suite(self) -> Dict[str, TestResult]:
        """Run comprehensive stress test suite."""
        results = {}
        
        # Test 1: Simple SELECT queries with different pool configurations
        pool_configs = {
            "small_pool": PoolConfig(max_size=5, min_idle=1),
            "medium_pool": PoolConfig(max_size=20, min_idle=5),
            "large_pool": PoolConfig(max_size=50, min_idle=10),
            "high_throughput": PoolConfig.high_throughput()
        }
        
        for config_name, pool_config in pool_configs.items():
            result = await self.concurrent_load_test(
                test_name=f"Simple_SELECT_{config_name}",
                num_workers=20,
                duration_seconds=30,
                pool_config=pool_config,
                query="SELECT @@VERSION",
                params=None
            )
            results[f"simple_select_{config_name}"] = result
        
        # Test 2: Parameterized queries
        result = await self.concurrent_load_test(
            test_name="Parameterized_Query",
            num_workers=25,
            duration_seconds=30,
            pool_config=PoolConfig.high_throughput(),
            query="SELECT * FROM load_test_table WHERE test_number = @P1 AND test_value > @P2",
            params=[42, 100.0]
        )
        results["parameterized_query"] = result
        
        # Test 3: Complex queries with JOINs (self-join on test table)
        result = await self.concurrent_load_test(
            test_name="Complex_Query_JOIN",
            num_workers=15,
            duration_seconds=30,
            pool_config=PoolConfig.high_throughput(),
            query="""
            SELECT t1.id, t1.test_data, t2.test_value 
            FROM load_test_table t1 
            JOIN load_test_table t2 ON t1.test_number = t2.id 
            WHERE t1.test_number < @P1
            """,
            params=[10]
        )
        results["complex_query"] = result
        
        # Test 4: INSERT operations
        result = await self.concurrent_load_test(
            test_name="INSERT_Operations",
            num_workers=10,
            duration_seconds=20,
            pool_config=PoolConfig.high_throughput(),
            query="INSERT INTO load_test_table (test_data, test_number, test_value) VALUES (@P1, @P2, @P3)",
            params=["Load test insert", 999, 123.45]
        )
        results["insert_operations"] = result
        
        # Test 5: Mixed workload
        # This would require alternating between different query types
        result = await self.concurrent_load_test(
            test_name="Mixed_Workload",
            num_workers=20,
            duration_seconds=45,
            pool_config=PoolConfig.high_throughput(),
            query="SELECT COUNT(*) as count FROM load_test_table WHERE test_number % @P1 = 0",
            params=[3]
        )
        results["mixed_workload"] = result
        
        return results
    
    def print_results(self, results: Dict[str, TestResult]):
        """Print formatted test results."""
        print("\n" + "="*80)
        print("FASTMSSQL LOAD TEST RESULTS")
        print("="*80)
        
        for test_name, result in results.items():
            print(f"\n📊 {result.test_name}")
            print("-" * 50)
            print(f"Duration:           {result.duration:.2f} seconds")
            print(f"Total Requests:     {result.total_requests:,}")
            print(f"Successful:         {result.successful_requests:,}")
            print(f"Failed:             {result.failed_requests:,}")
            print(f"Requests/Second:    {result.rps:.2f}")
            print(f"Error Rate:         {result.error_rate:.2%}")
            print(f"Memory Usage:       {result.memory_usage_mb:.1f} MB")
            print(f"Peak Memory:        {result.peak_memory_mb:.1f} MB")
            print()
            print("Response Times:")
            print(f"  Average:          {result.avg_response_time*1000:.2f} ms")
            print(f"  Min:              {result.min_response_time*1000:.2f} ms")
            print(f"  Max:              {result.max_response_time*1000:.2f} ms")
            print(f"  50th percentile:  {result.p50_response_time*1000:.2f} ms")
            print(f"  95th percentile:  {result.p95_response_time*1000:.2f} ms")
            print(f"  99th percentile:  {result.p99_response_time*1000:.2f} ms")
            
            if result.errors:
                print(f"\nFirst 5 Errors:")
                for i, error in enumerate(result.errors[:5]):
                    print(f"  {i+1}. {error}")
    
    def save_results(self, results: Dict[str, TestResult], filename: str):
        """Save results to JSON file."""
        data = {
            'timestamp': time.time(),
            'test_date': time.strftime('%Y-%m-%d %H:%M:%S'),
            'system_info': {
                'python_version': sys.version,
                'platform': sys.platform,
                'cpu_count': os.cpu_count(),
                'memory_gb': psutil.virtual_memory().total / (1024**3)
            },
            'results': {name: result.to_dict() for name, result in results.items()}
        }
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"\n💾 Results saved to: {filename}")


async def main():
    """Main function to run load tests."""
    parser = argparse.ArgumentParser(description="FastMSSQL Load Testing Tool")
    parser.add_argument(
        "--connection-string", 
        required=True,
        help="SQL Server connection string"
    )
    parser.add_argument(
        "--test-type",
        choices=["full", "ramp-up", "stress"],
        default="full",
        help="Type of test to run (default: full)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=50,
        help="Maximum number of concurrent workers for ramp-up test (default: 50)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Duration for each test in seconds (default: 30)"
    )
    parser.add_argument(
        "--output",
        default="load_test_results.json",
        help="Output file for results (default: load_test_results.json)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    print("🚀 FastMSSQL Load Testing Tool")
    print("="*50)
    print(f"Connection: {args.connection_string[:50]}...")
    print(f"Test Type: {args.test_type}")
    print(f"Max Workers: {args.max_workers}")
    print(f"Duration: {args.duration}s")
    print("="*50)
    
    tester = LoadTester(args.connection_string, args.verbose)
    
    try:
        if args.test_type == "ramp-up":
            results_list = await tester.ramp_up_test(
                max_workers=args.max_workers,
                step_size=5,
                step_duration=args.duration
            )
            # Convert list to dict for consistent handling
            results = {f"ramp_up_{i}": result for i, result in enumerate(results_list)}
        
        elif args.test_type == "stress":
            results = await tester.stress_test_suite()
        
        else:  # full
            # Run both ramp-up and stress tests
            print("\n🔄 Running ramp-up test...")
            ramp_results = await tester.ramp_up_test(
                max_workers=args.max_workers,
                step_size=10,
                step_duration=args.duration // 2
            )
            
            print("\n🔥 Running stress test suite...")
            stress_results = await tester.stress_test_suite()
            
            # Combine results
            results = {}
            results.update({f"ramp_up_{i}": result for i, result in enumerate(ramp_results)})
            results.update(stress_results)
        
        # Print and save results
        tester.print_results(results)
        tester.save_results(results, args.output)
        
        # Summary
        print("\n📈 SUMMARY")
        print("-" * 30)
        best_rps = max(result.rps for result in results.values())
        avg_error_rate = statistics.mean(result.error_rate for result in results.values())
        peak_memory = max(result.peak_memory_mb for result in results.values())
        
        print(f"Best RPS achieved:     {best_rps:.2f}")
        print(f"Average error rate:    {avg_error_rate:.2%}")
        print(f"Peak memory usage:     {peak_memory:.1f} MB")
        
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
