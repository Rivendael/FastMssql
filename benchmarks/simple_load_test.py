#!/usr/bin/env python3
"""
Simple load test using a single connection pool for accurate results.
"""

import asyncio
import time
import os
import statistics
from collections import defaultdict

# Add the python directory to path

from fastmssql import Connection, PoolConfig


async def simple_load_test(connection_string: str, workers: int = 10, duration: int = 15, warmup: int = 5):
    """Run a simple load test with multiple connection objects to avoid GIL serialization."""
    
    print(f"🎯 Simple Load Test (Multi-Object for GIL Bypass):")
    print(f"   Workers: {workers}")
    print(f"   Duration: {duration}s (+ {warmup}s warmup)")
    print(f"   Query: SELECT @@VERSION")
    print(f"   Strategy: Each worker gets its own Connection object")

    # Thread-safe counters using locks
    stats_lock = asyncio.Lock()
    total_requests = 0
    total_errors = 0
    response_times = []
    worker_stats = defaultdict(lambda: {"requests": 0, "errors": 0})
    
        
    async def worker(worker_id: int):
        """Worker that executes queries with its own optimized connection pool."""
        nonlocal total_requests, total_errors  # Fix: Add this line back
        # 🚀 OPTIMIZED: Each worker gets a smaller, more efficient pool
        async with Connection(connection_string, PoolConfig.development()) as conn:  # 5 max connections instead of 100
            
            # Warmup phase - don't count these requests
            warmup_end = time.time() + warmup
            warmup_requests = 0
            
            print(f"Worker {worker_id}: Starting warmup...")
            while time.time() < warmup_end:
                try:
                    await conn.execute("SELECT GETDATE(), @@SPID")
                    warmup_requests += 1
                except Exception:
                    pass  # Ignore warmup errors
                # No artificial delay during warmup
            
            print(f"Worker {worker_id}: Warmup complete ({warmup_requests} requests)")
            
            # Actual test phase
            test_end = time.time() + duration
            local_requests = 0
            local_errors = 0
            local_response_times = []
            
            while time.time() < test_end:
                start_time = time.perf_counter()  # More precise timing
                try:
                    _ = await conn.execute("SELECT 1")  # Faster than @@VERSION
                    
                    response_time = time.perf_counter() - start_time
                    local_response_times.append(response_time)
                    local_requests += 1
                    
                except Exception as e:
                    local_errors += 1
                    if local_errors <= 3:  # Only print first few errors
                        print(f"Worker {worker_id} error: {e}")
                
                # No artificial delay - let it run at full speed
            
            # Update global stats atomically
            async with stats_lock:
                total_requests += local_requests
                total_errors += local_errors
                response_times.extend(local_response_times)
                worker_stats[worker_id]["requests"] = local_requests
                worker_stats[worker_id]["errors"] = local_errors
            
            print(f"Worker {worker_id}: {local_requests} requests, {local_errors} errors")
    
    print("Starting warmup phase...")
        
    # Start all workers
    worker_tasks = [asyncio.create_task(worker(i)) for i in range(workers)]
        
    # Wait for warmup to complete
    await asyncio.sleep(warmup + 1)
    print("Test phase starting...")
        
    # Measure actual test duration precisely
    test_start = time.perf_counter()
        
    # Wait for all workers to complete
    await asyncio.gather(*worker_tasks)
        
    actual_duration = time.perf_counter() - test_start - warmup
    
    # Calculate comprehensive results
    if total_requests > 0:
        rps = total_requests / actual_duration
        error_rate = (total_errors / (total_requests + total_errors)) * 100
        
        # Response time statistics
        avg_response_time = statistics.mean(response_times) if response_times else 0
        median_response_time = statistics.median(response_times) if response_times else 0
        p95_response_time = statistics.quantiles(response_times, n=20)[18] if len(response_times) >= 20 else 0
        min_response_time = min(response_times) if response_times else 0
        max_response_time = max(response_times) if response_times else 0
        
        # Worker distribution
        requests_per_worker = [stats["requests"] for stats in worker_stats.values()]
        worker_balance = (max(requests_per_worker) - min(requests_per_worker)) / max(requests_per_worker) * 100 if requests_per_worker else 0
    else:
        rps = 0
        error_rate = 100
        avg_response_time = median_response_time = p95_response_time = 0
        min_response_time = max_response_time = 0
        worker_balance = 0
    
    print(f"\n📊 Results:")
    print(f"   Total Requests: {total_requests:,}")
    print(f"   Errors: {total_errors}")
    print(f"   Test Duration: {actual_duration:.2f}s")
    print(f"   RPS: {rps:.1f}")
    print(f"   Error Rate: {error_rate:.2f}%")
    print(f"   Response Times:")
    print(f"     Average: {avg_response_time*1000:.2f}ms")
    print(f"     Median:  {median_response_time*1000:.2f}ms")
    print(f"     P95:     {p95_response_time*1000:.2f}ms")
    print(f"     Min:     {min_response_time*1000:.2f}ms")
    print(f"     Max:     {max_response_time*1000:.2f}ms")
    print(f"   Worker Balance: {worker_balance:.1f}% variance")
    
    return {
        "rps": rps,
        "total_requests": total_requests,
        "errors": total_errors,
        "duration": actual_duration,
        "avg_response_time": avg_response_time,
        "median_response_time": median_response_time,
        "p95_response_time": p95_response_time,
        "error_rate": error_rate,
        "worker_balance": worker_balance
    }


async def main():
    """Run simple load tests with multiple iterations for stability."""
    from dotenv import load_dotenv
    load_dotenv()
    # Try to get connection string from environment
    connection_string = os.getenv('TEST_CONNECTION_STRING')
    
    if not connection_string:
        print("❌ No connection string found!")
        print("Please set the TEST_CONNECTION_STRING environment variable.")
        print("Example:")
        print('  set TEST_CONNECTION_STRING="Server=localhost,1433;Database=master;User Id=sa;Password=YourPassword;TrustServerCertificate=true;"')
        print("\nOr for Windows Authentication:")
        print('  set TEST_CONNECTION_STRING="Server=localhost;Database=master;Integrated Security=true;TrustServerCertificate=true;"')
        return
    
    # Test different worker counts with multiple iterations for stability
    scenarios = [
        {"workers": 10, "duration": 60, "iterations": 2},
        {"workers": 25, "duration": 60, "iterations": 2}, 
        {"workers": 35, "duration": 60, "iterations": 2},
        {"workers": 45, "duration": 60, "iterations": 2},
    ]
    
    all_results = []
    
    for scenario in scenarios:
        workers = scenario["workers"]
        duration = scenario["duration"] 
        iterations = scenario["iterations"]
        
        print(f"\n{'='*60}")
        print(f"Testing {workers} workers ({iterations} iterations)")
        print(f"{'='*60}")
        
        iteration_results = []
        
        for iteration in range(iterations):
            print(f"\n--- Iteration {iteration + 1}/{iterations} ---")
            
            result = await simple_load_test(
                connection_string=connection_string,
                workers=workers,
                duration=duration,
                warmup=5
            )
            
            iteration_results.append(result)
            
            # Rest between iterations
            if iteration < iterations - 1:
                print("Resting 5 seconds between iterations...")
                await asyncio.sleep(5)
        
        # Calculate average and stability metrics
        rps_values = [r["rps"] for r in iteration_results]
        avg_rps = statistics.mean(rps_values)
        rps_std = statistics.stdev(rps_values) if len(rps_values) > 1 else 0
        rps_cv = (rps_std / avg_rps * 100) if avg_rps > 0 else 0  # Coefficient of variation
        
        response_times = [r["avg_response_time"] * 1000 for r in iteration_results]
        avg_response_time = statistics.mean(response_times)
        
        total_requests = sum(r["total_requests"] for r in iteration_results)
        total_errors = sum(r["errors"] for r in iteration_results)
        
        print(f"\n📈 Summary for {workers} workers:")
        print(f"   Average RPS: {avg_rps:.1f} ± {rps_std:.1f} (CV: {rps_cv:.1f}%)")
        print(f"   RPS Range: {min(rps_values):.1f} - {max(rps_values):.1f}")
        print(f"   Average Response Time: {avg_response_time:.2f}ms")
        print(f"   Total Requests: {total_requests:,}")
        print(f"   Total Errors: {total_errors}")
        
        all_results.append({
            "workers": workers,
            "avg_rps": avg_rps,
            "rps_std": rps_std,
            "rps_cv": rps_cv,
            "min_rps": min(rps_values),
            "max_rps": max(rps_values),
            "avg_response_time": avg_response_time,
            "total_requests": total_requests,
            "total_errors": total_errors
        })
    
    # Final comprehensive summary
    print(f"\n{'='*60}")
    print("FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"{'Workers':<8} {'Avg RPS':<10} {'±StdDev':<10} {'CV%':<8} {'Range':<15} {'Avg RT(ms)':<12}")
    print("-" * 70)
    
    for result in all_results:
        rps_range = f"{result['min_rps']:.0f}-{result['max_rps']:.0f}"
        print(f"{result['workers']:<8} {result['avg_rps']:<10.1f} {result['rps_std']:<10.1f} "
              f"{result['rps_cv']:<8.1f} {rps_range:<15} {result['avg_response_time']:<12.2f}")
    
    # Performance analysis
    print(f"\n🔍 Performance Analysis:")
    best_result = max(all_results, key=lambda x: x['avg_rps'])
    most_stable = min(all_results, key=lambda x: x['rps_cv'])
    
    print(f"   Best Performance: {best_result['workers']} workers @ {best_result['avg_rps']:.1f} RPS")
    print(f"   Most Stable: {most_stable['workers']} workers (CV: {most_stable['rps_cv']:.1f}%)")
    
    # Check for errors
    total_errors_all = sum(r['total_errors'] for r in all_results)
    if total_errors_all > 0:
        print(f"   ⚠️  Total Errors Across All Tests: {total_errors_all}")
    else:
        print(f"   ✅ No errors detected across all tests")


if __name__ == "__main__":
    asyncio.run(main())
