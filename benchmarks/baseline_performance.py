#!/usr/bin/env python3
"""
Absolute baseline performance test - SELECT 1 with minimal overhead.
This shows the theoretical maximum RPS fastmssql can achieve.
"""

import asyncio
import time
import os
from fastmssql import Connection, PoolConfig


async def baseline_test(connection_string: str, workers: int = 1, duration: int = 10):
    """Run absolute baseline test with SELECT 1."""
    
    print(f"\n📊 Baseline Test (SELECT 1):")
    print(f"   Workers: {workers}")
    print(f"   Duration: {duration}s")
    print(f"   Query: SELECT 1 (no joins, no complexity)")
    
    total_requests = 0
    total_errors = 0
    
    async def worker(worker_id: int, conn: Connection):
        nonlocal total_requests, total_errors
        local_requests = 0
        local_errors = 0
        
        test_end = time.time() + duration
        while time.time() < test_end:
            try:
                await conn.execute("SELECT 1 as test")
                local_requests += 1
            except Exception as e:
                local_errors += 1
                if local_errors <= 3:
                    print(f"Worker {worker_id} error: {e}")
        
        total_requests += local_requests
        total_errors += local_errors
        print(f"Worker {worker_id}: {local_requests} requests, {local_errors} errors")
    
    print("Starting test...")
    
    # Create a single shared connection for all workers
    async with Connection(connection_string, PoolConfig.performance()) as shared_conn:
        pool_stats = await shared_conn.pool_stats()
        print(f"Pool: {pool_stats}")
        
        # Start all workers
        worker_tasks = [asyncio.create_task(worker(i, shared_conn)) for i in range(workers)]
        
        # Measure precisely
        test_start = time.perf_counter()
        await asyncio.gather(*worker_tasks)
        actual_duration = time.perf_counter() - test_start
        
        # Calculate results
        rps = total_requests / actual_duration if actual_duration > 0 else 0
        
        print(f"\n📈 Results:")
        print(f"   Total Requests: {total_requests:,}")
        print(f"   Errors: {total_errors}")
        print(f"   Duration: {actual_duration:.2f}s")
        print(f"   RPS: {rps:.1f}")
        print(f"   Avg latency: {actual_duration*1000/total_requests:.3f}ms per query")
        
        return {
            "workers": workers,
            "rps": rps,
            "total_requests": total_requests,
            "errors": total_errors,
            "duration": actual_duration
        }


async def main():
    """Run baseline tests with different worker counts."""
    from dotenv import load_dotenv
    load_dotenv()
    connection_string = os.getenv('FASTMSSQL_TEST_CONNECTION_STRING')
    
    if not connection_string:
        print("❌ No connection string found!")
        return
    
    print("=" * 70)
    print("BASELINE PERFORMANCE TEST - SELECT 1")
    print("=" * 70)
    
    scenarios = [
        {"workers": 1, "duration": 10},
        {"workers": 5, "duration": 10},
        {"workers": 10, "duration": 10},
        {"workers": 20, "duration": 10},
    ]
    
    results = []
    for scenario in scenarios:
        result = await baseline_test(
            connection_string=connection_string,
            workers=scenario["workers"],
            duration=scenario["duration"]
        )
        results.append(result)
        await asyncio.sleep(2)  # Rest between tests
    
    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"{'Workers':<10} {'RPS':<15} {'Queries':<15} {'Latency (ms)':<15}")
    print("-" * 70)
    
    for r in results:
        latency = r["duration"] * 1000 / r["total_requests"] if r["total_requests"] > 0 else 0
        print(f"{r['workers']:<10} {r['rps']:<15.1f} {r['total_requests']:<15,} {latency:<15.3f}")
    
    # Analysis
    best = max(results, key=lambda x: x["rps"])
    print(f"\n✅ Best performance: {best['workers']} workers @ {best['rps']:.0f} RPS")
    
    # Check scaling efficiency
    single_worker = results[0]["rps"]
    print(f"\n📊 Scaling Analysis (relative to single worker @ {single_worker:.0f} RPS):")
    for r in results[1:]:
        efficiency = (r["rps"] / single_worker / r["workers"]) * 100
        print(f"   {r['workers']} workers: {r['rps']:.0f} RPS ({efficiency:.1f}% efficiency)")


if __name__ == "__main__":
    asyncio.run(main())
