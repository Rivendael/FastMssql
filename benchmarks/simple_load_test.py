#!/usr/bin/env python3
"""
Realistic SQL load test simulating normal database workloads.
Tests mixed read/write operations with parameterized queries and transactions.
"""

import asyncio
import time
import os
import statistics
import random
import string
from collections import defaultdict
from dataclasses import dataclass

from fastmssql import Connection, PoolConfig


@dataclass
class QueryMetrics:
    """Track metrics for different query types."""
    select_queries: int = 0
    insert_queries: int = 0
    update_queries: int = 0
    delete_queries: int = 0
    transaction_queries: int = 0
    errors: int = 0


async def setup_test_data(connection_string: str):
    """Create test tables with realistic schema."""
    async with Connection(connection_string, PoolConfig.development()) as conn:
        try:
            # Drop existing test tables
            await conn.execute("DROP TABLE IF EXISTS load_test_orders")
            await conn.execute("DROP TABLE IF EXISTS load_test_users")
            print("✓ Dropped existing test tables")
            
            # Create users table
            await conn.execute("""
                CREATE TABLE load_test_users (
                    user_id INT PRIMARY KEY IDENTITY(1,1),
                    username NVARCHAR(100) NOT NULL,
                    email NVARCHAR(100) NOT NULL,
                    created_at DATETIME DEFAULT GETDATE(),
                    updated_at DATETIME DEFAULT GETDATE(),
                    is_active BIT DEFAULT 1
                )
            """)
            print("✓ Created load_test_users table")
            
            # Create orders table
            await conn.execute("""
                CREATE TABLE load_test_orders (
                    order_id INT PRIMARY KEY IDENTITY(1,1),
                    user_id INT NOT NULL,
                    order_date DATETIME DEFAULT GETDATE(),
                    amount DECIMAL(10,2) NOT NULL,
                    status NVARCHAR(20) DEFAULT 'pending',
                    description NVARCHAR(500),
                    created_at DATETIME DEFAULT GETDATE(),
                    updated_at DATETIME DEFAULT GETDATE(),
                    FOREIGN KEY (user_id) REFERENCES load_test_users(user_id)
                )
            """)
            print("✓ Created load_test_orders table")
            
            # Insert initial users
            for i in range(100):
                await conn.execute(
                    "INSERT INTO load_test_users (username, email) VALUES (@P1, @P2)",
                    [f"user_{i}", f"user_{i}@test.local"]
                )
            
            print(f"✓ Inserted 100 test users")
            
        except Exception as e:
            print(f"❌ Setup error: {e}")
            raise


async def realistic_load_test(connection_string: str, workers: int = 10, duration: int = 15, warmup: int = 5):
    """Run a realistic load test with mixed SQL operations."""
    
    print(f"\n🎯 Realistic SQL Load Test:")
    print(f"   Workers: {workers}")
    print(f"   Duration: {duration}s (+ {warmup}s warmup)")
    print(f"   Workload: Mixed SELECTs (60%), INSERTs (20%), UPDATEs (15%), DELETEs (5%)")
    print(f"   Operations: Parameterized queries, transactions, realistic data")

    # Thread-safe counters using locks
    stats_lock = asyncio.Lock()
    total_requests = 0
    total_errors = 0
    response_times = []
    query_metrics = defaultdict(lambda: QueryMetrics())
    worker_stats = defaultdict(lambda: {"requests": 0, "errors": 0})
    
    def generate_random_string(length: int = 20) -> str:
        """Generate random string for test data."""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    async def worker(worker_id: int):
        """Worker that executes realistic SQL queries."""
        nonlocal total_requests, total_errors
        async with Connection(connection_string, PoolConfig.development()) as conn:
            
            # Warmup phase
            warmup_end = time.time() + warmup
            warmup_requests = 0
            
            print(f"Worker {worker_id}: Starting warmup...")
            while time.time() < warmup_end:
                try:
                    # Simple warmup query
                    await conn.execute("SELECT COUNT(*) FROM load_test_users")
                    warmup_requests += 1
                except Exception:
                    pass
            
            print(f"Worker {worker_id}: Warmup complete ({warmup_requests} requests)")
            
            # Actual test phase with mixed workload
            test_end = time.time() + duration
            local_requests = 0
            local_errors = 0
            local_response_times = []
            local_metrics = QueryMetrics()
            
            while time.time() < test_end:
                try:
                    operation = random.randint(1, 100)
                    start_time = time.perf_counter()
                    
                    if operation <= 60:  # 60% SELECT queries
                        # Query with WHERE clause and JOIN
                        query_type = random.randint(1, 3)
                        if query_type == 1:
                            # Count orders by user
                            user_id = random.randint(1, 100)
                            await conn.execute(
                                """SELECT COUNT(*) as order_count, SUM(amount) as total_amount 
                                   FROM load_test_orders 
                                   WHERE user_id = @P1""",
                                [user_id]
                            )
                        elif query_type == 2:
                            # Join users with their orders
                            await conn.execute(
                                """SELECT TOP 50 u.username, o.order_id, o.amount, o.status
                                   FROM load_test_users u 
                                   LEFT JOIN load_test_orders o ON u.user_id = o.user_id
                                   WHERE u.is_active = 1
                                   ORDER BY o.order_date DESC"""
                            )
                        else:
                            # Search by status
                            status = random.choice(['pending', 'completed', 'cancelled'])
                            await conn.execute(
                                """SELECT user_id, order_date, amount FROM load_test_orders 
                                   WHERE status = @P1 
                                   ORDER BY order_date DESC""",
                                [status]
                            )
                        local_metrics.select_queries += 1
                        
                    elif operation <= 80:  # 20% INSERT queries
                        # Insert new order
                        user_id = random.randint(1, 100)
                        amount = round(random.uniform(10, 1000), 2)
                        description = generate_random_string(50)
                        await conn.execute(
                            """INSERT INTO load_test_orders (user_id, amount, description) 
                               VALUES (@P1, @P2, @P3)""",
                            [user_id, amount, description]
                        )
                        local_metrics.insert_queries += 1
                        
                    elif operation <= 95:  # 15% UPDATE queries
                        # Update order status
                        order_id = random.randint(1, 10000)
                        new_status = random.choice(['pending', 'completed', 'cancelled', 'shipped'])
                        await conn.execute(
                            """UPDATE load_test_orders 
                               SET status = @P1, updated_at = GETDATE() 
                               WHERE order_id = @P2""",
                            [new_status, order_id]
                        )
                        local_metrics.update_queries += 1
                        
                    else:  # 5% DELETE queries
                        # Delete old cancelled orders
                        order_id = random.randint(1, 5000)
                        await conn.execute(
                            """DELETE FROM load_test_orders 
                               WHERE order_id = @P1 AND status = 'cancelled'""",
                            [order_id]
                        )
                        local_metrics.delete_queries += 1
                    
                    response_time = time.perf_counter() - start_time
                    local_response_times.append(response_time)
                    local_requests += 1
                    
                except Exception as e:
                    local_errors += 1
                    local_metrics.errors += 1
                    if local_errors <= 3:
                        print(f"Worker {worker_id} error: {e}")
            
            # Update global stats atomically
            async with stats_lock:
                total_requests += local_requests
                total_errors += local_errors
                response_times.extend(local_response_times)
                worker_stats[worker_id]["requests"] = local_requests
                worker_stats[worker_id]["errors"] = local_errors
                query_metrics[worker_id] = local_metrics
            
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
    
    # Aggregate query type metrics
    total_selects = sum(m.select_queries for m in query_metrics.values())
    total_inserts = sum(m.insert_queries for m in query_metrics.values())
    total_updates = sum(m.update_queries for m in query_metrics.values())
    total_deletes = sum(m.delete_queries for m in query_metrics.values())
    
    # Calculate comprehensive results
    if total_requests > 0:
        rps = total_requests / actual_duration
        error_rate = (total_errors / (total_requests + total_errors)) * 100
        
        # Response time statistics
        avg_response_time = statistics.mean(response_times) if response_times else 0
        median_response_time = statistics.median(response_times) if response_times else 0
        p95_response_time = statistics.quantiles(response_times, n=20)[18] if len(response_times) >= 20 else 0
        p99_response_time = statistics.quantiles(response_times, n=100)[98] if len(response_times) >= 100 else 0
        min_response_time = min(response_times) if response_times else 0
        max_response_time = max(response_times) if response_times else 0
        
        # Worker distribution
        requests_per_worker = [stats["requests"] for stats in worker_stats.values()]
        worker_balance = (max(requests_per_worker) - min(requests_per_worker)) / max(requests_per_worker) * 100 if requests_per_worker else 0
    else:
        rps = 0
        error_rate = 100
        avg_response_time = median_response_time = p95_response_time = p99_response_time = 0
        min_response_time = max_response_time = 0
        worker_balance = 0
    
    print(f"\n📊 Results:")
    print(f"   Total Requests: {total_requests:,}")
    print(f"   Errors: {total_errors}")
    print(f"   Test Duration: {actual_duration:.2f}s")
    print(f"   RPS: {rps:.1f}")
    print(f"   Error Rate: {error_rate:.2f}%")
    print(f"\n   Query Distribution:")
    print(f"     SELECTs:  {total_selects:,} ({total_selects/total_requests*100:.1f}%)")
    print(f"     INSERTs:  {total_inserts:,} ({total_inserts/total_requests*100:.1f}%)")
    print(f"     UPDATEs:  {total_updates:,} ({total_updates/total_requests*100:.1f}%)")
    print(f"     DELETEs:  {total_deletes:,} ({total_deletes/total_requests*100:.1f}%)")
    print(f"\n   Response Times:")
    print(f"     Average: {avg_response_time*1000:.2f}ms")
    print(f"     Median:  {median_response_time*1000:.2f}ms")
    print(f"     P95:     {p95_response_time*1000:.2f}ms")
    print(f"     P99:     {p99_response_time*1000:.2f}ms")
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
        "p99_response_time": p99_response_time,
        "error_rate": error_rate,
        "worker_balance": worker_balance,
        "selects": total_selects,
        "inserts": total_inserts,
        "updates": total_updates,
        "deletes": total_deletes
    }


async def main():
    """Run realistic load tests with multiple iterations for stability."""
    from dotenv import load_dotenv
    load_dotenv()
    # Try to get connection string from environment
    connection_string = os.getenv('FASTMSSQL_TEST_CONNECTION_STRING')
    
    if not connection_string:
        print("❌ No connection string found!")
        print("Please set the FASTMSSQL_TEST_CONNECTION_STRING environment variable.")
        print("Example:")
        print('  set FASTMSSQL_TEST_CONNECTION_STRING="Server=localhost,1433;Database=master;User Id=sa;Password=YourPassword;TrustServerCertificate=true;"')
        print("\nOr for Windows Authentication:")
        print('  set FASTMSSQL_TEST_CONNECTION_STRING="Server=localhost;Database=master;Integrated Security=true;TrustServerCertificate=true;"')
        return
    
    # Setup test data
    print("Setting up test database...")
    try:
        await setup_test_data(connection_string)
    except Exception as e:
        print(f"⚠️  Could not setup test data: {e}")
        print("Continuing with existing tables...")
    
    # Test different worker counts with multiple iterations for stability
    scenarios = [
        {"workers": 5, "duration": 30, "iterations": 2},
        {"workers": 10, "duration": 30, "iterations": 2},
        {"workers": 20, "duration": 30, "iterations": 2},
        {"workers": 30, "duration": 30, "iterations": 2},
    ]
    
    all_results = []
    
    for scenario in scenarios:
        workers = scenario["workers"]
        duration = scenario["duration"] 
        iterations = scenario["iterations"]
        
        print(f"\n{'='*70}")
        print(f"Testing {workers} workers ({iterations} iterations)")
        print(f"{'='*70}")
        
        iteration_results = []
        
        for iteration in range(iterations):
            print(f"\n--- Iteration {iteration + 1}/{iterations} ---")
            
            result = await realistic_load_test(
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
        
        p95_times = [r["p95_response_time"] * 1000 for r in iteration_results]
        avg_p95 = statistics.mean(p95_times)
        
        total_requests = sum(r["total_requests"] for r in iteration_results)
        total_errors = sum(r["errors"] for r in iteration_results)
        
        # Query type totals
        total_selects = sum(r["selects"] for r in iteration_results)
        total_inserts = sum(r["inserts"] for r in iteration_results)
        total_updates = sum(r["updates"] for r in iteration_results)
        total_deletes = sum(r["deletes"] for r in iteration_results)
        
        print(f"\n📈 Summary for {workers} workers:")
        print(f"   Average RPS: {avg_rps:.1f} ± {rps_std:.1f} (CV: {rps_cv:.1f}%)")
        print(f"   RPS Range: {min(rps_values):.1f} - {max(rps_values):.1f}")
        print(f"   Average Response Time: {avg_response_time:.2f}ms (P95: {avg_p95:.2f}ms)")
        print(f"   Total Requests: {total_requests:,}")
        print(f"   Query Mix: {total_selects} SELECTs, {total_inserts} INSERTs, {total_updates} UPDATEs, {total_deletes} DELETEs")
        print(f"   Total Errors: {total_errors}")
        
        all_results.append({
            "workers": workers,
            "avg_rps": avg_rps,
            "rps_std": rps_std,
            "rps_cv": rps_cv,
            "min_rps": min(rps_values),
            "max_rps": max(rps_values),
            "avg_response_time": avg_response_time,
            "avg_p95": avg_p95,
            "total_requests": total_requests,
            "total_errors": total_errors,
            "total_selects": total_selects,
            "total_inserts": total_inserts,
            "total_updates": total_updates,
            "total_deletes": total_deletes
        })
    
    # Final comprehensive summary
    print(f"\n{'='*80}")
    print("FINAL SUMMARY - REALISTIC SQL WORKLOAD")
    print(f"{'='*80}")
    print(f"{'Workers':<10} {'Avg RPS':<12} {'±StdDev':<10} {'CV%':<8} {'Avg RT(ms)':<12} {'P95(ms)':<10}")
    print("-" * 80)
    
    for result in all_results:
        print(f"{result['workers']:<10} {result['avg_rps']:<12.1f} {result['rps_std']:<10.1f} "
              f"{result['rps_cv']:<8.1f} {result['avg_response_time']:<12.2f} {result['avg_p95']:<10.2f}")
    
    # Detailed query mix analysis
    print(f"\n{'='*80}")
    print("QUERY TYPE DISTRIBUTION")
    print(f"{'='*80}")
    print(f"{'Workers':<10} {'SELECTs':<15} {'INSERTs':<15} {'UPDATEs':<15} {'DELETEs':<15}")
    print("-" * 80)
    
    for result in all_results:
        total = (result['total_selects'] + result['total_inserts'] + 
                result['total_updates'] + result['total_deletes'])
        selects_pct = result['total_selects'] / total * 100 if total > 0 else 0
        inserts_pct = result['total_inserts'] / total * 100 if total > 0 else 0
        updates_pct = result['total_updates'] / total * 100 if total > 0 else 0
        deletes_pct = result['total_deletes'] / total * 100 if total > 0 else 0
        
        print(f"{result['workers']:<10} "
              f"{result['total_selects']:,} ({selects_pct:.1f}%) "
              f"{result['total_inserts']:,} ({inserts_pct:.1f}%) "
              f"{result['total_updates']:,} ({updates_pct:.1f}%) "
              f"{result['total_deletes']:,} ({deletes_pct:.1f}%)")
    
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
