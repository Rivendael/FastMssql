#!/usr/bin/env python3
"""
Quick load test runner with predefined scenarios for fastmssql testing.

This script provides simple, one-command load testing scenarios.
"""

import asyncio
import argparse
import os
import sys

# Add the python directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python'))

from benchmarks.load_test import LoadTester, PoolConfig


async def quick_test(connection_string: str, scenario: str = "basic"):
    """Run quick load tests with predefined scenarios."""
    
    tester = LoadTester(connection_string, verbose=True)
    
    scenarios = {
        "basic": {
            "workers": 10,
            "duration": 15,
            "query": "SELECT @@VERSION",
            "pool": PoolConfig(max_size=20)
        },
        "medium": {
            "workers": 25,
            "duration": 30,
            "query": "SELECT GETDATE() as current_time, @@SPID as session_id",
            "pool": PoolConfig.high_throughput()
        },
        "heavy": {
            "workers": 50,
            "duration": 60,
            "query": "SELECT COUNT(*) FROM sys.objects WHERE type = 'U'",
            "pool": PoolConfig(max_size=100, min_idle=10)
        },
        "extreme": {
            "workers": 100,
            "duration": 30,
            "query": "SELECT 1",
            "pool": PoolConfig(max_size=200, min_idle=20, connection_timeout_secs=5)
        }
    }
    
    if scenario not in scenarios:
        print(f"Unknown scenario: {scenario}")
        print(f"Available scenarios: {', '.join(scenarios.keys())}")
        return
    
    config = scenarios[scenario]
    
    print(f"🎯 Running '{scenario}' scenario:")
    print(f"   Workers: {config['workers']}")
    print(f"   Duration: {config['duration']}s")
    print(f"   Query: {config['query']}")
    print(f"   Pool max size: {config['pool'].max_size}")
    
    result = await tester.concurrent_load_test(
        test_name=f"Quick_{scenario}_test",
        num_workers=config['workers'],
        duration_seconds=config['duration'],
        pool_config=config['pool'],
        query=config['query']
    )
    
    print(f"\n📊 Results:")
    print(f"   RPS: {result.rps:.2f}")
    print(f"   Avg response time: {result.avg_response_time*1000:.2f}ms")
    print(f"   Error rate: {result.error_rate:.2%}")
    print(f"   Memory usage: {result.memory_usage_mb:.1f}MB")
    
    if result.errors:
        print(f"   First error: {result.errors[0]}")


def main():
    parser = argparse.ArgumentParser(description="Quick FastMSSQL Load Test")
    parser.add_argument("connection_string", help="SQL Server connection string")
    parser.add_argument(
        "--scenario", 
        choices=["basic", "medium", "heavy", "extreme"],
        default="basic",
        help="Test scenario to run"
    )
    
    args = parser.parse_args()
    
    asyncio.run(quick_test(args.connection_string, args.scenario))


if __name__ == "__main__":
    main()
