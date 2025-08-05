#!/usr/bin/env python3
"""
Memory benchmarking script for fastmssql vs other Python SQL Server libraries.
Measures memory usage during database operations to compare efficiency.
"""

import asyncio
import gc
import os
import psutil
import traceback
import tracemalloc
from typing import Dict

# Try to import available libraries
libraries = {}

try:
    import fastmssql
    libraries['fastmssql'] = True
except ImportError:
    libraries['fastmssql'] = False
    print("Warning: fastmssql not available")

try:
    import pyodbc
    libraries['pyodbc'] = True
except ImportError:
    libraries['pyodbc'] = False
    print("Warning: pyodbc not available")

try:
    import pymssql
    libraries['pymssql'] = True
except ImportError:
    libraries['pymssql'] = False
    print("Warning: pymssql not available")

try:
    import aioodbc
    libraries['aioodbc'] = True
except ImportError:
    libraries['aioodbc'] = False
    print("Warning: aioodbc not available")


class MemoryProfiler:
    """Memory profiling utility"""
    
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
        
        print(f"{self.name}:")
        print(f"  Memory increase: {self.memory_increase:.2f} MB")
        print(f"  Peak traced memory: {self.peak_memory:.2f} MB")
        print(f"  Start: {self.start_memory:.2f} MB, End: {end_memory:.2f} MB")


async def benchmark_fastmssql_memory(connection_string: str, iterations: int = 100):
    """Benchmark fastmssql memory usage"""
    if not libraries['fastmssql']:
        return None
        
    print(f"\n=== FastMSSQL Memory Benchmark ({iterations} iterations) ===")
    
    with MemoryProfiler("FastMSSQL") as profiler:
        # Create connection pool
        async with fastmssql.Connection(connection_string) as conn:
            
            # Perform multiple queries to test memory accumulation
            for i in range(iterations):
                result = await conn.execute("SELECT @@VERSION, GETDATE(), NEWID()")
                # Process results to ensure they're materialized
                for row in result:
                    _ = row['@@VERSION']
                    
                if i % 20 == 0:
                    gc.collect()  # Periodic cleanup
                    
            # Pool stats
            stats = conn.pool_stats()
            print(f"  Pool stats: {stats}")
            
    return {
        'memory_increase': profiler.memory_increase,
        'peak_memory': profiler.peak_memory,
        'iterations': iterations
    }


def benchmark_pyodbc_memory(connection_string: str, iterations: int = 100):
    """Benchmark pyodbc memory usage"""
    if not libraries['pyodbc']:
        return None
        
    print(f"\n=== PyODBC Memory Benchmark ({iterations} iterations) ===")
    
    with MemoryProfiler("PyODBC") as profiler:
        connections = []
        
        try:
            # Create connections for each query (no built-in pooling)
            for i in range(iterations):
                conn = pyodbc.connect(connection_string)
                connections.append(conn)
                
                cursor = conn.cursor()
                cursor.execute("SELECT @@VERSION, GETDATE(), NEWID()")
                rows = cursor.fetchall()
                
                # Process results
                for row in rows:
                    _ = row[0]
                    
                cursor.close()
                
                if i % 20 == 0:
                    gc.collect()
                    
        finally:
            # Clean up connections
            for conn in connections:
                try:
                    conn.close()
                except:
                    pass
                    
    return {
        'memory_increase': profiler.memory_increase,
        'peak_memory': profiler.peak_memory,
        'iterations': iterations
    }


def benchmark_pymssql_memory(connection_string: str, iterations: int = 100):
    """Benchmark pymssql memory usage"""
    if not libraries['pymssql']:
        return None
        
    print(f"\n=== PyMSSQL Memory Benchmark ({iterations} iterations) ===")
    
    # Convert connection string to pymssql format
    # This is a simplified conversion - might need adjustment
    parts = connection_string.split(';')
    server = None
    database = None
    user = None
    password = None
    
    for part in parts:
        if 'Server=' in part:
            server = part.split('=')[1]
        elif 'Database=' in part:
            database = part.split('=')[1]
        elif 'User Id=' in part:
            user = part.split('=')[1]
        elif 'Password=' in part:
            password = part.split('=')[1]
    
    if not all([server, database, user, password]):
        print("  Skipping pymssql - unable to parse connection string")
        return None
    
    with MemoryProfiler("PyMSSQL") as profiler:
        connections = []
        
        try:
            for i in range(iterations):
                conn = pymssql.connect(server=server, user=user, password=password, database=database)
                connections.append(conn)
                
                cursor = conn.cursor()
                cursor.execute("SELECT @@VERSION, GETDATE(), NEWID()")
                rows = cursor.fetchall()
                
                # Process results
                for row in rows:
                    _ = row[0]
                    
                cursor.close()
                
                if i % 20 == 0:
                    gc.collect()
                    
        except Exception as e:
            print(f"  Error with pymssql: {e}")
            return None
        finally:
            for conn in connections:
                try:
                    conn.close()
                except:
                    pass
                    
    return {
        'memory_increase': profiler.memory_increase,
        'peak_memory': profiler.peak_memory,
        'iterations': iterations
    }


async def benchmark_aioodbc_memory(connection_string: str, iterations: int = 100):
    """Benchmark aioodbc memory usage"""
    if not libraries['aioodbc']:
        return None
        
    print(f"\n=== AioODBC Memory Benchmark ({iterations} iterations) ===")
    
    with MemoryProfiler("AioODBC") as profiler:
        connections = []
        
        try:
            for i in range(iterations):
                conn = await aioodbc.connect(dsn=connection_string)
                connections.append(conn)
                
                cursor = await conn.cursor()
                await cursor.execute("SELECT @@VERSION, GETDATE(), NEWID()")
                rows = await cursor.fetchall()
                
                # Process results
                for row in rows:
                    _ = row[0]
                    
                await cursor.close()
                
                if i % 20 == 0:
                    gc.collect()
                    
        except Exception as e:
            print(f"  Error with aioodbc: {e}")
            return None
        finally:
            for conn in connections:
                try:
                    await conn.close()
                except:
                    pass
                    
    return {
        'memory_increase': profiler.memory_increase,
        'peak_memory': profiler.peak_memory,
        'iterations': iterations
    }


def format_memory_results(results: Dict) -> None:
    """Format and display memory benchmark results"""
    print("\n" + "="*60)
    print("MEMORY BENCHMARK RESULTS")
    print("="*60)
    
    if not results:
        print("No results to display")
        return
    
    # Sort by memory efficiency (lower is better)
    sorted_results = sorted(
        [(name, data) for name, data in results.items() if data],
        key=lambda x: x[1]['memory_increase']
    )
    
    print(f"{'Library':<12} {'Mem Increase':<15} {'Peak Memory':<15} {'Per Operation':<15}")
    print("-" * 60)
    
    baseline = None
    for name, data in sorted_results:
        mem_increase = data['memory_increase']
        peak_memory = data['peak_memory']
        per_op = mem_increase / data['iterations'] * 1024  # KB per operation
        
        if baseline is None:
            baseline = mem_increase
            multiplier = "Baseline"
        else:
            multiplier = f"{mem_increase / baseline:.1f}x more"
        
        print(f"{name:<12} {mem_increase:>10.2f} MB   {peak_memory:>10.2f} MB   {per_op:>10.2f} KB   {multiplier}")
    
    print("\n" + "="*60)
    
    # Summary
    if len(sorted_results) > 1:
        best = sorted_results[0]
        worst = sorted_results[-1]
        improvement = worst[1]['memory_increase'] / best[1]['memory_increase']
        print(f"Memory efficiency: {best[0]} uses {improvement:.1f}x less memory than {worst[0]}")


async def main():
    """Main memory benchmarking function"""
    print("Memory Benchmark for Python SQL Server Libraries")
    print("=" * 50)
    
    # Configuration
    connection_string = os.getenv('CONNECTION_STRING', 
                                 "Server=localhost;Database=master;User Id=sa;Password=YourPassword123")
    iterations = int(os.getenv('ITERATIONS', '50'))  # Fewer iterations for memory testing
    
    print(f"Iterations per library: {iterations}")
    print(f"Available libraries: {[name for name, available in libraries.items() if available]}")
    
    results = {}
    
    # Benchmark each library
    if libraries['fastmssql']:
        try:
            results['fastmssql'] = await benchmark_fastmssql_memory(connection_string, iterations)
        except Exception as e:
            print(f"FastMSSQL benchmark failed: {e}")
            traceback.print_exc()
    
    if libraries['pyodbc']:
        try:
            results['pyodbc'] = benchmark_pyodbc_memory(connection_string, iterations)
        except Exception as e:
            print(f"PyODBC benchmark failed: {e}")
    
    if libraries['pymssql']:
        try:
            results['pymssql'] = benchmark_pymssql_memory(connection_string, iterations)
        except Exception as e:
            print(f"PyMSSQL benchmark failed: {e}")
    
    if libraries['aioodbc']:
        try:
            results['aioodbc'] = await benchmark_aioodbc_memory(connection_string, iterations)
        except Exception as e:
            print(f"AioODBC benchmark failed: {e}")
    
    # Display results
    format_memory_results(results)
    
    # Additional analysis
    print("\nMemory Analysis Notes:")
    print("- Memory increase: Additional RSS memory used during operations")
    print("- Peak memory: Maximum traced memory allocation")
    print("- Per operation: Average memory overhead per database operation")
    print("- Lower values indicate better memory efficiency")


if __name__ == "__main__":
    asyncio.run(main())
