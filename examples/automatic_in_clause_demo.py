#!/usr/bin/env python3
"""
Demo: Automatic IN Clause Expansion (Rust-Powered)

This example shows how lists, tuples, and other iterables are automatically
expanded for use in SQL IN clauses when using the Parameter and Parameters classes.
The expansion logic is now handled in Rust for maximum performance!
"""

import asyncio
import sys
import os

# Add the parent directory to Python path for development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    from mssql import Connection, Parameter, Parameters
except ImportError:
    print("mssql_python_rust not available - run 'maturin develop' first")
    sys.exit(1)

# Connection string - modify as needed
CONNECTION_STRING = "Server=SNOWFLAKE\\SQLEXPRESS,50014;Database=pymssql_test;Integrated Security=true;TrustServerCertificate=yes"

async def demo_automatic_expansion():
    """Demonstrate automatic iterable expansion (handled by Rust for speed)."""
    
    print("🔧 Demo: Automatic IN Clause Expansion (Rust-Powered)")
    print("=" * 60)
    
    try:
        async with Connection(CONNECTION_STRING) as conn:
            print("✅ Connected to database")
            
            # Example 1: Simple list gets expanded automatically by Rust
            print("\n📋 Example 1: Simple list parameter (Rust expansion)")
            user_ids = [1, 2, 3, 4, 5]
            param = Parameter(user_ids)
            print(f"   Parameter: {param}")
            print(f"   Is expanded: {param.is_expanded}")
            print(f"   Value: {param.value}")
            print("   ⚡ Rust will expand this into multiple @P1, @P2, @P3, @P4, @P5 placeholders")
            
            # The Rust layer handles expanding this into multiple placeholders automatically
            result = await conn.execute("SELECT @P1 as user_id_list", [param])
            if result.has_rows():
                row = result.rows()[0]
                print(f"   Result: {row['user_id_list']}")
            
            # Example 2: Using Parameters class with mixed types (Rust expansion)
            print("\n📋 Example 2: Mixed parameters with Rust-powered expansion")
            params = Parameters(
                "Active",           # Regular string - not expanded
                [10, 20, 30, 40],   # List - automatically expanded by Rust
                True                # Boolean - not expanded
            )
            
            print(f"   Parameters count: {len(params)}")
            for i, p in enumerate(params.positional):
                expansion_note = " (Rust expands)" if p.is_expanded else ""
                print(f"   Param {i+1}: value={p.value}, expanded={p.is_expanded}{expansion_note}")
            
            result = await conn.execute(
                "SELECT @P1 as status, @P2 as id_list, @P3 as active", 
                params
            )
            if result.has_rows():
                row = result.rows()[0]
                print(f"   Results: status={row['status']}, id_list={row['id_list']}, active={row['active']}")
            
            # Example 3: Method chaining with iterables
            print("\n📋 Example 3: Method chaining with automatic expansion")
            params = (Parameters()
                     .add("Manager", "VARCHAR")           # Regular value
                     .add([100, 200, 300], "INT")         # List with type hint
                     .set("dept_codes", ("HR", "IT", "SALES")))  # Tuple gets expanded
            
            print(f"   Parameters: {len(params)} total")
            for p in params.positional:
                print(f"   Positional: value={p.value}, expanded={p.is_expanded}, type={p.sql_type}")
            for name, p in params.named.items():
                print(f"   Named '{name}': value={p.value}, expanded={p.is_expanded}")
            
            # Example 4: What doesn't get expanded
            print("\n📋 Example 4: Strings and bytes are NOT expanded")
            params = Parameters(
                "Hello World",      # String - iterable but not expanded
                b"binary data",     # Bytes - iterable but not expanded
                [1, 2, 3]          # List - expanded
            )
            
            for i, p in enumerate(params.positional):
                print(f"   Param {i+1}: {type(p.value).__name__} -> expanded={p.is_expanded}")
            
            print("\n✅ Demo completed successfully!")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nNote: Make sure the database server is running and accessible.")
        print("Update the CONNECTION_STRING if needed.")

if __name__ == "__main__":
    asyncio.run(demo_automatic_expansion())
