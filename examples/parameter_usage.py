"""
Demonstration of the Parameter and Parameters classes for cleaner parameterized queries.

This example shows different ways to use parameters with Connection.execute().
"""

import asyncio
import os
import sys

# Add the parent directory to the path so we can import mssql
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

from fastmssql import Connection, Parameters, Parameter

async def simple_list_parameters():
    """Example using simple list of parameters (existing approach)."""
    print("=== Simple List Parameters ===")
    
    try:
        async with Connection(
            server="localhost",
            database="tempdb",
            trusted_connection=True
        ) as conn:
            # Traditional way - just a list
            result = await conn.execute(
                "SELECT @P1 as num, @P2 as text", 
                [42, "Hello World"]
            )
            
            for row in result.rows():
                print(f"Number: {row['num']}, Text: {row['text']}")
                
    except Exception as e:
        print(f"Connection failed: {e}")


async def parameters_object_basic():
    """Example using Parameters object - basic usage."""
    print("\n=== Parameters Object - Basic ===")
    
    try:
        async with Connection(
            server="localhost",
            database="tempdb",
            trusted_connection=True
        ) as conn:
            # Using Parameters constructor with positional args
            params = Parameters(25, "John Doe", True)
            
            result = await conn.execute(
                "SELECT @P1 as age, @P2 as name, @P3 as active", 
                params
            )
            
            for row in result.rows():
                print(f"Age: {row['age']}, Name: {row['name']}, Active: {row['active']}")
                
    except Exception as e:
        print(f"Connection failed: {e}")


async def parameters_object_chaining():
    """Example using Parameters object with method chaining."""
    print("\n=== Parameters Object - Method Chaining ===")
    
    try:
        async with Connection(
            server="localhost",
            database="tempdb",
            trusted_connection=True
        ) as conn:
            # Using method chaining for fluent API
            params = (Parameters()
                     .add(100)
                     .add("Product A")
                     .add(29.99))
            
            result = await conn.execute(
                "SELECT @P1 as id, @P2 as product_name, @P3 as price", 
                params
            )
            
            for row in result.rows():
                print(f"ID: {row['id']}, Product: {row['product_name']}, Price: ${row['price']}")
                
    except Exception as e:
        print(f"Connection failed: {e}")


async def parameters_with_type_hints():
    """Example using Parameters with SQL type hints."""
    print("\n=== Parameters with Type Hints ===")
    
    try:
        async with Connection(
            server="localhost",
            database="tempdb",
            trusted_connection=True
        ) as conn:
            # Using type hints for better SQL Server integration
            params = (Parameters()
                     .add(123, "INT")
                     .add("Important Note", "NVARCHAR")
                     .add(3.14159, "FLOAT"))
            
            result = await conn.execute(
                "SELECT @P1 as order_id, @P2 as notes, @P3 as calculated_value", 
                params
            )
            
            for row in result.rows():
                print(f"Order ID: {row['order_id']}")
                print(f"Notes: {row['notes']}")
                print(f"Value: {row['calculated_value']}")
                
    except Exception as e:
        print(f"Connection failed: {e}")


async def individual_parameter_objects():
    """Example using individual Parameter objects."""
    print("\n=== Individual Parameter Objects ===")
    
    try:
        async with Connection(
            server="localhost",
            database="tempdb",
            trusted_connection=True
        ) as conn:
            # Creating individual Parameter objects
            id_param = Parameter(456, "BIGINT")
            name_param = Parameter("Advanced User", "NVARCHAR")
            score_param = Parameter(95.5, "DECIMAL")
            
            params = Parameters(id_param, name_param, score_param)
            
            result = await conn.execute(
                "SELECT @P1 as user_id, @P2 as username, @P3 as score", 
                params
            )
            
            for row in result.rows():
                print(f"User ID: {row['user_id']}, Username: {row['username']}, Score: {row['score']}")
                
    except Exception as e:
        print(f"Connection failed: {e}")


async def reusable_parameters():
    """Example showing how Parameters can be reused and modified."""
    print("\n=== Reusable Parameters ===")
    
    try:
        async with Connection(
            server="localhost",
            database="tempdb",
            trusted_connection=True
        ) as conn:
            # Create a base parameter set
            base_params = Parameters().add("SELECT").add("tempdb")
            
            # First query
            print("First query:")
            result1 = await conn.execute(
                "SELECT @P1 as query_type, @P2 as database_name", 
                base_params
            )
            for row in result1.rows():
                print(f"  Type: {row['query_type']}, DB: {row['database_name']}")
            
            # Add more parameters for second query
            extended_params = Parameters().add("INSERT").add("testdb").add(42)
            
            print("Second query:")
            result2 = await conn.execute(
                "SELECT @P1 as query_type, @P2 as database_name, @P3 as record_count", 
                extended_params
            )
            for row in result2.rows():
                print(f"  Type: {row['query_type']}, DB: {row['database_name']}, Count: {row['record_count']}")
                
    except Exception as e:
        print(f"Connection failed: {e}")


async def main():
    """Run all parameter examples."""
    print("Microsoft SQL Server Parameter Usage Demo")
    print("=" * 50)
    
    await simple_list_parameters()
    await parameters_object_basic()
    await parameters_object_chaining()
    await parameters_with_type_hints()
    await individual_parameter_objects()
    await reusable_parameters()
    
    print("\n" + "=" * 50)
    print("Demo completed!")
    print("\nKey Benefits of Parameters:")
    print("• Cleaner, more readable code")
    print("• Optional SQL type hints for better performance")
    print("• Method chaining for fluent API")
    print("• Reusable parameter objects")
    print("• Still compatible with simple lists")


if __name__ == "__main__":
    asyncio.run(main())
