# Async Support in mssql-python-rust

This document describes the asynchronous features of the mssql-python-rust library.

## Overview

The library now supports both synchronous and asynchronous operations. The async support is built on top of:
- **Rust**: Tokio runtime with Tiberius async SQL Server driver
- **Python**: Native async/await syntax with asyncio integration

## Key Features

### Async Methods
All major operations have async equivalents:
- `connect()` → `connect_async()`
- `execute()` → `execute_async()`
- `execute_non_query()` → `execute_non_query_async()`

### Context Managers
Supports both sync and async context managers:
```python
# Synchronous
with mssql.connect(conn_string) as conn:
    rows = conn.execute("SELECT * FROM users")

# Asynchronous  
async with mssql.connect_async(conn_string) as conn:
    rows = await conn.execute("SELECT * FROM users")
```

## Usage Examples

### Basic Async Usage

```python
import asyncio
import mssql_python_rust as mssql

async def main():
    connection_string = "Server=localhost;Database=test;Integrated Security=true"
    
    async with mssql.connect_async(connection_string) as conn:
        # Simple query
        rows = await conn.execute("SELECT @@VERSION as version")
        print(rows[0]['version'])
        
        # Non-query operation
        affected = await conn.execute_non_query(
            "UPDATE users SET last_login = GETDATE() WHERE active = 1"
        )
        print(f"Updated {affected} rows")

if __name__ == "__main__":
    asyncio.run(main())
```

### Concurrent Operations

The real power of async comes from concurrent operations:

```python
import asyncio
import mssql_python_rust as mssql

async def fetch_user_data(user_id: int, connection_string: str):
    async with mssql.connect_async(connection_string) as conn:
        return await conn.execute(f"SELECT * FROM users WHERE id = {user_id}")

async def main():
    connection_string = "Server=localhost;Database=test;Integrated Security=true"
    user_ids = [1, 2, 3, 4, 5]
    
    # Execute all queries concurrently
    tasks = [fetch_user_data(uid, connection_string) for uid in user_ids]
    results = await asyncio.gather(*tasks)
    
    for user_data in results:
        if user_data:
            print(f"User: {user_data[0]['name']}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Error Handling

```python
import asyncio
import mssql_python_rust as mssql

async def safe_query(connection_string: str, sql: str):
    try:
        async with mssql.connect_async(connection_string) as conn:
            return await conn.execute(sql)
    except Exception as e:
        print(f"Query failed: {e}")
        return []

async def main():
    connection_string = "Server=localhost;Database=test;Integrated Security=true"
    
    # This will handle the error gracefully
    results = await safe_query(connection_string, "SELECT * FROM non_existent_table")
    print(f"Got {len(results)} results")

if __name__ == "__main__":
    asyncio.run(main())
```

## Performance Benefits

### Concurrent Database Operations
When you have multiple independent database operations, async allows them to run concurrently:

```python
# Synchronous - operations run sequentially
def sync_operations():
    with mssql.connect(conn_string) as conn:
        users = conn.execute("SELECT * FROM users")      # Wait
        orders = conn.execute("SELECT * FROM orders")    # Wait  
        products = conn.execute("SELECT * FROM products") # Wait
    return users, orders, products

# Asynchronous - operations run concurrently  
async def async_operations():
    async def get_users():
        async with mssql.connect_async(conn_string) as conn:
            return await conn.execute("SELECT * FROM users")
    
    async def get_orders():
        async with mssql.connect_async(conn_string) as conn:
            return await conn.execute("SELECT * FROM orders")
            
    async def get_products():
        async with mssql.connect_async(conn_string) as conn:
            return await conn.execute("SELECT * FROM products")
    
    # All three queries run concurrently
    return await asyncio.gather(get_users(), get_orders(), get_products())
```

### Integration with Async Web Frameworks

Works seamlessly with FastAPI, aiohttp, and other async frameworks:

```python
from fastapi import FastAPI
import mssql_python_rust as mssql

app = FastAPI()

@app.get("/users/{user_id}")
async def get_user(user_id: int):
    connection_string = "Server=localhost;Database=test;Integrated Security=true"
    
    async with mssql.connect_async(connection_string) as conn:
        users = await conn.execute(f"SELECT * FROM users WHERE id = {user_id}")
        if users:
            return users[0]
        return {"error": "User not found"}
```

## API Reference

### Async Connection Class: `AsyncMSSQLConnection`

```python
class AsyncMSSQLConnection:
    async def connect(self) -> None
    async def disconnect(self) -> None  
    def is_connected(self) -> bool
    async def execute(self, sql: str) -> List[Dict[str, Any]]
    async def execute_scalar(self, sql: str) -> Any
    async def execute_non_query(self, sql: str) -> int
    
    # Async context manager support
    async def __aenter__(self)
    async def __aexit__(self, exc_type, exc_val, exc_tb)
```

### Convenience Functions

```python
# Connection factory
mssql.connect_async(connection_string: str) -> AsyncMSSQLConnection

# Direct execution  
await mssql.execute_async(connection_string: str, sql: str) -> List[Dict[str, Any]]
await mssql.execute_scalar_async(connection_string: str, sql: str) -> Any
```

## Migration from Sync to Async

Converting existing synchronous code to async is straightforward:

1. Add `async` to your function definitions
2. Add `await` before async operations  
3. Use `connect_async()` instead of `connect()`
4. Use `async with` instead of `with`
5. Run your main function with `asyncio.run()`

```python
# Before (sync)
def get_user_count():
    with mssql.connect(conn_string) as conn:
        result = conn.execute_scalar("SELECT COUNT(*) FROM users")
        return result

count = get_user_count()

# After (async)  
async def get_user_count():
    async with mssql.connect_async(conn_string) as conn:
        result = await conn.execute_scalar("SELECT COUNT(*) FROM users")
        return result

count = await get_user_count()
# or
count = asyncio.run(get_user_count())
```

## Building and Installing

To use the async features, make sure you build with the async dependencies:

```bash
# Install/update dependencies
pip install maturin

# Build the extension
maturin develop

# Or for release builds
maturin build --release
```

The async functionality requires:
- Python 3.8+
- Rust with Tokio runtime
- pyo3-asyncio for Python-Rust async bridge
