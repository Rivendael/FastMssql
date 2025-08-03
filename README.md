
# mssql-python-rust

A high-performance Python library for Microsoft SQL Server, built with Rust using the [Tiberius](https://github.com/prisma/tiberius) driver and [PyO3](https://github.com/PyO3/pyo3).

## Features

- **High Performance**: Built with Rust for memory safety and speed
- **Async/Await Support**: Built on Tokio for excellent concurrency
- **Type Safety**: Strong typing with automatic Python type conversion
- **Connection Pooling**: Efficient connection management
- **Cross-Platform**: Works on Windows, macOS, and Linux
- **Easy Integration**: Drop-in replacement for other SQL Server libraries

## Installation

### Prerequisites

- Python 3.8 or higher
- Rust toolchain (for building from source)
- Microsoft SQL Server (any recent version)

### From Source

1. Clone the repository:
```bash
git clone <your-repo-url>
cd mssql-python-rust
```

2. Install Rust if you haven't already:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

3. Install maturin:
```bash
pip install maturin
```

4. Build and install the package:
```bash
# On Windows
build.bat

# On Unix-like systems
./build.sh

# Or manually
maturin develop --release
```

## Quick Start

### Basic Usage

```python
import mssql_python_rust as mssql

# Connect to SQL Server
connection_string = "Server=localhost;Database=master;Integrated Security=true"

# Using context manager (recommended)
with mssql.connect(connection_string) as conn:
    rows = conn.execute("SELECT @@VERSION as version")
    for row in rows:
        print(row['version'])

# One-liner for simple queries
result = mssql.execute(connection_string, "SELECT GETDATE() as current_time")
print(result[0]['current_time'])
```

### Connection Strings

The library supports standard SQL Server connection string formats:

```python
# Windows Authentication
conn_str = "Server=localhost;Database=MyDB;Integrated Security=true"

# SQL Server Authentication
conn_str = "Server=localhost;Database=MyDB;User Id=sa;Password=MyPassword"

# With specific port
conn_str = "Server=localhost,1433;Database=MyDB;User Id=myuser;Password=mypass"

# Azure SQL Database
conn_str = "Server=tcp:myserver.database.windows.net,1433;Database=MyDB;User Id=myuser;Password=mypass;Encrypt=true"
```

### Working with Data

```python
import mssql_python_rust as mssql

with mssql.connect(connection_string) as conn:
    # Execute queries
    users = conn.execute("SELECT id, name, email FROM users WHERE active = 1")
    
    # Iterate through results
    for user in users:
        print(f"User {user['id']}: {user['name']} ({user['email']})")
    
    # Execute non-query operations
    rows_affected = conn.execute_non_query(
        "UPDATE users SET last_login = GETDATE() WHERE id = 123"
    )
    print(f"Updated {rows_affected} rows")
    
    # Work with different data types
    data = conn.execute("""
        SELECT 
            42 as int_value,
            3.14159 as float_value,
            'Hello World' as string_value,
            GETDATE() as datetime_value,
            CAST(1 as BIT) as bool_value,
            NULL as null_value
    """)
    
    row = data[0]
    for column in row.columns():
        value = row.get(column)
        print(f"{column}: {value} (type: {type(value).__name__})")
```

## Usage

### Synchronous Usage

```python
import mssql_python_rust as mssql

# Basic connection and query
connection_string = "Server=localhost;Database=test;Integrated Security=true"

with mssql.connect(connection_string) as conn:
    rows = conn.execute("SELECT * FROM users WHERE active = 1")
    for row in rows:
        print(f"User: {row['name']}")

# Non-query operations
with mssql.connect(connection_string) as conn:
    affected = conn.execute_non_query("UPDATE users SET last_login = GETDATE()")
    print(f"Updated {affected} rows")
```

### Asynchronous Usage

```python
import asyncio
import mssql_python_rust as mssql

async def main():
    connection_string = "Server=localhost;Database=test;Integrated Security=true"
    
    # Basic async connection and query
    async with mssql.connect_async(connection_string) as conn:
        rows = await conn.execute("SELECT * FROM users WHERE active = 1")
        for row in rows:
            print(f"User: {row['name']}")
    
    # Concurrent operations for better performance
    async def get_user_data(user_id):
        async with mssql.connect_async(connection_string) as conn:
            return await conn.execute(f"SELECT * FROM users WHERE id = {user_id}")
    
    # Execute multiple queries concurrently
    user_ids = [1, 2, 3, 4, 5]
    tasks = [get_user_data(uid) for uid in user_ids]
    results = await asyncio.gather(*tasks)
    
    for user_data in results:
        if user_data:
            print(f"User: {user_data[0]['name']}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Performance Comparison

The async version shines when you have multiple independent operations:

```python
# Synchronous - operations run one after another (slower)
with mssql.connect(connection_string) as conn:
    users = conn.execute("SELECT * FROM users")
    orders = conn.execute("SELECT * FROM orders") 
    products = conn.execute("SELECT * FROM products")

# Asynchronous - operations run concurrently (faster)
async def get_all_data():
    async def get_users():
        async with mssql.connect_async(connection_string) as conn:
            return await conn.execute("SELECT * FROM users")
    
    async def get_orders():
        async with mssql.connect_async(connection_string) as conn:
            return await conn.execute("SELECT * FROM orders")
            
    async def get_products():
        async with mssql.connect_async(connection_string) as conn:
            return await conn.execute("SELECT * FROM products")
    
    return await asyncio.gather(get_users(), get_orders(), get_products())

# This can be 3x faster for independent queries
users, orders, products = await get_all_data()
```

## Examples

Run the provided examples to see both sync and async patterns:

```bash
# Basic synchronous usage
python examples/basic_usage.py

# Advanced synchronous features  
python examples/advanced_usage.py

# Asynchronous usage patterns
python examples/async_usage.py

# Mixed sync/async comparison
python examples/mixed_usage.py
```

## Development

### Building from Source

```bash
# Install development dependencies
pip install maturin pytest pytest-asyncio black ruff

# Build in development mode
maturin develop

# Run tests
python -m pytest tests/

# Format code
black python/
ruff check python/
```

### Project Structure

```
mssql-python-rust/
├── src/                    # Rust source code
│   ├── lib.rs             # Main library entry point
│   ├── connection.rs      # Connection handling
│   ├── query.rs           # Query execution
│   └── types.rs           # Type definitions
├── python/                # Python source code
│   ├── __init__.py        # Main Python module
│   ├── mssql.py          # High-level API
│   └── types.py          # Python type definitions
├── examples/              # Usage examples
├── tests/                 # Test files
├── Cargo.toml            # Rust dependencies
├── pyproject.toml        # Python project configuration
└── README.md             # This file
```

### Testing

Run the examples to test your installation:

```bash
# Basic functionality
python examples/basic_usage.py

# Advanced features
python examples/advanced_usage.py
```

## API Reference

### Core Classes

#### `Connection`
Main connection class for database operations.

**Methods:**
- `connect()` - Connect to the database
- `disconnect()` - Close the connection
- `execute(sql: str) -> List[Row]` - Execute a query
- `execute_non_query(sql: str) -> int` - Execute without returning results
- `is_connected() -> bool` - Check connection status

#### `Row`
Represents a database row with column access.

**Methods:**
- `get(column: str) -> Value` - Get value by column name
- `get_by_index(index: int) -> Value` - Get value by column index
- `columns() -> List[str]` - Get column names
- `values() -> List[Value]` - Get all values
- `to_dict() -> dict` - Convert to dictionary

### Module Functions

- `connect(connection_string: str) -> Connection` - Create a new connection
- `execute(connection_string: str, sql: str) -> List[dict]` - Execute query directly
- `version() -> str` - Get library version

## Documentation

- [Async Usage Guide](ASYNC_USAGE.md) - Detailed guide on async features
- [API Reference](API_REFERENCE.md) - Complete API documentation (TODO)
- [Performance Guide](PERFORMANCE.md) - Performance optimization tips (TODO)

## Async Benefits

- **Non-blocking operations**: Don't block the thread while waiting for database responses
- **Concurrent execution**: Run multiple database operations simultaneously
- **Better resource utilization**: More efficient CPU and memory usage
- **Web framework integration**: Perfect for FastAPI, aiohttp, and other async frameworks
- **Scalability**: Handle more concurrent requests with the same resources

```python
from mssql import Connection

conn = Connection("server", "username", "password", "database")
conn.connect()

result = conn.execute_query("SELECT * FROM my_table")
print(result)
```

## Troubleshooting

### Common Issues

1. **Import Error**: Make sure you've built the extension with `maturin develop`
2. **Connection Fails**: Check your connection string and SQL Server configuration
3. **Build Errors**: Ensure you have the Rust toolchain installed
4. **Windows Issues**: Make sure you have the Microsoft Visual C++ Build Tools

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Tiberius](https://github.com/prisma/tiberius) - The excellent Rust SQL Server driver
- [PyO3](https://github.com/PyO3/pyo3) - Python bindings for Rust
- [Maturin](https://github.com/PyO3/maturin) - Build tool for Python extensions in Rust

## Usage

### Synchronous Usage

```python
import mssql_python_rust as mssql

# Basic connection and query
connection_string = "Server=localhost;Database=test;Integrated Security=true"

with mssql.connect(connection_string) as conn:
    rows = conn.execute("SELECT * FROM users WHERE active = 1")
    for row in rows:
        print(f"User: {row['name']}")

# Non-query operations
with mssql.connect(connection_string) as conn:
    affected = conn.execute_non_query("UPDATE users SET last_login = GETDATE()")
    print(f"Updated {affected} rows")
```

### Asynchronous Usage

```python
import asyncio
import mssql_python_rust as mssql

async def main():
    connection_string = "Server=localhost;Database=test;Integrated Security=true"
    
    # Basic async connection and query
    async with mssql.connect_async(connection_string) as conn:
        rows = await conn.execute("SELECT * FROM users WHERE active = 1")
        for row in rows:
            print(f"User: {row['name']}")
    
    # Concurrent operations for better performance
    async def get_user_data(user_id):
        async with mssql.connect_async(connection_string) as conn:
            return await conn.execute(f"SELECT * FROM users WHERE id = {user_id}")
    
    # Execute multiple queries concurrently
    user_ids = [1, 2, 3, 4, 5]
    tasks = [get_user_data(uid) for uid in user_ids]
    results = await asyncio.gather(*tasks)
    
    for user_data in results:
        if user_data:
            print(f"User: {user_data[0]['name']}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Performance Comparison

The async version shines when you have multiple independent operations:

```python
# Synchronous - operations run one after another (slower)
with mssql.connect(connection_string) as conn:
    users = conn.execute("SELECT * FROM users")
    orders = conn.execute("SELECT * FROM orders") 
    products = conn.execute("SELECT * FROM products")

# Asynchronous - operations run concurrently (faster)
async def get_all_data():
    async def get_users():
        async with mssql.connect_async(connection_string) as conn:
            return await conn.execute("SELECT * FROM users")
    
    async def get_orders():
        async with mssql.connect_async(connection_string) as conn:
            return await conn.execute("SELECT * FROM orders")
            
    async def get_products():
        async with mssql.connect_async(connection_string) as conn:
            return await conn.execute("SELECT * FROM products")
    
    return await asyncio.gather(get_users(), get_orders(), get_products())

# This can be 3x faster for independent queries
users, orders, products = await get_all_data()
```

## Examples

Run the provided examples to see both sync and async patterns:

```bash
# Basic synchronous usage
python examples/basic_usage.py

# Advanced synchronous features  
python examples/advanced_usage.py

# Asynchronous usage patterns
python examples/async_usage.py

# Mixed sync/async comparison
python examples/mixed_usage.py
```

## Documentation

- [Async Usage Guide](ASYNC_USAGE.md) - Detailed guide on async features
- [API Reference](API_REFERENCE.md) - Complete API documentation (TODO)
- [Performance Guide](PERFORMANCE.md) - Performance optimization tips (TODO)

## Async Benefits

- **Non-blocking operations**: Don't block the thread while waiting for database responses
- **Concurrent execution**: Run multiple database operations simultaneously
- **Better resource utilization**: More efficient CPU and memory usage
- **Web framework integration**: Perfect for FastAPI, aiohttp, and other async frameworks
- **Scalability**: Handle more concurrent requests with the same resources

```python
from mssql import Connection

conn = Connection("server", "username", "password", "database")
conn.connect()

result = conn.execute_query("SELECT * FROM my_table")
print(result)
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.