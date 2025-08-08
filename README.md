
# Fastmssql ⚡

A Python library for Microsoft SQL Server built with Rust using the [Tiberius](https://github.com/prisma/tiberius) driver, [PyO3](https://github.com/PyO3/pyo3), and [bb8](https://github.com/djc/bb8) connection pooling.

[![Language](https://img.shields.io/badge/Language-Rust_Backend-red)](https://github.com/Rivendael/pymssql-rs)
[![Async](https://img.shields.io/badge/Async-Native_Tokio-blue)](https://github.com/Rivendael/pymssql-rs)
[![Performance](https://img.shields.io/badge/Performance-17%2C800%2B_RPS-brightgreen)](https://github.com/Rivendael/pymssql-rs)

## Features

- **High Performance**: Exceptional throughput with 17,800+ RPS capability
- **Rust-Powered Backend**: Built with Rust for memory safety and reliability
- **No ODBC Required**: Direct native SQL Server connection without ODBC drivers
- **Connection Pooling**: Intelligent bb8-based connection pooling (default: 75 max, 25 min idle)
- **Async-Only Design**: Built on Tokio with clean async/await API
- **Context Managers**: Automatic resource management with `async with`
- **Type Safety**: Strong typing with automatic Python type conversion
- **Thread Safety**: Support for concurrent operations
- **Cross-Platform**: Works on Windows, macOS, and Linux
- **Simple API**: Clean, intuitive async-only interface with separate `query()` and `execute()` methods

## Key API Methods

FastMSSQL provides two distinct methods for database operations:

- **`query()`** - For SELECT statements that return rows
- **`execute()`** - For INSERT/UPDATE/DELETE statements that return affected row count

```python
# Use query() for SELECT statements
result = await conn.query("SELECT * FROM users WHERE age > @P1", [25])
rows = result.rows()

# Use execute() for data modification
affected = await conn.execute("INSERT INTO users (name) VALUES (@P1)", ["John"])
```

## Performance Highlights

Fastmssql delivers exceptional database performance through Rust-powered architecture:

- **Outstanding Throughput**: Up to **17,800+ RPS** with multiple connection pattern
- **High Performance**: **5,000+ RPS** with single connection (default usage)
- **Low Latency**: ~2ms average query latency under high load
- **Scalable Architecture**: Linear scaling with multiple connection objects
- **Production Ready**: Stable API with comprehensive error handling
- **Connection Pooling**: Efficient resource management with configurable pools
- **Type Conversion**: Automatic conversion between SQL Server and Python types
- **SSL/TLS Support**: Secure connections with flexible encryption options

### Performance Benchmarks

| Pattern | RPS | Configuration | Use Case |
|---------|-----|---------------|----------|
| Single Connection (Default) | **5,000+** | Default pool (75/25) | Standard applications |
| Multiple Connections | **17,800+** | 50 workers, high_throughput() | High-concurrency scenarios |
| Conservative Load | 3,500+ | Shared connection | Traditional pooling |

**Benchmark Environment:**
- Database: SQL Server (local instance)
- Query: `SELECT 1` (minimal overhead)  
- Test Duration: 15-30 seconds per test
- Hardware: Modern development machine

**Key Performance Insights:**
1. **Multiple Connection Objects**: Creating separate `Connection()` objects eliminates serialization bottlenecks
2. **Pool Configuration**: Use `PoolConfig.high_throughput()` for demanding workloads
3. **Optimal Worker Count**: 30-50 concurrent workers provides best throughput
4. **Tokio Optimization**: Aggressive threading configuration maximizes async performance

## Installation

### From PyPI (Recommended)

Install fastmssql using pip:

```bash
pip install fastmssql
```

### Prerequisites

- Python 3.8 to 3.13
- Microsoft SQL Server (any recent version)

### From Source (Development)
Ensure you have Docker, Rust, and Python installed.
If you want to build from source or contribute to development:

1. Clone the repository:
```bash
git clone <your-repo-url>
cd mssql-python-rust
```

2. Run the setup script
```
./setup.sh
```

## Quick Start

### Basic Async Usage (Recommended)

```python
import asyncio
from fastmssql import Connection

async def main():
    # Connect to SQL Server using async context manager
    connection_string = "Server=localhost;Database=master;User Id=myuser;Password=mypass"
    
    # Automatic connection pool management
    async with Connection(connection_string) as conn:
        # Use query() for SELECT statements that return rows
        result = await conn.query("SELECT @@VERSION as version")
        rows = result.rows()
        for row in rows:
            print(row['version'])
        
        # Pool statistics
        stats = conn.pool_stats()
        print(f"Pool: {stats['active_connections']}/{stats['connections']} connections active")

asyncio.run(main())
```

### Connection Methods

The library supports two ways to connect to SQL Server:

#### 1. Connection String (Traditional)

```python
import asyncio
from fastmssql import Connection

async def main():
    # Traditional connection string approach
    connection_string = "Server=localhost;Database=master;User Id=myuser;Password=mypass"
    
    async with Connection(connection_string=connection_string) as conn:
        # Use query() for SELECT statements that return rows
        result = await conn.query("SELECT @@VERSION as version")
        rows = result.rows()
        for row in rows:
            print(row['version'])

asyncio.run(main())
```

#### 2. Individual Parameters

```python
import asyncio
from fastmssql import Connection

async def main():
    # Using individual connection parameters
    
    # SQL Server Authentication  
    async with Connection(
        server="localhost",
        database="master",
        username="myuser",
        password="mypassword"
    ) as conn:
        # Use query() for SELECT statements that return rows
        result = await conn.query("SELECT SUSER_NAME() as login")
        rows = result.rows()
        for row in rows:
            print(f"Logged in as: {row['login']}")

asyncio.run(main())
```

### Performance Patterns

For maximum performance in high-concurrency scenarios, create multiple Connection objects:

```python
import asyncio
from fastmssql import Connection, PoolConfig

async def high_performance_pattern():
    """Optimal pattern for maximum throughput."""
    connection_string = "Server=localhost;Database=master;User Id=myuser;Password=mypass"
    config = PoolConfig.high_throughput()  # Optimized settings
    
    async def worker():
        # Each worker gets its own Connection object for maximum throughput
        async with Connection(connection_string, pool_config=config) as conn:
            for _ in range(1000):
                # Use query() for SELECT statements that return rows
                result = await conn.query("SELECT data FROM my_table WHERE id = @P1", [123])
                rows = result.rows()
                # Process results...
    
    # Launch multiple workers - each with their own connection
    workers = [asyncio.create_task(worker()) for _ in range(50)]
    await asyncio.gather(*workers)
    
    # This pattern can achieve 17,800+ RPS

asyncio.run(high_performance_pattern())
```

**Key Performance Insight**: While a single Connection object provides excellent performance (5,000+ RPS), 
creating multiple Connection objects eliminates serialization bottlenecks and can achieve 17,800+ RPS 
for maximum throughput scenarios.

### Connection Pool Configuration

Configure the connection pool for your specific needs:

```python
import asyncio
from fastmssql import Connection, PoolConfig

async def main():
    # Custom pool configuration
    pool_config = PoolConfig(
        max_size=20,              # Maximum connections in pool
        min_idle=5,               # Minimum idle connections
        max_lifetime_secs=3600,   # Connection max lifetime (1 hour)
        idle_timeout_secs=600,    # Idle connection timeout (10 min)
        connection_timeout_secs=30 # Max wait time for connection
    )

    async with Connection(connection_string, pool_config) as conn:
        # Use query() for SELECT statements that return rows
        result = await conn.query("SELECT * FROM users")
        rows = result.rows()
        for row in rows:
            print(f"User: {row['name']}")
        
    # Predefined configurations for different scenarios
    high_throughput_config = PoolConfig.high_throughput()     # 100 connections, optimized for high RPS
    maximum_performance = PoolConfig.maximum_performance()     # 150 connections, optimized for peak load
    low_resource_config = PoolConfig.low_resource()           # 3 connections, minimal resources  
    dev_config = PoolConfig.development()                     # 5 connections, shorter timeouts

    # Default configuration is optimized for high performance
    # Default: max_size=75, min_idle=25 - ready for production workloads
    
    # For maximum throughput, use multiple Connection objects:
    async def high_perf_worker():
        async with Connection(connection_string, maximum_performance) as conn:
            # Use query() for SELECT statements that return rows
            result = await conn.query("SELECT * FROM fast_table")
            return result.rows()
    
    # Each worker gets its own connection for optimal performance
    tasks = [asyncio.create_task(high_perf_worker()) for _ in range(50)]
    results = await asyncio.gather(*tasks)

asyncio.run(main())
```



### Connection Strings

The library supports standard SQL Server connection string formats:

```python
# SQL Server Authentication
conn_str = "Server=localhost;Database=MyDB;User Id=sa;Password=MyPassword"

# With specific port
conn_str = "Server=localhost,1433;Database=MyDB;User Id=myuser;Password=mypass"

# Azure SQL Database
conn_str = "Server=tcp:myserver.database.windows.net,1433;Database=MyDB;User Id=myuser;Password=mypass;Encrypt=true"
```

### Working with Data

```python
import asyncio
from fastmssql import Connection

async def main():
    async with Connection(connection_string) as conn:
        # === SELECT QUERIES - Use query() method (returns rows) ===
        result = await conn.query("SELECT id, name, email FROM users WHERE active = 1")
        rows = result.rows()
        
        # Iterate through results
        for row in rows:
            print(f"User {row['id']}: {row['name']} ({row['email']})")
        
        # === DATA MODIFICATION - Use execute() method (returns affected row count) ===
        # INSERT operation
        rows_affected = await conn.execute(
            "INSERT INTO users (name, email, active) VALUES (@P1, @P2, @P3)",
            ["John Doe", "john@example.com", 1]
        )
        print(f"Inserted {rows_affected} row(s)")
        
        # UPDATE operation
        rows_affected = await conn.execute(
            "UPDATE users SET last_login = GETDATE() WHERE id = @P1",
            [123]
        )
        print(f"Updated {rows_affected} row(s)")
        
        # DELETE operation
        rows_affected = await conn.execute(
            "DELETE FROM users WHERE active = 0 AND last_login < DATEADD(year, -1, GETDATE())"
        )
        print(f"Deleted {rows_affected} inactive users")
        
        # === WORKING WITH DIFFERENT DATA TYPES ===
        result = await conn.query("""
            SELECT 
                42 as int_value,
                3.14159 as float_value,
                'Hello World' as string_value,
                GETDATE() as datetime_value,
                CAST(1 as BIT) as bool_value,
                NULL as null_value
        """)
        
        rows = result.rows()
        if rows:
            row = rows[0]
            for column_name, value in row.items():
                print(f"{column_name}: {value} (type: {type(value).__name__})")

asyncio.run(main())
```

## Usage

### Asynchronous Usage with Connection Pooling

Full async/await support with automatic connection pool management:

```python
import asyncio
from fastmssql import Connection

async def main():
    connection_string = "Server=localhost;Database=test;Integrated Security=true"
    
    # Async context manager with automatic pool management
    async with Connection(connection_string) as conn:
        # Use query() for SELECT statements that return rows
        result = await conn.query("SELECT name FROM sys.databases")
        rows = result.rows()
        for row in rows:
            print(row['name'])
            
        # Pool statistics
        stats = conn.pool_stats()
        print(f"Pool: {stats['active_connections']}/{stats['connections']} connections active")
    
    # High-performance concurrent operations
    async def fetch_user_data(user_id):
        async with Connection(connection_string) as conn:
            # Use query() for SELECT statements that return rows
            result = await conn.query(f"SELECT * FROM users WHERE id = {user_id}")
            return result.rows()
    
    # Execute multiple queries concurrently using the connection pool
    user_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    tasks = [fetch_user_data(uid) for uid in user_ids]
    results = await asyncio.gather(*tasks)  # bb8 pool handles concurrent connections efficiently
    
    for user_data in results:
        if user_data:
            print(f"User: {user_data[0]['name']}")

asyncio.run(main())
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

### Query vs Execute Methods

FastMSSQL provides two distinct methods for database operations:

- **`query()`** - For SELECT statements that return rows
  - Returns a `QueryResult` object with a `rows()` method
  - Use for retrieving data from the database

- **`execute()`** - For INSERT, UPDATE, DELETE statements  
  - Returns the number of affected rows as an integer
  - Use for modifying data in the database

**SQL Server Parameter Syntax**: Use positional parameters `@P1`, `@P2`, `@P3`, etc.

```python
# SELECT queries - use query()
result = await conn.query("SELECT * FROM users WHERE age > @P1 AND city = @P2", [25, "New York"])
rows = result.rows()

# INSERT/UPDATE/DELETE - use execute()  
affected = await conn.execute("INSERT INTO users (name, email) VALUES (@P1, @P2)", ["John", "john@example.com"])
```

### Core Classes

#### `Connection`
Main connection class with bb8 connection pool management.

**Constructor:**
```python
Connection(connection_string: str, pool_config: Optional[PoolConfig] = None)
```

**Context Manager Support:**
```python
# Asynchronous (recommended)
async with Connection(conn_str) as conn:
    # Use query() for SELECT statements
    result = await conn.query("SELECT * FROM table")
    rows = result.rows()
    
    # Use execute() for INSERT/UPDATE/DELETE statements  
    affected = await conn.execute("INSERT INTO table (col) VALUES (@P1)", ["value"])
```
```

**Methods:**
- `query(sql: str, params: Optional[List] = None) -> QueryResult` - Execute SELECT queries that return rows
- `execute(sql: str, params: Optional[List] = None) -> int` - Execute INSERT/UPDATE/DELETE statements, returns affected row count
- `pool_stats() -> dict` - Get connection pool statistics
- `disconnect()` - Close the connection pool
- `is_connected() -> bool` - Check if pool is active

**Method Details:**

`query()` - For SELECT statements that return data:
```python
# Returns a QueryResult object with rows() method
result = await conn.query("SELECT * FROM users WHERE age > @P1", [21])
rows = result.rows()
for row in rows:
    print(row['name'])
```

`execute()` - For INSERT/UPDATE/DELETE statements:
```python
# Returns the number of affected rows
affected = await conn.execute("INSERT INTO users (name) VALUES (@P1)", ["John"])
print(f"Inserted {affected} row(s)")

affected = await conn.execute("UPDATE users SET age = @P1 WHERE name = @P2", [25, "John"])
print(f"Updated {affected} row(s)")
```

**Pool Statistics:**
```python
stats = conn.pool_stats()
# Returns: {
#     'connections': 10,        # Total connections in pool
#     'active_connections': 3,  # Currently active connections  
#     'idle_connections': 7     # Available idle connections
# }
```

#### `PoolConfig` 
Configuration class for bb8 connection pool settings.

**Constructor:**
```python
PoolConfig(
    max_size: int = 10,                    # Maximum connections in pool
    min_idle: int = 0,                     # Minimum idle connections
    max_lifetime_secs: Optional[int] = None,  # Connection max lifetime
    idle_timeout_secs: Optional[int] = None,  # Idle connection timeout
    connection_timeout_secs: int = 30         # Max wait time for connection
)
```

**Predefined Configurations:**
```python
# High throughput applications (web servers, APIs)
config = PoolConfig.high_throughput()    # 20 connections, optimized settings

# Low resource environments (embedded, containers)  
config = PoolConfig.low_resource()       # 3 connections, minimal overhead

# Development environments
config = PoolConfig.development()        # 5 connections, shorter timeouts
```

#### `Row`
Represents a database row with column access.

**Methods:**
- `get(column: str) -> Value` - Get value by column name
- `get_by_index(index: int) -> Value` - Get value by column index  
- `columns() -> List[str]` - Get column names
- `values() -> List[Value]` - Get all values
- `to_dict() -> dict` - Convert to dictionary

### Module Functions

#### Connection Management
```python
# Create connection with connection pooling
Connection(connection_string: str, pool_config: Optional[PoolConfig] = None) -> Connection

# Usage with async context manager (recommended)
async with Connection(connection_string) as conn:
    # Use query() for SELECT statements that return rows
    result = await conn.query("SELECT * FROM users")
    rows = result.rows()
    
    # Use execute() for INSERT/UPDATE/DELETE statements that return affected count
    affected = await conn.execute("INSERT INTO users (name) VALUES (@P1)", ["John"])
```

#### Utility Functions
```python
version() -> str  # Get library version
```

### Connection Pool Architecture

The library uses the bb8 connection pool for efficient resource management:

1. **Pool Initialization**: Creates a pool of reusable connections on first use
2. **Connection Reuse**: Automatically reuses idle connections for new requests
3. **Load Balancing**: Distributes connections across concurrent operations
4. **Automatic Cleanup**: Closes idle connections based on timeout settings
5. **Thread Safety**: Safe for use across multiple threads and async tasks

### Error Handling

```python
try:
    async with mssql.connect_async(connection_string) as conn:
        result = await conn.query("SELECT * FROM invalid_table")
        rows = result.rows()
except Exception as e:
    print(f"Database error: {e}")
    # Connection automatically returned to pool even on error
```

## Migration to Async-Only Architecture

This library has been upgraded to use async-only operations with the bb8 connection pool for improved performance and reliability.

**Async-Only API:**
```python  
# Async-only with automatic connection pooling
async with mssql.connect_async(conn_str) as conn:
    result = await conn.query("SELECT * FROM table")
    rows = result.rows()
    
    # Pool statistics
    stats = conn.pool_stats()
    print(f"Pool utilization: {stats['active_connections']}/{stats['connections']}")
```

## Advanced Usage Patterns

### Custom Pool Configuration for Different Scenarios

```python
from fastmssql import Connection, PoolConfig

# High-load web application
web_config = PoolConfig(
    max_size=50,               # Handle many concurrent requests
    min_idle=10,               # Keep connections ready
    max_lifetime_secs=1800,    # 30 min connection lifetime
    idle_timeout_secs=300,     # 5 min idle timeout
    connection_timeout_secs=10 # Fast timeout for web responses
)

# Batch processing application  
batch_config = PoolConfig(
    max_size=5,                # Fewer connections
    min_idle=2,                # Always keep some ready
    max_lifetime_secs=7200,    # 2 hour lifetime for long operations
    idle_timeout_secs=1800,    # 30 min idle timeout
    connection_timeout_secs=60 # Longer timeout for batch work
)

# Microservice with limited resources
micro_config = PoolConfig(
    max_size=3,                # Minimal connections
    min_idle=1,                # One always ready
    max_lifetime_secs=3600,    # 1 hour lifetime
    idle_timeout_secs=600,     # 10 min idle timeout
    connection_timeout_secs=15 # Quick timeout
)
```

### Monitoring Pool Health

```python
async def monitor_database_pool():
    """Monitor connection pool health and performance"""
    
    async with mssql.connect_async(connection_string) as conn:
        while True:
            stats = conn.pool_stats()
            utilization = stats['active_connections'] / stats['connections'] * 100
            
            print(f"Pool Utilization: {utilization:.1f}%")
            print(f"Active: {stats['active_connections']}, Idle: {stats['idle_connections']}")
            
            # Alert if pool utilization is too high
            if utilization > 90:
                print("WARNING: High pool utilization detected!")
                
            await asyncio.sleep(30)  # Check every 30 seconds
```

### Optimizing for Different Database Workloads

```python
# OLTP (Online Transaction Processing) - Many small, fast queries
oltp_config = PoolConfig.high_throughput()
async def oltp_operations():
    async with mssql.connect_async(conn_str, oltp_config) as conn:
        # Fast, concurrent transactions
        tasks = [
            conn.query("SELECT * FROM users WHERE id = @P1", [user_id])
            for user_id in range(1, 101)
        ]
        results = await asyncio.gather(*tasks)

# OLAP (Online Analytical Processing) - Fewer, longer-running queries  
olap_config = PoolConfig.low_resource()
async def olap_operations():
    async with mssql.connect_async(conn_str, olap_config) as conn:
        # Long-running analytical queries
        result = await conn.query("""
            SELECT 
                DATE_TRUNC('quarter', order_date) as quarter,
                SUM(total_amount) as total_revenue,
                COUNT(*) as order_count
            FROM orders 
            WHERE order_date >= '2024-01-01'
            GROUP BY DATE_TRUNC('quarter', order_date)
            ORDER BY quarter
        """)
        return result.rows()
```

## API Reference

### Common Issues

1. **Import Error**: Make sure you've built the extension with `maturin develop`
2. **Connection Fails**: Check your connection string and SQL Server configuration. Note that Windows authentication is not supported - use SQL Server authentication with username and password.
3. **Build Errors**: Ensure you have the Rust toolchain installed
4. **Build Issues**: Make sure you have the Microsoft Visual C++ Build Tools on Windows


## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.


## License

This project is licensed under the PolyForm Noncommercial License 1.0.0. See the LICENSE file for details.

## Third-Party Attributions

This project includes and depends on third-party libraries licensed under the Apache License 2.0 and MIT License, as well as other open source licenses.

**Note:** Additional third-party libraries and their license information are listed in `licenses/NOTICE.txt`.

- [Tiberius](https://github.com/prisma/tiberius) (Apache License 2.0)
- [PyO3](https://github.com/PyO3/pyo3) (Apache License 2.0)
- [pyo3-asyncio](https://github.com/PyO3/pyo3-asyncio) (Apache License 2.0)
- [bb8](https://github.com/djc/bb8) (MIT or Apache License 2.0)
- [bb8-tiberius](https://github.com/prisma/tiberius) (Apache License 2.0)
- [tokio](https://github.com/tokio-rs/tokio) (MIT or Apache License 2.0)
- [tokio-util](https://github.com/tokio-rs/tokio) (MIT or Apache License 2.0)
- [futures](https://github.com/rust-lang/futures-rs) (MIT or Apache License 2.0)
- [serde](https://github.com/serde-rs/serde) (MIT or Apache License 2.0)
- [serde_json](https://github.com/serde-rs/json) (MIT or Apache License 2.0)
- [anyhow](https://github.com/dtolnay/anyhow) (MIT or Apache License 2.0)
- [chrono](https://github.com/chronotope/chrono) (MIT or Apache License 2.0)
- [uuid](https://github.com/uuid-rs/uuid) (MIT or Apache License 2.0)
- [tempfile](https://github.com/Stebalien/tempfile) (MIT or Apache License 2.0)
- [pytest](https://github.com/pytest-dev/pytest) (MIT License)
- [pytest-asyncio](https://github.com/pytest-dev/pytest-asyncio) (MIT License)
- [black](https://github.com/psf/black) (MIT License)
- [ruff](https://github.com/astral-sh/ruff) (MIT License)
- [maturin](https://github.com/PyO3/maturin) (Apache License 2.0 or MIT License)
- [Python](https://www.python.org/) and [asyncio](https://docs.python.org/3/library/asyncio.html) (Python Software Foundation License)

See the `licenses/NOTICE.txt` file for full attribution and copyright information.
The full text of the Apache License 2.0 is provided in the `licenses/APACHE_LICENSE_2.0.txt` file.
The full text of the MIT License is provided in the `licenses/MIT_LICENSE.txt` file.

## Acknowledgments

- [Tiberius](https://github.com/prisma/tiberius) - Rust SQL Server driver (Apache License 2.0)
- [PyO3](https://github.com/PyO3/pyo3) - Python bindings for Rust (Apache License 2.0)
- [pyo3-asyncio](https://github.com/PyO3/pyo3-asyncio) - Async bridge for PyO3 (Apache License 2.0)
- [pytest](https://github.com/pytest-dev/pytest) - Python testing framework (MIT License)
- [pytest-asyncio](https://github.com/pytest-dev/pytest-asyncio) - Async test support for pytest (MIT License)
- [black](https://github.com/psf/black) - Python code formatter (MIT License)
- [ruff](https://github.com/astral-sh/ruff) - Python linter (MIT License)
- [Python](https://www.python.org/) and [asyncio](https://docs.python.org/3/library/asyncio.html) - Python standard library (Python Software Foundation License)
- [Maturin](https://github.com/PyO3/maturin) - Build tool for Python extensions in Rust