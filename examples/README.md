# Examples for mssql-python-rust

This directory contains example scripts demonstrating the async-only API of mssql-python-rust.

## Available Examples

### [`basic_usage.py`](basic_usage.py)
Demonstrates basic async database operations:
- Simple connection and queries
- Basic data type handling
- Error handling patterns
- Context manager usage

### [`async_usage.py`](async_usage.py)
Advanced async patterns:
- Concurrent query execution
- Connection pool utilization
- Performance comparisons
- Real-world async patterns

### [`advanced_usage.py`](advanced_usage.py)
Complex database operations:
- Bulk operations
- Transaction handling
- Advanced data manipulation
- Error recovery patterns

### [`advanced_pool_config.py`](advanced_pool_config.py)
Connection pool configuration:
- Custom pool settings
- Predefined configurations
- Pool monitoring and statistics
- Performance optimization

## Running the Examples

1. **Prerequisites**: Make sure you have built the library:
   ```bash
   maturin develop --release
   ```

2. **Update connection string**: Edit the connection string in each example to match your SQL Server setup.

3. **Run examples**:
   ```bash
   # From the examples directory
   python basic_usage.py
   python async_usage.py
   python advanced_usage.py
   python advanced_pool_config.py
   ```

## Connection String Examples

Update the `CONNECTION_STRING` variable in each example:

```python
# Windows Authentication
CONNECTION_STRING = "Server=localhost;Database=master;Integrated Security=true;TrustServerCertificate=yes"

# SQL Server Authentication
CONNECTION_STRING = "Server=localhost,1433;Database=mydb;User Id=sa;Password=MyPassword;TrustServerCertificate=yes"

# Azure SQL Database
CONNECTION_STRING = "Server=tcp:myserver.database.windows.net,1433;Database=mydb;User Id=myuser@myserver;Password=mypassword;Encrypt=true"

# Named instance
CONNECTION_STRING = "Server=MYSERVER\\SQLEXPRESS,1433;Database=mydb;Integrated Security=true;TrustServerCertificate=yes"
```

## API Design

All examples use the new async-only API design:

```python
import asyncio
from mssql_python_rust import Connection, PoolConfig

async def main():
    # Simple usage
    async with Connection(connection_string) as conn:
        result = await conn.execute("SELECT @@VERSION")
        rows_affected = await conn.execute_non_query("UPDATE ...")
    
    # With custom pool config
    pool_config = PoolConfig(max_size=10, min_idle=2)
    async with Connection(connection_string, pool_config) as conn:
        result = await conn.execute("SELECT * FROM users")

asyncio.run(main())
```

## Key Benefits Demonstrated

- **Performance**: bb8 connection pooling for high throughput
- **Concurrency**: True async operations with proper resource sharing
- **Simplicity**: Clean API without confusing sync/async method suffixes
- **Reliability**: Automatic connection management and error handling
- **Flexibility**: Configurable connection pooling for different scenarios

Each example includes comprehensive error handling and demonstrates best practices for production use.
