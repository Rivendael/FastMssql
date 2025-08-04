"""
mssql-python-rust: A high-performance Python library for Microsoft SQL Server

This library provides a Python interface to Microsoft SQL Server using the Tiberius
Rust driver for excellent performance and memory safety.

**ASYNC ONLY**: This library only supports asynchronous operations.

Basic Usage:
    >>> import mssql
    
    # Using the connect() function:
    >>> async with mssql.connect("Server=localhost;Database=test;Integrated Security=true") as conn:
    ...     result = await conn.execute("SELECT * FROM users WHERE age > 18")
    ...     for row in result.rows:
    ...         print(row['name'], row['age'])
    
    # Or using the Connection class directly:
    >>> async with mssql.Connection("Server=localhost;Database=test;Integrated Security=true") as conn:
    ...     result = await conn.execute("SELECT * FROM users WHERE age > 18")
    ...     for row in result.rows:
    ...         print(row['name'], row['age'])

One-off Queries:
    >>> result = await mssql.execute_async(conn_string, "SELECT COUNT(*) FROM users")
    >>> count = result.rows[0][0]
    
    >>> rows_dict = await mssql.execute_dict_async(conn_string, "SELECT name, age FROM users")
    >>> for user in rows_dict:
    ...     print(f"{user['name']} is {user['age']} years old")
"""

# Import core Rust types
try:
    from .mssql_python_rust import (
        PyConnection,  # Keep original name to avoid conflict
        PyQuery as Query,
        PyRow,
        PyValue,
        PyExecutionResult,
        PyPoolConfig,
        connect as rust_connect,  # Rename to avoid conflict
        version,
    )
except ImportError:
    # Fallback for development builds
    try:
        from mssql_python_rust import (
            PyConnection,
            PyQuery as Query,
            PyRow,
            PyValue,
            PyExecutionResult,
            PyPoolConfig,
            connect as rust_connect,
            version,
        )
    except ImportError:
        # Direct import from the built module
        import mssql_python_rust
        PyConnection = mssql_python_rust.PyConnection
        Query = mssql_python_rust.PyQuery
        PyRow = mssql_python_rust.PyRow
        PyValue = mssql_python_rust.PyValue
        PyExecutionResult = mssql_python_rust.PyExecutionResult
        PyPoolConfig = mssql_python_rust.PyPoolConfig
        rust_connect = mssql_python_rust.connect
        version = mssql_python_rust.version

# Import high-level Python API
from .mssql import (
    Connection,
    Row,
    ExecutionResult,
    PoolConfig,
    connect,
    execute_async,
    execute_scalar_async,
    execute_dict_async,
)

__version__ = version()

# Main public API - what users should primarily use
__all__ = [
    # High-level async API (recommended)
    'Connection',
    'connect',
    'execute_async', 
    'execute_scalar_async',
    'execute_dict_async',
    'Row',
    'ExecutionResult',
    'PoolConfig',
    
    # Lower-level API for advanced usage
    'PyConnection',
    'Query',
    'PyRow',
    'PyValue', 
    'PyExecutionResult',
    'PyPoolConfig',
    'rust_connect',
    'version'
]