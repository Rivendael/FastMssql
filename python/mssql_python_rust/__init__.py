"""
mssql-python-rust: A high-performance Python library for Microsoft SQL Server

This library provides a Python interface to Microsoft SQL Server using the Tiberius
Rust driver for excellent performance and memory safety.

Supports asynchronous operations.

Example (async):
    >>> from mssql_python_rust import Connection
    >>> async with Connection("DATABASE_CONNECTION_STRING") as conn:
    ...     rows = await conn.execute("SELECT * FROM users WHERE age > 18")
    ...     for row in rows:
    ...         print(row['name'], row['age'])
"""

try:
    from . import mssql_python_rust as _rust_core
    Connection = _rust_core.Connection
    Query = _rust_core.Query
    Row = _rust_core.Row
    Value = _rust_core.Value
    PoolConfig = _rust_core.PoolConfig
    version = _rust_core.version
except ImportError:
    # Fallback for development builds
    try:
        import mssql_python_rust.mssql_python_rust as _rust_core
        Connection = _rust_core.Connection
        Query = _rust_core.Query
        Row = _rust_core.Row
        Value = _rust_core.Value
        PoolConfig = _rust_core.PoolConfig
        version = _rust_core.version
    except ImportError:
        # Direct import from the built module
        print("Warning: Using direct module import")
        import mssql_python_rust
        Connection = mssql_python_rust.Connection
        Query = mssql_python_rust.Query
        Row = mssql_python_rust.Row
        Value = mssql_python_rust.Value
        PoolConfig = mssql_python_rust.PoolConfig
        version = mssql_python_rust.version

__version__ = version()

__all__ = [
    # Core Rust types
    'Connection',
    'Query', 
    'Row',
    'Value',
    'PoolConfig',
    'version',
]