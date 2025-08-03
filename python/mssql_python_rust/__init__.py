"""
mssql-python-rust: A high-performance Python library for Microsoft SQL Server

This library provides a Python interface to Microsoft SQL Server using the Tiberius
Rust driver for excellent performance and memory safety.

Supports both synchronous and asynchronous operations.

Example (sync):
    >>> import mssql_python_rust as mssql
    >>> conn = mssql.connect("Server=localhost;Database=test;Integrated Security=true")
    >>> with conn:
    ...     rows = conn.execute("SELECT * FROM users WHERE age > 18")
    ...     for row in rows:
    ...         print(row['name'], row['age'])

Example (async):
    >>> import mssql_python_rust as mssql
    >>> async with mssql.connect_async("Server=localhost;Database=test;Integrated Security=true") as conn:
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
    connect = _rust_core.connect
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
        connect = _rust_core.connect
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
        connect = mssql_python_rust.connect
        version = mssql_python_rust.version

from .mssql import (
    MSSQLConnection,
    AsyncMSSQLConnection,
    connect,
    connect_async,
    execute,
    execute_async,
    execute_scalar,
    execute_scalar_async,
)

__version__ = version()

__all__ = [
    # Core Rust types
    'Connection',
    'Query', 
    'Row',
    'Value',
    'PoolConfig',
    # Simplified API
    'connect',
    'connect_async',
    'execute',
    'execute_async',
    'execute_scalar',
    'execute_scalar_async',
    'version',
    # Connection classes
    'MSSQLConnection',
    'AsyncMSSQLConnection',
]

# No need for convenience aliases anymore - the functions are already named properly

# Legacy convenience functions (sync) - kept for backward compatibility
def execute_legacy(connection_string: str, sql: str) -> list:
    """Execute a query directly with a connection string (legacy function).
    
    Args:
        connection_string: Database connection string
        sql: SQL query to execute
        
    Returns:
        List of rows as dictionaries
        
    Example:
        >>> rows = mssql.execute_legacy("Server=localhost;Database=test", "SELECT * FROM users")
    """
    with connect(connection_string) as conn:
        return conn.execute(sql)

def execute_scalar_legacy(connection_string: str, sql: str):
    """Execute a query and return the first column of the first row (legacy function).
    
    Args:
        connection_string: Database connection string
        sql: SQL query to execute
        
    Returns:
        Single value from the first row and column
        
    Example:
        >>> count = mssql.execute_scalar_legacy("Server=localhost;Database=test", "SELECT COUNT(*) FROM users")
    """
    with connect(connection_string) as conn:
        return conn.execute_scalar(sql)