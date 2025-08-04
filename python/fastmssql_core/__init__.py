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
    from . import fastmssql_core as _rust_core
    PyConnection = _rust_core.Connection
    PyQuery = _rust_core.Query
    PyRow = _rust_core.Row
    PyValue = _rust_core.Value
    PyExecutionResult = _rust_core.ExecutionResult
    PyPoolConfig = _rust_core.PoolConfig
    PySslConfig = _rust_core.SslConfig
    EncryptionLevel = _rust_core.EncryptionLevel
    version = _rust_core.version
except ImportError:
    # Fallback for development builds
    try:
        import fastmssql_core as _rust_core
        PyConnection = _rust_core.Connection
        PyQuery = _rust_core.Query
        PyRow = _rust_core.Row
        PyValue = _rust_core.Value
        PyExecutionResult = _rust_core.ExecutionResult
        PyPoolConfig = _rust_core.PoolConfig
        PySslConfig = _rust_core.SslConfig
        EncryptionLevel = _rust_core.EncryptionLevel
        version = _rust_core.version
    except ImportError:
        # Direct import from the built module
        print("Warning: Using direct module import")
        import fastmssql_core
        PyConnection = fastmssql_core.Connection
        PyQuery = fastmssql_core.Query
        PyRow = fastmssql_core.Row
        PyValue = fastmssql_core.Value
        PyExecutionResult = fastmssql_core.ExecutionResult
        PyPoolConfig = fastmssql_core.PoolConfig
        PySslConfig = fastmssql_core.SslConfig
        EncryptionLevel = fastmssql_core.EncryptionLevel
        version = fastmssql_core.version

__version__ = version()

__all__ = [
    # Core Rust types
    'PyConnection',
    'PyQuery', 
    'PyRow',
    'PyValue',
    'PyExecutionResult',
    'PyPoolConfig',
    'PySslConfig',
    'EncryptionLevel',
    'version',
]