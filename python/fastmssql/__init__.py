"""
fastmssql: A high-performance Python library for Microsoft SQL Server - Pure Rust Implementation

This library provides direct access to high-performance Rust implementations
with minimal Python overhead for maximum performance.

Example (async):
    >>> from fastmssql import Connection
    >>> async with Connection("DATABASE_CONNECTION_STRING") as conn:
    ...     result = await conn.execute("SELECT * FROM users WHERE age > @P1", [18])
    ...     for row in result:
    ...         print(row['name'], row['age'])
"""

# Import from the maturin-generated module
from .fastmssql import *

# Preserve module documentation
__doc__ = fastmssql.__doc__
if hasattr(fastmssql, "__all__"):
    __all__ = fastmssql.__all__
