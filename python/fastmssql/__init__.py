"""
fastmssql: A high-performance Python library for Microsoft SQL Server

This library provides a Python interface to Microsoft SQL Server using the Tiberius
Rust driver for excellent performance and memory safety.

Example (async):
    >>> from fastmssql import Connection
    >>> async with Connection("DATABASE_CONNECTION_STRING") as conn:
    ...     result = await conn.execute("SELECT * FROM users WHERE age > @P1", [18])
    ...     for row in result:
    ...         print(row['name'], row['age'])
"""

# Import everything from the main API module
from .fastmssql import *

__version__ = version()
