"""
High-level Python API for mssql-python-rust

This module provides convenient Python functions that wrap the Rust core functionality.
Supports both synchronous and asynchronous operations.
"""

import asyncio
from typing import List, Dict, Any, Optional, Union

try:
    import mssql_python_rust as _core
except ImportError:
    # Fallback for development
    try:
        from . import mssql_python_rust as _core
    except ImportError:
        import sys
        print("mssql_python_rust module not found. Make sure you've built it with 'maturin develop'")
        sys.exit(1)

class AsyncMSSQLConnection:
    """Async wrapper around the Rust Connection class."""
    
    def __init__(self, connection_string: str, auto_connect: bool = False):
        """Initialize a new async connection.
        
        Args:
            connection_string: SQL Server connection string
            auto_connect: If True, automatically connect on creation
        """
        self._conn = _core.connect(connection_string)
        self._connected = False
        if auto_connect:
            # Note: Can't await in __init__, so auto_connect won't work for async
            pass
    
    async def connect(self) -> None:
        """Connect to the database asynchronously."""
        await self._conn.connect_async()
        self._connected = True
    
    async def disconnect(self) -> None:
        """Disconnect from the database asynchronously."""
        await self._conn.disconnect_async()
        self._connected = False
    
    def is_connected(self) -> bool:
        """Check if connected to the database."""
        return self._conn.is_connected()
    
    async def execute(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a query asynchronously and return results as dictionaries.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            List of rows as dictionaries
        """
        if not self._connected:
            raise RuntimeError("Not connected to database. Call await conn.connect() first.")
        
        rows = await self._conn.execute_async(sql)
        return [row.to_dict() for row in rows]
    
    async def execute_scalar(self, sql: str) -> Any:
        """Execute a query asynchronously and return the first value.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            First column value of the first row
        """
        results = await self.execute(sql)
        if results:
            first_row = results[0]
            if first_row:
                return list(first_row.values())[0]
        return None
    
    async def execute_non_query(self, sql: str) -> int:
        """Execute a query asynchronously without returning results.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            Number of rows affected
        """
        if not self._connected:
            raise RuntimeError("Not connected to database. Call await conn.connect() first.")
        
        return await self._conn.execute_non_query_async(sql)
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()

class MSSQLConnection:
    """High-level wrapper around the Rust Connection class."""
    
    def __init__(self, connection_string: str, auto_connect: bool = True):
        """Initialize a new connection.
        
        Args:
            connection_string: SQL Server connection string
            auto_connect: If True, automatically connect on creation
        """
        self._conn = _core.connect(connection_string)
        self._connected = False
        if auto_connect:
            self.connect()
    
    def connect(self) -> None:
        """Connect to the database."""
        self._conn.connect()
        self._connected = True
    
    def disconnect(self) -> None:
        """Disconnect from the database."""
        self._conn.disconnect()
        self._connected = False
    
    def is_connected(self) -> bool:
        """Check if connected to the database."""
        return self._conn.is_connected()
    
    def execute(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a query and return results as dictionaries.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            List of rows as dictionaries
        """
        if not self._connected:
            raise RuntimeError("Not connected to database. Call conn.connect() first.")
        
        rows = self._conn.execute(sql)
        return [row.to_dict() for row in rows]
    
    def execute_scalar(self, sql: str) -> Any:
        """Execute a query and return the first value.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            First column value of the first row
        """
        results = self.execute(sql)
        if results:
            first_row = results[0]
            if first_row:
                return list(first_row.values())[0]
        return None
    
    def execute_non_query(self, sql: str) -> int:
        """Execute a query without returning results.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            Number of rows affected
        """
        if not self._connected:
            raise RuntimeError("Not connected to database. Call conn.connect() first.")
        
        return self._conn.execute_non_query(sql)
    
    def __enter__(self):
        """Context manager entry."""
        if not self._connected:
            self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


# Simplified API - these create connected connections ready to use
def connect(connection_string: str) -> MSSQLConnection:
    """Create and connect to MSSQL database synchronously.
    
    Args:
        connection_string: SQL Server connection string
        
    Returns:
        Connected MSSQLConnection instance ready to use
        
    Example:
        conn = mssql.connect("Server=localhost;Database=test;Integrated Security=true")
        rows = conn.execute("SELECT * FROM users")
    """
    return MSSQLConnection(connection_string, auto_connect=True)

def connect_async(connection_string: str) -> AsyncMSSQLConnection:
    """Create async connection to MSSQL database.
    
    Note: You must await conn.connect() or use async with conn: before using.
    
    Args:
        connection_string: SQL Server connection string
        
    Returns:
        AsyncMSSQLConnection instance (not yet connected)
        
    Example:
        async with mssql.connect_async(conn_string) as conn:
            rows = await conn.execute("SELECT * FROM users")
    """
    return AsyncMSSQLConnection(connection_string, auto_connect=False)

# Convenience functions for one-off queries
def execute(connection_string: str, sql: str) -> List[Dict[str, Any]]:
    """Execute a SQL query directly with a connection string.
    
    Args:
        connection_string: SQL Server connection string
        sql: SQL query to execute
        
    Returns:
        List of rows as dictionaries
    """
    with connect(connection_string) as conn:
        return conn.execute(sql)

async def execute_async(connection_string: str, sql: str) -> List[Dict[str, Any]]:
    """Execute a SQL query asynchronously with a connection string.
    
    Args:
        connection_string: SQL Server connection string
        sql: SQL query to execute
        
    Returns:
        List of rows as dictionaries
    """
    async with connect_async(connection_string) as conn:
        return await conn.execute(sql)

def execute_scalar(connection_string: str, sql: str) -> Any:
    """Execute a query and return the first value.
    
    Args:
        connection_string: SQL Server connection string
        sql: SQL query to execute
        
    Returns:
        First column value of the first row
    """
    with connect(connection_string) as conn:
        return conn.execute_scalar(sql)

async def execute_scalar_async(connection_string: str, sql: str) -> Any:
    """Execute a query asynchronously and return the first value.
    
    Args:
        connection_string: SQL Server connection string
        sql: SQL query to execute
        
    Returns:
        First column value of the first row
    """
    async with connect_async(connection_string) as conn:
        return await conn.execute_scalar(sql)