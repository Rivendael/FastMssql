"""
High-level Python API for mssql-python-rust

This module provides convenient Python functions that wrap the Rust core functionality.
Supports asynchronous operations only.
"""

from typing import List, Dict, Any, Optional, Union

try:
    # Try to import the compiled Rust module directly
    import mssql_python_rust.mssql_python_rust as _core
except ImportError:
    # Fallback for development
    try:
        import mssql_python_rust as _core
    except ImportError:
        import sys
        print("mssql_python_rust module not found. Make sure you've built it with 'maturin develop'")
        sys.exit(1)

class Row:
    """Python wrapper around Row for better type hints and documentation."""
    
    def __init__(self, py_row):
        """Initialize with a Row instance."""
        self._row = py_row
    
    def get(self, column: Union[str, int]) -> Any:
        """Get value by column name or index.
        
        Args:
            column: Column name (str) or index (int)
            
        Returns:
            The column value
        """
        return self._row.get(column)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert row to dictionary.
        
        Returns:
            Dictionary mapping column names to values
        """
        return self._row.to_dict()
    
    def to_tuple(self) -> tuple:
        """Convert row to tuple.
        
        Returns:
            Tuple of column values in order
        """
        return self._row.to_tuple()
    
    def __getitem__(self, key: Union[str, int]) -> Any:
        """Get value by column name or index."""
        return self._row[key]
    
    def __len__(self) -> int:
        """Get number of columns in the row."""
        return len(self._row)
    
    def __repr__(self) -> str:
        """String representation of the row."""
        return f"Row({self.to_dict()})"


class ExecutionResult:
    """Python wrapper around ExecutionResult for better type hints."""
    
    def __init__(self, py_result):
        """Initialize with a ExecutionResult instance."""
        self._result = py_result
    
    def rows(self) -> List[Row]:
        """Query result rows (for SELECT queries).
        
        Returns:
            List of Row objects
        """
        if self._result.has_rows():
            # Get raw rows - could be property or method
            try:
                if callable(self._result.rows):
                    raw_rows = self._result.rows()
                else:
                    raw_rows = self._result.rows
                return [Row(py_row) for py_row in raw_rows]
            except Exception:
                return []
        return []
    
    @property
    def affected_rows(self) -> Optional[int]:
        """Number of affected rows (for INSERT/UPDATE/DELETE).
        
        Returns:
            Number of affected rows, or None if not applicable
        """
        return self._result.affected_rows
    
    def has_rows(self) -> bool:
        """Check if result contains rows.
        
        Returns:
            True if result has rows (SELECT query), False otherwise
        """
        return self._result.has_rows()
    
    def __len__(self) -> int:
        """Get number of rows in the result."""
        return len(self.rows())
    
    def __iter__(self):
        """Iterate over rows in the result."""
        return iter(self.rows())
    
    def __repr__(self) -> str:
        """String representation of the result."""
        if self.has_rows():
            return f"ExecutionResult(rows={len(self.rows())})"
        else:
            return f"ExecutionResult(affected_rows={self.affected_rows})"


class Query:
    """A parameterized SQL query for safe execution."""
    
    def __init__(self, sql: str):
        """Initialize a new parameterized query.
        
        Args:
            sql: SQL query string with parameter placeholders (?)
        """
        self._query = _core.Query(sql)
    
    def add_parameter(self, value: Any) -> None:
        """Add a parameter to the query.
        
        Args:
            value: Parameter value (int, float, str, bool, bytes, None)
        """
        self._query.add_parameter(value)
    
    def set_parameters(self, params: List[Any]) -> None:
        """Set all parameters at once.
        
        Args:
            params: List of parameter values
        """
        self._query.set_parameters(params)
    
    @property
    def sql(self) -> str:
        """Get the SQL string."""
        return self._query.get_sql()
    
    @property 
    def parameters(self) -> List[Any]:
        """Get the parameter list."""
        return self._query.get_parameters()
    
    async def execute(self, connection: 'Connection') -> ExecutionResult:
        """Execute the query on a connection.
        
        Args:
            connection: Connection object to execute the query on
            
        Returns:
            ExecutionResult object with rows or affected row count
        """
        raw_result = await self._query.execute(connection._conn)
        return ExecutionResult(raw_result)
    
    async def execute_non_query(self, connection: 'Connection') -> int:
        """Execute the query and return affected row count.
        
        Args:
            connection: Connection object to execute the query on
            
        Returns:
            Number of affected rows
        """
        result = await self.execute(connection)
        return result.affected_rows or 0
    
    def __str__(self) -> str:
        """String representation of the query."""
        return self._query.__str__()
    
    def __repr__(self) -> str:
        """Representation of the query."""
        return self._query.__repr__()
class PoolConfig:
    """Python wrapper around PoolConfig for better documentation."""
    
    def __init__(
        self,
        max_size: int = 10,
        min_idle: Optional[int] = None,
        max_lifetime_secs: Optional[int] = None,
        idle_timeout_secs: Optional[int] = None,
        connection_timeout_secs: Optional[int] = None
    ):
        """Initialize connection pool configuration.
        
        Args:
            max_size: Maximum number of connections in pool (default: 10)
            min_idle: Minimum number of idle connections to maintain
            max_lifetime_secs: Maximum lifetime of connections in seconds
            idle_timeout_secs: How long a connection can be idle before being closed (seconds)
            connection_timeout_secs: Timeout for establishing new connections (seconds)
        """
        self._config = _core.PoolConfig(
            max_size=max_size,
            min_idle=min_idle,
            max_lifetime_secs=max_lifetime_secs,
            idle_timeout_secs=idle_timeout_secs,
            connection_timeout_secs=connection_timeout_secs
        )
    
    @property
    def max_size(self) -> int:
        """Maximum number of connections in pool."""
        return self._config.max_size
    
    @property
    def min_idle(self) -> Optional[int]:
        """Minimum number of idle connections."""
        return self._config.min_idle
    
    @property
    def max_lifetime_secs(self) -> Optional[int]:
        """Maximum lifetime of connections in seconds."""
        return self._config.max_lifetime_secs
    
    @property
    def idle_timeout_secs(self) -> Optional[int]:
        """Idle timeout in seconds."""
        return self._config.idle_timeout_secs
    
    @property
    def connection_timeout_secs(self) -> Optional[int]:
        """Connection timeout in seconds."""
        return self._config.connection_timeout_secs
    
    @staticmethod
    def high_throughput() -> 'PoolConfig':
        """Create configuration for high-throughput scenarios."""
        config = PoolConfig.__new__(PoolConfig)
        config._config = _core.PoolConfig.high_throughput()
        return config
    
    @staticmethod
    def low_resource() -> 'PoolConfig':
        """Create configuration for low-resource scenarios."""
        config = PoolConfig.__new__(PoolConfig)
        config._config = _core.PoolConfig.low_resource()
        return config
    
    @staticmethod 
    def development() -> 'PoolConfig':
        """Create configuration for development scenarios."""
        config = PoolConfig.__new__(PoolConfig)
        config._config = _core.PoolConfig.development()
        return config


class Connection:
    """Async connection to Microsoft SQL Server database with enhanced type support."""
    
    def __init__(
        self, 
        connection_string: str, 
        pool_config: Optional[PoolConfig] = None,
        auto_connect: bool = False
    ):
        """Initialize a new async connection.
        
        Args:
            connection_string: SQL Server connection string
            pool_config: Optional connection pool configuration
            auto_connect: If True, automatically connect on creation (not supported for async)
        """
        py_pool_config = pool_config._config if pool_config else None
        self._conn = _core.connect(connection_string, py_pool_config)
        self._connected = False
        if auto_connect:
            # Note: Can't await in __init__, so auto_connect won't work for async
            pass
    
    async def connect(self) -> None:
        """Connect to the database asynchronously."""
        await self._conn.connect()
        self._connected = True
    
    async def disconnect(self) -> None:
        """Disconnect from the database asynchronously."""
        await self._conn.disconnect()
        self._connected = False
    
    async def is_connected(self) -> bool:
        """Check if connected to the database."""
        return await self._conn.is_connected()
    
    async def execute(self, sql: str, parameters: Optional[List[Any]] = None) -> ExecutionResult:
        """Execute a query asynchronously and return enhanced results.
        
        Args:
            sql: SQL query to execute
            parameters: Optional list of parameter values for parameterized queries
            
        Returns:
            ExecutionResult object with rows or affected row count
        """
        if not self._connected:
            raise RuntimeError("Not connected to database. Call await conn.connect() first.")
        
        if parameters is None:
            py_result = await self._conn.execute(sql)
        else:
            py_result = await self._conn.execute_with_python_params(sql, parameters)
        
        return ExecutionResult(py_result)
    
    async def execute_dict(self, sql: str, parameters: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query asynchronously and return results as dictionaries.
        
        Args:
            sql: SQL query to execute
            parameters: Optional list of parameter values for parameterized queries
            
        Returns:
            List of rows as dictionaries
        """
        result = await self.execute(sql, parameters)
        return [row.to_dict() for row in result.rows()]
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()


# Simplified API - create async connections
def connect(
    connection_string: str, 
    pool_config: Optional[PoolConfig] = None
) -> Connection:
    """Create async connection to MSSQL database.
    
    Note: You must await conn.connect() or use async with conn: before using.
    
    Args:
        connection_string: SQL Server connection string
        pool_config: Optional connection pool configuration
        
    Returns:
        Connection instance (not yet connected)
        
    Example:
        async with mssql.connect(conn_string) as conn:
            result = await conn.execute("SELECT * FROM users")
            for row in result.rows:
                print(row['name'])
                
        # Or create directly:
        async with mssql.Connection(conn_string) as conn:
            result = await conn.execute("SELECT * FROM users")
    """
    return Connection(connection_string, pool_config, auto_connect=False)

# Convenience functions for one-off queries
async def execute_async(
    connection_string: str, 
    sql: str,
    pool_config: Optional[PoolConfig] = None
) -> ExecutionResult:
    """Execute a SQL query asynchronously with a connection string.
    
    Args:
        connection_string: SQL Server connection string
        sql: SQL query to execute
        pool_config: Optional connection pool configuration
        
    Returns:
        ExecutionResult object with rows or affected row count
    """
    async with connect(connection_string, pool_config) as conn:
        return await conn.execute(sql)

async def execute_dict_async(
    connection_string: str, 
    sql: str,
    pool_config: Optional[PoolConfig] = None
) -> List[Dict[str, Any]]:
    """Execute a query asynchronously and return results as dictionaries.
    
    Args:
        connection_string: SQL Server connection string
        sql: SQL query to execute
        pool_config: Optional connection pool configuration
        
    Returns:
        List of rows as dictionaries
    """
    async with connect(connection_string, pool_config) as conn:
        return await conn.execute_dict(sql)

def version() -> str:
    """Get the version of the mssql-python-rust library.
    
    Returns:
        Version string
    """
    return _core.version()

# Re-export core types for direct access if needed
RustConnection = _core.Connection  # Rename to avoid conflict with our main connect() function
PyRow = _core.Row
PyValue = _core.Value
PyExecutionResult = _core.ExecutionResult
PyQuery = _core.Query

# Export main API
__all__ = [
    'Connection',        # Main connection class
    'Query',            # Parameterized query class
    'Row', 
    'ExecutionResult',
    'PoolConfig',
    'connect',          # Convenience function
    'execute_async',
    'execute_dict_async',
    'version',          # Version function
    # Core types for advanced usage
    'RustConnection',
    'PyRow',
    'PyValue', 
    'PyExecutionResult',
    'PyQuery'
]