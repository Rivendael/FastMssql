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


class Parameter:
    """Represents a SQL parameter with value and optional type information."""
    
    def __init__(self, value: Any, sql_type: Optional[str] = None):
        """Initialize a parameter.
        
        Args:
            value: The parameter value (None, bool, int, float, str, bytes)
            sql_type: Optional SQL type hint (e.g., 'VARCHAR', 'INT', 'DATETIME')
        """
        self.value = value
        self.sql_type = sql_type
    
    def __repr__(self) -> str:
        if self.sql_type:
            return f"Parameter(value={self.value!r}, type={self.sql_type})"
        return f"Parameter(value={self.value!r})"


class Parameters:
    """Container for SQL parameters that can be passed to execute()."""
    
    def __init__(self, *args, **kwargs):
        """Initialize parameters container.
        
        Args:
            *args: Positional parameter values (for ? placeholders)
            **kwargs: Named parameter values (for @name placeholders, if supported)
        """
        self._positional = []
        self._named = {}
        
        # Handle positional parameters
        for arg in args:
            if isinstance(arg, Parameter):
                self._positional.append(arg)
            else:
                self._positional.append(Parameter(arg))
        
        # Handle named parameters
        for name, value in kwargs.items():
            if isinstance(value, Parameter):
                self._named[name] = value
            else:
                self._named[name] = Parameter(value)
    
    def add(self, value: Any, sql_type: Optional[str] = None) -> 'Parameters':
        """Add a positional parameter and return self for chaining.
        
        Args:
            value: Parameter value
            sql_type: Optional SQL type hint
            
        Returns:
            Self for method chaining
        """
        self._positional.append(Parameter(value, sql_type))
        return self
    
    def set(self, name: str, value: Any, sql_type: Optional[str] = None) -> 'Parameters':
        """Set a named parameter and return self for chaining.
        
        Args:
            name: Parameter name
            value: Parameter value
            sql_type: Optional SQL type hint
            
        Returns:
            Self for method chaining
        """
        self._named[name] = Parameter(value, sql_type)
        return self
    
    @property
    def positional(self) -> List[Parameter]:
        """Get positional parameters."""
        return self._positional.copy()
    
    @property
    def named(self) -> Dict[str, Parameter]:
        """Get named parameters."""
        return self._named.copy()
    
    def to_list(self) -> List[Any]:
        """Convert to simple list of values for compatibility."""
        return [param.value for param in self._positional]
    
    def __len__(self) -> int:
        """Get total number of parameters."""
        return len(self._positional) + len(self._named)
    
    def __repr__(self) -> str:
        parts = []
        if self._positional:
            parts.append(f"positional={len(self._positional)}")
        if self._named:
            parts.append(f"named={len(self._named)}")
        return f"Parameters({', '.join(parts)})"


class Connection:
    """Async connection to Microsoft SQL Server database with enhanced type support."""
    
    def __init__(
        self, 
        connection_string: Optional[str] = None, 
        pool_config: Optional[PoolConfig] = None,
        auto_connect: bool = False,
        server: Optional[str] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        trusted_connection: Optional[bool] = None
    ):
        """Initialize a new async connection.
        
        Args:
            connection_string: SQL Server connection string (if not using individual parameters)
            pool_config: Optional connection pool configuration
            auto_connect: If True, automatically connect on creation (not supported for async)
            server: Database server hostname or IP address
            database: Database name to connect to
            username: Username for SQL Server authentication
            password: Password for SQL Server authentication
            trusted_connection: Use Windows integrated authentication (default: True if no username provided)
            
        Note:
            Either connection_string OR server must be provided.
            If using individual parameters, server is required.
            If username is provided, password should also be provided for SQL authentication.
            If username is not provided, Windows integrated authentication will be used.
        """
        py_pool_config = pool_config._config if pool_config else None
        self._conn = _core.Connection(
            connection_string, 
            py_pool_config, 
            server, 
            database, 
            username, 
            password, 
            trusted_connection
        )
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
    
    async def execute(self, sql: str, parameters: Optional[Union[List[Any], Parameters]] = None) -> ExecutionResult:
        """Execute a query asynchronously and return enhanced results.
        
        Args:
            sql: SQL query to execute
            parameters: Optional parameters - can be:
                       - List of values for @P1 placeholders
                       - Parameters object for more control
            
        Returns:
            ExecutionResult object with rows or affected row count
            
        Examples:
            # Simple list of parameters
            result = await conn.execute("SELECT * FROM users WHERE age > @P1 AND name = @P2", [18, "John"])
            
            # Using Parameters object
            params = Parameters(18, "John")
            result = await conn.execute("SELECT * FROM users WHERE age > @P1 AND name = @P2", params)
            
            # Using Parameters with type hints
            params = Parameters().add(18, "INT").add("John", "VARCHAR")
            result = await conn.execute("SELECT * FROM users WHERE age > @P1 AND name = @P2", params)
        """
        if not self._connected:
            raise RuntimeError("Not connected to database. Call await conn.connect() first.")
        
        if parameters is None:
            py_result = await self._conn.execute(sql)
        elif isinstance(parameters, Parameters):
            # Convert Parameters object to list of values
            param_values = parameters.to_list()
            py_result = await self._conn.execute_with_python_params(sql, param_values)
        else:
            # Assume it's a list
            py_result = await self._conn.execute_with_python_params(sql, parameters)
        
        return ExecutionResult(py_result)
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()


def version() -> str:
    """Get the version of the mssql-python-rust library.
    
    Returns:
        Version string
    """
    return _core.version()

# Re-export core types for direct access if needed
RustConnection = _core.Connection  # Rename to avoid conflict with our main connect() function
RustQuery = _core.Query  # Rename to avoid conflict with our wrapper
PyRow = _core.Row
PyValue = _core.Value
PyExecutionResult = _core.ExecutionResult
PyQuery = _core.Query

# Export main API
__all__ = [
    'Connection',        # Main connection class
    'Parameter',        # Individual parameter with optional type
    'Parameters',       # Parameter container for execute()
    'Row', 
    'ExecutionResult',
    'PoolConfig',
    'version',          # Version function
    # Core types for advanced usage
    'RustConnection',
    'RustQuery',
    'PyRow',
    'PyValue', 
    'PyExecutionResult',
    'PyQuery'
]