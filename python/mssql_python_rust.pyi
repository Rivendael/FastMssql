"""
Type stubs for mssql_python_rust

This file provides type information for the compiled Rust module.
"""

from typing import Any, Dict, List, Optional, Union

class PyConnection:
    """Connection to Microsoft SQL Server database."""
    
    def __init__(self, connection_string: str, pool_config: Optional[PyPoolConfig] = None) -> None:
        """Initialize a new connection.
        
        Args:
            connection_string: SQL Server connection string
            pool_config: Optional pool configuration
        """
        ...
    
    async def connect(self) -> None:
        """Connect to the database asynchronously."""
        ...
    
    async def disconnect(self) -> None:
        """Disconnect from the database asynchronously."""
        ...
    
    async def is_connected(self) -> bool:
        """Check if connected to the database."""
        ...
    
    async def execute(self, sql: str) -> PyExecutionResult:
        """Execute a SQL query asynchronously.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            Execution result containing rows or affected row count
        """
        ...
    
    async def __aenter__(self) -> "PyConnection":
        """Async context manager entry."""
        ...
    
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        ...

class PyRow:
    """Represents a row from a SQL query result."""
    
    def get(self, column: Union[str, int]) -> Any:
        """Get value by column name or index.
        
        Args:
            column: Column name or index
            
        Returns:
            Column value
        """
        ...
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert row to dictionary.
        
        Returns:
            Dictionary mapping column names to values
        """
        ...
    
    def to_tuple(self) -> tuple:
        """Convert row to tuple.
        
        Returns:
            Tuple of column values
        """
        ...
    
    def __getitem__(self, key: Union[str, int]) -> Any:
        """Get value by column name or index."""
        ...
    
    def __len__(self) -> int:
        """Get number of columns."""
        ...

class PyValue:
    """Represents a SQL value with type information."""
    
    @property
    def value(self) -> Any:
        """The actual value."""
        ...
    
    @property
    def sql_type(self) -> str:
        """SQL data type name."""
        ...
    
    def is_null(self) -> bool:
        """Check if value is NULL."""
        ...

class PyExecutionResult:
    """Result of SQL query execution."""
    
    @property
    def rows(self) -> List[PyRow]:
        """Query result rows (for SELECT queries)."""
        ...
    
    @property
    def affected_rows(self) -> Optional[int]:
        """Number of affected rows (for INSERT/UPDATE/DELETE)."""
        ...
    
    def has_rows(self) -> bool:
        """Check if result contains rows."""
        ...

class PyPoolConfig:
    """Connection pool configuration."""
    
    def __init__(
        self,
        max_size: int = 10,
        min_idle: Optional[int] = None,
        max_lifetime_secs: Optional[int] = None,
        idle_timeout_secs: Optional[int] = None,
        connection_timeout_secs: Optional[int] = None
    ) -> None:
        """Initialize pool configuration.
        
        Args:
            max_size: Maximum number of connections in pool
            min_idle: Minimum number of idle connections
            max_lifetime_secs: Maximum lifetime of connections in seconds
            idle_timeout_secs: Idle timeout in seconds
            connection_timeout_secs: Connection timeout in seconds
        """
        ...
    
    @property
    def max_size(self) -> int: ...
    @property
    def min_idle(self) -> Optional[int]: ...
    @property
    def max_lifetime_secs(self) -> Optional[int]: ...
    @property
    def idle_timeout_secs(self) -> Optional[int]: ...
    @property
    def connection_timeout_secs(self) -> Optional[int]: ...
    
    @staticmethod
    def high_throughput() -> "PyPoolConfig": ...
    @staticmethod
    def low_resource() -> "PyPoolConfig": ...
    @staticmethod
    def development() -> "PyPoolConfig": ...

def connect(connection_string: str, pool_config: Optional[PyPoolConfig] = None) -> PyConnection:
    """Create a new database connection.
    
    Args:
        connection_string: SQL Server connection string
        pool_config: Optional pool configuration
        
    Returns:
        New connection instance
    """
    ...

def version() -> str:
    """Get library version.
    
    Returns:
        Version string
    """
    ...
