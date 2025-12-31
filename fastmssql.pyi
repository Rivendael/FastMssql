"""
FastMSSQL - High-Performance Microsoft SQL Server Driver for Python

High-performance Rust-backed Python driver for SQL Server with:
- Async/await support for non-blocking operations
- Connection pooling with configurable parameters
- SSL/TLS encryption support
- Parameterized queries with automatic type conversion
- Memory-efficient result handling
"""

from typing import Any, List, Dict, Optional, Tuple, Coroutine
try:
    from enum import StrEnum
except ImportError:
    # Python 3.10 compatibility: StrEnum was added in Python 3.11
    from enum import Enum
    class StrEnum(str, Enum):
        pass

class PoolConfig:
    """
    Configuration for connection pool behavior.
    
    Attributes:
        max_size: Maximum number of connections in the pool (default: 10)
        min_idle: Minimum number of idle connections to maintain (default: 2)
        max_lifetime_secs: Maximum lifetime of a connection in seconds (default: None = unlimited)
        idle_timeout_secs: Timeout for idle connections in seconds (default: None = no timeout)
        connection_timeout_secs: Timeout for acquiring a connection in seconds (default: 30)
        test_on_check_out: Whether to test connections when checking out (default: None)
        retry_connection: Whether to retry connection attempts (default: None)
    """

    max_size: int
    min_idle: Optional[int]
    max_lifetime_secs: Optional[int]
    idle_timeout_secs: Optional[int]
    connection_timeout_secs: Optional[int]
    test_on_check_out: Optional[bool]
    retry_connection: Optional[bool]

    def __init__(
        self,
        max_size: int = 10,
        min_idle: int = 2,
        max_lifetime_secs: Optional[int] = None,
        idle_timeout_secs: Optional[int] = None,
        connection_timeout_secs: int = 30,
        test_on_check_out: Optional[bool] = None,
        retry_connection: Optional[bool] = None,
    ) -> None: ...

    @staticmethod
    def one() -> PoolConfig:
        """Pre-configured pool for single-connection scenarios (max_size=1, min_idle=1)."""
        ...
    
    @staticmethod
    def high_throughput() -> PoolConfig:
        """Pre-configured pool for high-throughput scenarios (max_size=50, min_idle=15)."""
        ...
    
    @staticmethod
    def low_resource() -> PoolConfig:
        """Pre-configured pool for resource-constrained environments (max_size=3, min_idle=1)."""
        ...
    
    @staticmethod
    def development() -> PoolConfig:
        """Pre-configured pool for development (max_size=5, min_idle=1)."""
        ...
    
    @staticmethod
    def performance() -> PoolConfig:
        """Pre-configured pool for maximum performance (max_size=100, min_idle=30)."""
        ...

class EncryptionLevel(StrEnum):
    """SQL Server encryption level constants."""
    Disabled: str
    """No encryption."""
    LoginOnly: str
    """Encrypt only during login."""
    Required: str
    """Full encryption required."""

class ApplicationIntent(StrEnum):
    """SQL Server application intent constants."""
    READ_ONLY: str
    """Read-only workload."""
    READ_WRITE: str
    """Read-write workload."""

class SslConfig:
    """
    Configuration for SSL/TLS encrypted connections.
    
    Attributes:
        encryption_level: Level of encryption (Disabled, LoginOnly, or Required)
        trust_server_certificate: Whether to trust the server certificate without validation
        ca_certificate_path: Path to CA certificate file for certificate validation
        enable_sni: Enable Server Name Indication (SNI) for TLS handshake (default: True)
        server_name: Custom server name for certificate validation (optional)
    """
    encryption_level: str
    trust_server_certificate: bool
    ca_certificate_path: Optional[str]
    enable_sni: bool
    server_name: Optional[str]
    
    def __init__(
        self,
        encryption_level: str = "Required",
        trust_server_certificate: bool = False,
        ca_certificate_path: Optional[str] = None,
        enable_sni: bool = True,
        server_name: Optional[str] = None,
    ) -> None: ...
    
    @staticmethod
    def development() -> SslConfig:
        """Development configuration (LoginOnly encryption, trust server certificate)."""
        ...
    
    @staticmethod
    def login_only() -> SslConfig:
        """LoginOnly encryption configuration."""
        ...
    
    @staticmethod
    def disabled() -> SslConfig:
        """No encryption configuration."""
        ...
    
    @staticmethod
    def with_ca_certificate(path: str) -> SslConfig:
        """Create config with CA certificate validation from file path."""
        ...

class FastRow:
    """
    Represents a single row from a query result with optimized column access.
    
    Provides zero-copy access to row data with both dictionary-like and index-based access patterns.
    """
    
    def __getitem__(self, key: str | int) -> Any:
        """Access column value by name (string) or index (int)."""
        ...
    
    def columns(self) -> List[str]:
        """Get list of all column names in this row."""
        ...
    
    def __len__(self) -> int:
        """Get number of columns in this row."""
        ...
    
    def get(self, column: str) -> Any:
        """Get column value by name."""
        ...
    
    def get_by_index(self, index: int) -> Any:
        """Get column value by index."""
        ...
    
    def values(self) -> List[Any]:
        """Get all column values as a list in column order."""
        ...
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert row to dictionary mapping column names to values."""
        ...

class FastExecutionResult:
    """
    Result object for query execution containing rows or affected count.
    
    Provides both immediate and streaming access to result rows, with support for
    affected row counts from INSERT/UPDATE/DELETE operations.
    """
    
    def rows(self) -> List[FastRow] | None:
        """Get all rows if this is a SELECT result, or None if this is an update result."""
        ...
    
    def affected_rows(self) -> int | None:
        """Get number of affected rows for INSERT/UPDATE/DELETE operations, or None for SELECT."""
        ...
    
    def has_rows(self) -> bool:
        """Check if this result contains any rows."""
        ...
    
    def has_affected_count(self) -> bool:
        """Check if this result contains an affected row count."""
        ...
    
    def row_count(self) -> int:
        """Get the number of rows in this result set."""
        ...
    
    def fetchone(self) -> FastRow | None:
        """Fetch the next row from the result set, or None if no more rows."""
        ...
    
    def fetchmany(self, size: int = 1) -> List[FastRow]:
        """Fetch up to `size` rows from the result set."""
        ...
    
    def fetchall(self) -> List[FastRow]:
        """Fetch all remaining rows from the result set."""
        ...

class Parameter:
    """
    Parameter object for SQL queries with optional type hints.
    
    Use in parameter lists for parameterized queries. Parameters can specify explicit SQL types
    for automatic conversion and validation.
    
    Attributes:
        value: The parameter value (any Python type that can be converted to SQL)
        sql_type: Optional SQL Server type name (e.g., 'INT', 'VARCHAR', 'DATETIME2')
        is_expanded: Whether this parameter is an iterable for IN clause expansion
    """
    
    value: Any
    sql_type: Optional[str]
    is_expanded: bool
    
    def __init__(
        self,
        value: Any,
        sql_type: Optional[str] = None,
    ) -> None:
        """
        Create a new parameter with optional type specification.
        
        Args:
            value: The parameter value
            sql_type: Optional SQL Server type name for explicit type conversion
        """
        ...

class Parameters:
    """
    Collection of parameters for SQL queries with positional and named support.
    
    Supports both positional parameters (@P1, @P2, etc.) and named parameters (@name, @id, etc.).
    Can be constructed with positional and keyword arguments, with optional type specifications.
    
    Attributes:
        *args: List of Parameter objects in positional order
        **kwargs: Dictionary of named Parameter objects
    """
    
    positional: List[Parameter]
    named: Dict[str, Parameter]
    
    def __init__(
        self,
        *args: Any | Parameter,
        **kwargs: Any | Parameter,
    ) -> None:
        """
        Create a new Parameters collection.
        
        Args:
            *args: Positional parameters (raw values or Parameter objects)
            **kwargs: Named parameters (raw values or Parameter objects with keys as names)
        """
        ...
    
    def add(
        self,
        value: Any,
        sql_type: Optional[str] = None,
    ) -> Parameters:
        """
        Add a positional parameter and return self for chaining.
        
        Args:
            value: The parameter value
            sql_type: Optional SQL Server type name
            
        Returns:
            Self for method chaining
        """
        ...
    
    def set(
        self,
        key: str,
        value: Any,
        sql_type: Optional[str] = None,
    ) -> Parameters:
        """
        Add or update a named parameter and return self for chaining.
        
        Args:
            key: Parameter name
            value: The parameter value
            sql_type: Optional SQL Server type name
            
        Returns:
            Self for method chaining
        """
        ...
    
    def to_list(self) -> List[Any]:
        """Convert positional parameters to a list of values."""
        ...
    
    def __len__(self) -> int:
        """Get total number of parameters (positional + named)."""
        ...
    
    def __repr__(self) -> str:
        """Get string representation of parameters."""
        ...

class Connection:
    """
    High-performance SQL Server connection with async/await support.
    
    Supports multiple initialization patterns:
    - Connection string: Connection("Server=localhost;Database=test")
    - Individual parameters: Connection(server="localhost", database="test")
    - SQL auth: Connection(server="host", username="user", password="pass")
    
    Features:
    - Thread-safe connection pooling with configurable parameters
    - Async/await support for non-blocking I/O
    - SSL/TLS encryption support
    - Parameterized queries with automatic type conversion
    - Batch operations for high-performance bulk inserts and multiple queries
    - Connection pool statistics and monitoring
    
    Example:
        async with Connection("Server=localhost;Database=mydb") as conn:
            result = await conn.query("SELECT * FROM users WHERE id = @P1", [123])
            for row in result.rows():
                print(row['name'])
    """
    def __init__(
        self,
        connection_string: Optional[str] = None,
        server: Optional[str] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        pool_config: Optional[PoolConfig] = None,
        ssl_config: Optional[SslConfig] = None,
        application_intent: Optional[ApplicationIntent | str] = None,
        port: Optional[int] = None,
        instance_name: Optional[str] = None,
        application_name: Optional[str] = None,
    ) -> None:
        """
        Initialize a new SQL Server connection.
        
        Args:
            connection_string: Complete ADO.NET-style connection string (takes precedence)
            server: SQL Server hostname or IP address
            database: Database name
            username: Username for SQL authentication (required when using individual parameters)
            password: Password for SQL authentication
            pool_config: Connection pool configuration
            ssl_config: SSL/TLS configuration
            application_intent: Sets ApplicationIntent to "ReadOnly" or "ReadWrite" (default: ReadWrite)
            port: TCP port number (default: 1433)
            instance_name: Named instance of SQL Server
            application_name: Application name for SQL Server connection
        """
        ...
    
    def connect(self) -> Coroutine[Any, Any, bool]:
        """Explicitly initialize the connection pool."""
        ...
    
    def disconnect(self) -> Coroutine[Any, Any, bool]:
        """Explicitly close the connection pool and all connections."""
        ...
    
    def is_connected(self) -> Coroutine[Any, Any, bool]:
        """Check if the connection pool is active and ready."""
        ...
    
    def query(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
    ) -> Coroutine[Any, Any, FastExecutionResult]:
        """
        Execute SELECT query that returns rows.
        
        Args:
            sql: SQL query with @P1, @P2, etc. placeholders for parameters
            params: List of parameter values in order
            
        Returns:
            FastExecutionResult containing the query rows
        """
        ...
    
    def execute(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
    ) -> Coroutine[Any, Any, int]:
        """
        Execute INSERT/UPDATE/DELETE/DDL command.
        
        Args:
            sql: SQL command with @P1, @P2, etc. placeholders
            params: List of parameter values in order
            
        Returns:
            Number of affected rows
        """
        ...
    
    def execute_batch(
        self,
        commands: List[Tuple[str, Optional[List[Any]]]],
    ) -> Coroutine[Any, Any, List[int]]:
        """
        Execute multiple commands in a single batch for better performance.
        
        Args:
            commands: List of (sql, params) tuples
            
        Returns:
            List of affected row counts for each command
        """
        ...
    
    def bulk_insert(
        self,
        table: str,
        columns: List[str],
        data: List[List[Any]],
    ) -> Coroutine[Any, Any, None]:
        """
        High-performance bulk insert for large datasets.
        
        Args:
            table: Target table name (can be schema-qualified)
            columns: List of column names
            data: List of rows, each row is a list of values
        """
        ...
    
    def query_batch(
        self,
        queries: List[str] | List[Tuple[str, Optional[List[Any]]]],
    ) -> Coroutine[Any, Any, List[FastExecutionResult]]:
        """
        Execute multiple SELECT queries in a single batch.
        
        Args:
            queries: List of (sql, params) tuples or just sql strings
            
        Returns:
            List of FastExecutionResult objects for each query
        """
        ...
    
    def pool_stats(self) -> Coroutine[Any, Any, Dict[str, int | bool]]:
        """
        Get connection pool statistics.
        
        Returns:
            Dictionary with keys: connections, idle_connections, active_connections, max_size, min_idle
            Or {'connected': False} if pool is not initialized
        """
        ...
    
    async def __aenter__(self) -> Connection:
        """Async context manager entry (initializes pool)."""
        ...
    
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit (closes pool)."""
        ...

    def version() -> str:
        """Get the fastmssql library version."""
        ...
def version() -> str: ...