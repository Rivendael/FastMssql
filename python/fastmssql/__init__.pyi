"""Type stubs for FastMSSQL wrapper classes."""

from typing import Any, Coroutine, Dict, List, Optional, Tuple, StrEnum
from .fastmssql import (
    AzureCredential,
    AzureCredentialType,
    ConversionError,
    EncryptionLevel,
    FastRow,
    Parameter,
    Parameters,
    PoolConfig,
    ProtocolError,
    QueryStream,
    SqlConnectionError,
    SqlError,
    SslConfig,
    TlsError,
    TypedNull,
)

class ApplicationIntent(StrEnum):
    """SQL Server application intent constants."""

    READ_ONLY: str
    READ_WRITE: str

class Connection:
    """
    High-performance SQL Server connection with async/await support.

    Thin wrapper around the Rust-backed connection that fixes async context manager behavior.
    Delegates all methods to the underlying Rust connection.

    Supports multiple initialization patterns:
    - Connection string: Connection("Server=localhost;Database=test")
    - Individual parameters: Connection(server="localhost", database="test")
    - SQL auth: Connection(server="host", username="user", password="pass")
    - Azure auth: Connection(server="host", azure_credential=azure_cred)

    Features:
    - Thread-safe connection pooling with configurable parameters
    - Async/await support for non-blocking I/O
    - SSL/TLS encryption support
    - Azure Active Directory authentication
    - Parameterized queries with automatic type conversion
    - Batch operations for high-performance bulk inserts and multiple queries
    - Connection pool statistics and monitoring
    """

    _conn: Any  # The underlying Rust connection

    def __init__(
        self,
        connection_string: Optional[str] = None,
        server: Optional[str] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        pool_config: Optional[PoolConfig] = None,
        ssl_config: Optional[SslConfig] = None,
        azure_credential: Optional[AzureCredential] = None,
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
            azure_credential: Azure Active Directory credential for authentication
            application_intent: Sets ApplicationIntent to "ReadOnly" or "ReadWrite" (default: ReadWrite)
            port: TCP port number (default: 1433)
            instance_name: Named instance of SQL Server
            application_name: Application name for SQL Server connection

        Note:
            - Either connection_string OR individual parameters must be provided
            - When using individual parameters, either username/password OR azure_credential must be provided
            - azure_credential and username/password are mutually exclusive
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
    ) -> Coroutine[Any, Any, QueryStream]:
        """
        Execute SELECT query that returns rows as an async stream.

        Returns a QueryStream for memory-efficient iteration over large result sets.

        Args:
            sql: SQL query with @P1, @P2, etc. placeholders for parameters
            params: List of parameter values in order
        Returns:
            QueryStream for iterating over result rows
        """
        ...

    def simple_query(
        self,
        sql: str,
    ) -> Coroutine[Any, Any, QueryStream]:
        """
        Execute a raw SQL query (non-prepared statement) that returns rows as an async stream.

        Only use this when required (creating stored procedures may require this in certain cases)

        Returns a QueryStream for memory-efficient iteration over large result sets.

        Args:
            sql: Raw SQL query
        Returns:
            QueryStream for iterating over result rows
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
    ) -> Coroutine[Any, Any, List[QueryStream]]:
        """
        Execute multiple SELECT queries in a single batch.

        Args:
            queries: List of (sql, params) tuples or just sql strings

        Returns:
            List of QueryStream objects for each query
        """
        ...

    def pool_stats(self) -> Coroutine[Any, Any, Dict[str, int | bool | None]]:
        """
        Get connection pool statistics.

        Returns a dictionary with the following keys:
        - connected (bool): Whether the pool is initialized and connected
        - connections (int): Total number of connections in the pool
        - idle_connections (int): Number of idle connections available
        - active_connections (int): Number of connections currently in use
        - max_size (int): Maximum pool size
        - min_idle (int | None): Minimum idle connections to maintain
        """
        ...

    async def __aenter__(self) -> Connection:
        """Async context manager entry (initializes pool)."""
        ...

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit (closes pool)."""
        ...

class Transaction:
    """
    Single dedicated connection for SQL Server transactions.

    Provides a non-pooled connection where all operations happen on the same
    underlying connection, ensuring transaction safety for BEGIN/COMMIT/ROLLBACK.

    Example:
        async with Transaction(server="localhost", database="mydb") as conn:
            await conn.execute("INSERT INTO ...")
    """

    _rust_conn: Any  # The underlying Rust transaction connection

    def __init__(
        self,
        connection_string: Optional[str] = None,
        ssl_config: Optional[SslConfig] = None,
        azure_credential: Optional[AzureCredential] = None,
        server: Optional[str] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        application_intent: Optional[ApplicationIntent | str] = None,
        port: Optional[int] = None,
        instance_name: Optional[str] = None,
        application_name: Optional[str] = None,
    ) -> None:
        """Initialize a dedicated non-pooled connection for transactions."""
        ...

    def query(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
    ) -> Coroutine[Any, Any, QueryStream]:
        """Execute a SELECT query that returns rows."""
        ...

    def execute(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
    ) -> Coroutine[Any, Any, int]:
        """Execute an INSERT/UPDATE/DELETE/DDL command."""
        ...

    def execute_batch(
        self,
        commands: List[Tuple[str, Optional[List[Any]]]],
    ) -> Coroutine[Any, Any, List[int]]:
        """Execute multiple commands in sequence on this connection."""
        ...

    def query_batch(
        self,
        queries: List[Tuple[str, Optional[List[Any]]]],
    ) -> Coroutine[Any, Any, List[QueryStream]]:
        """Execute multiple SELECT queries in sequence on this connection."""
        ...

    def simple_query(
        self,
        sql: str,
    ) -> Coroutine[Any, Any, QueryStream]:
        """
        Execute a raw (non-prepared) SQL query and return a QueryStream.

        Only use this when required (creating stored procedures may require this in certain cases)
        """
        ...

    def is_connected(self) -> bool:
        """Return True if the underlying connection is currently established."""
        ...

    async def begin(self) -> None:
        """Begin a transaction."""
        ...

    async def commit(self) -> None:
        """Commit the current transaction."""
        ...

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        ...

    async def close(self) -> None:
        """Close the connection."""
        ...

    async def __aenter__(self) -> Transaction:
        """Async context manager entry - automatically BEGIN transaction."""
        ...

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit - automatically COMMIT or ROLLBACK."""
        ...

__all__ = [
    "ApplicationIntent",
    "AzureCredential",
    "AzureCredentialType",
    "ConversionError",
    "Connection",
    "EncryptionLevel",
    "FastRow",
    "Parameter",
    "Parameters",
    "PoolConfig",
    "ProtocolError",
    "QueryStream",
    "SqlConnectionError",
    "SqlError",
    "SslConfig",
    "TlsError",
    "Transaction",
    "TypedNull",
    "version",
]

def version() -> str: ...
