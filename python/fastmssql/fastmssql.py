"""
High-level Python API for mssql-python-rust

This module provides convenient Python functions that wrap the Rust core functionality.
Supports asynchronous operations only.
"""

from typing import List, Dict, Any, Optional, Union, Iterable

try:
    # Try to import the compiled Rust module from the package
    from . import fastmssql_core as _core
except ImportError:
    # Fallback for development - try absolute import
    try:
        import fastmssql_core as _core
    except ImportError as e:
        import sys
        print(f"ERROR: fastmssql_core module not found: {e}")
        print("Solution: Build the extension with 'maturin develop' or 'maturin develop --release'")
        print("Make sure you're in the project root directory and have Rust/Maturin installed.")
        sys.exit(1)

class Row:
    """Python wrapper around Row for better type hints and documentation."""
    
    def __init__(self, py_row: Any) -> None:
        """Initialize with a Row instance.
        
        Args:
            py_row: The underlying Rust Row instance
        """
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
    
    # Valid SQL parameter types (base types without parameters)
    VALID_SQL_TYPES = {
        'VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR', 'TEXT', 'NTEXT',
        'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'BIT',
        'FLOAT', 'REAL', 'DECIMAL', 'NUMERIC', 'MONEY', 'SMALLMONEY',
        'DATETIME', 'DATETIME2', 'SMALLDATETIME', 'DATE', 'TIME', 'DATETIMEOFFSET',
        'BINARY', 'VARBINARY', 'IMAGE',
        'UNIQUEIDENTIFIER', 'XML', 'JSON'
    }
    
    @staticmethod
    def _extract_base_type(sql_type: str) -> str:
        """Extract the base SQL type from a type specification.
        
        Examples:
            VARCHAR(50) -> VARCHAR
            NVARCHAR(MAX) -> NVARCHAR
            DECIMAL(10,2) -> DECIMAL
            INT -> INT
        """
        # Find the first opening parenthesis and extract everything before it
        paren_pos = sql_type.find('(')
        if paren_pos != -1:
            return sql_type[:paren_pos].strip().upper()
        return sql_type.strip().upper()
    
    def __init__(self, value: Any, sql_type: Optional[str] = None) -> None:
        """Initialize a parameter.
        
        Args:
            value: The parameter value (None, bool, int, float, str, bytes, or iterable for IN clauses)
            sql_type: Optional SQL type hint. Can include parameters:
                     - 'VARCHAR(50)', 'NVARCHAR(MAX)', 'DECIMAL(10,2)', etc.
                     - Base types: 'VARCHAR', 'INT', 'DATETIME', etc.
            
        Raises:
            ValueError: If sql_type is provided but the base type is not recognized
            
        Note:
            Lists, tuples, sets and other iterables (except strings/bytes) are automatically
            expanded for use in IN clauses. So Parameter([1, 2, 3]) will expand to 
            placeholder values for "WHERE id IN (@P1, @P2, @P3)".
        """
        # Automatically detect iterables for IN clause expansion
        if self._is_iterable_value(value):
            self.value = list(value)  # Convert to list for consistency
            self.is_expanded = True
        else:
            self.value = value
            self.is_expanded = False
        
        if sql_type is not None:
            # Extract base type and validate it
            base_type = self._extract_base_type(sql_type)
            if base_type not in self.VALID_SQL_TYPES:
                raise ValueError(
                    f"Invalid sql_type '{sql_type}'. Base type '{base_type}' not recognized. "
                    f"Valid base types: {', '.join(sorted(self.VALID_SQL_TYPES))}"
                )
            # Store the original type specification (including parameters)
            self.sql_type = sql_type.upper()
        else:
            self.sql_type = None
    
    @staticmethod
    def _is_iterable_value(value: Any) -> bool:
        """Check if a value is an iterable that can be expanded for IN clauses.
        
        Returns True for lists, tuples, sets, etc., but False for strings and bytes
        which should be treated as single values.
        """
        return (
            hasattr(value, '__iter__') and 
            not isinstance(value, (str, bytes))
        )
    
    def __repr__(self) -> str:
        if self.is_expanded:
            type_info = f", type={self.sql_type}" if self.sql_type else ""
            return f"Parameter(IN_values={self.value!r}{type_info})"
        elif self.sql_type:
            return f"Parameter(value={self.value!r}, type={self.sql_type})"
        return f"Parameter(value={self.value!r})"


class Parameters:
    """Container for SQL parameters that can be passed to execute()."""
    
    def __init__(self, *args, **kwargs):
        """Initialize parameters container.
        
        Args:
            *args: Positional parameter values. Can be individual values or iterables.
                  Iterables (lists, tuples, etc.) will be expanded by Rust for performance.
                  Strings and bytes are treated as single values, not expanded.
            **kwargs: Named parameter values (for @name placeholders, if supported)
        
        Examples:
            # Individual parameters
            params = Parameters(1, "hello", 3.14)
            
            # Mix of individual and iterable parameters (expansion handled in Rust)
            params = Parameters(1, [2, 3, 4], "hello")  # Rust expands [2,3,4] automatically
            
            # All types of iterables work
            params = Parameters([1, 2], (3, 4), {5, 6})  # All expanded by Rust
        """
        self._positional = []
        self._named = {}
        
        # Handle positional parameters - let Rust handle expansion
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
            value: Parameter value (can be an iterable for automatic expansion by Rust)
            sql_type: Optional SQL type hint
            
        Returns:
            Self for method chaining
            
        Examples:
            params = Parameters().add(42).add("hello")
            params = Parameters().add([1, 2, 3])  # Rust expands automatically
        """
        self._positional.append(Parameter(value, sql_type))
        return self
    
    def extend(self, other: Union['Parameters', Iterable[Any]]) -> 'Parameters':
        """Extend parameters with another Parameters object or iterable.
        
        Args:
            other: Another Parameters object or an iterable of values
            
        Returns:
            Self for method chaining
            
        Examples:
            params1 = Parameters(1, 2)
            params2 = Parameters(3, 4)
            params1.extend(params2)  # params1 now has [1, 2, 3, 4]
            
            params = Parameters(1, 2)
            params.extend([3, 4, 5])  # params now has [1, 2, [3, 4, 5]] - Rust handles expansion
        """
        if isinstance(other, Parameters):
            self._positional.extend(other._positional)
            self._named.update(other._named)
        else:
            # Add as single parameter - Rust will expand if it's an iterable
            self._positional.append(Parameter(other))
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
        """Convert to simple list of values for compatibility.
        
        Note: Iterable expansion is now handled by Rust for performance,
        so this returns the raw values as-is.
        """
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


class SslConfig:
    """SSL/TLS configuration for database connections."""
    
    def __init__(
        self,
        encryption_level: Optional[str] = None,
        trust_server_certificate: bool = False,
        ca_certificate_path: Optional[str] = None,
        enable_sni: bool = True,
        server_name: Optional[str] = None
    ):
        """Initialize SSL configuration.
        
        Args:
            encryption_level: Encryption level - "Required", "LoginOnly", or "Off" (default: "Required")
            trust_server_certificate: Trust server certificate without validation (dangerous in production)
            ca_certificate_path: Path to custom CA certificate file (.pem, .crt, or .der)
            enable_sni: Enable Server Name Indication (default: True)
            server_name: Custom server name for certificate validation
            
        Note:
            trust_server_certificate and ca_certificate_path are mutually exclusive.
            For production, either use a valid certificate in the system trust store,
            or provide a ca_certificate_path to a trusted CA certificate.
        """
        # Set default encryption level
        if encryption_level is None:
            encryption_level = "Required"
        
        # Validate encryption level
        valid_levels = ["Required", "LoginOnly", "Off"]
        if encryption_level not in valid_levels:
            raise ValueError(f"Invalid encryption_level. Must be one of: {valid_levels}")
        
        # Map string encryption levels to enum values
        encryption_level_map = {
            "Required": _core.EncryptionLevel.REQUIRED,
            "LoginOnly": _core.EncryptionLevel.LOGIN_ONLY,
            "Off": _core.EncryptionLevel.OFF
        }
        
        self._config = _core.SslConfig(
            encryption_level=encryption_level_map[encryption_level],
            trust_server_certificate=trust_server_certificate,
            ca_certificate_path=ca_certificate_path,
            enable_sni=enable_sni,
            server_name=server_name
        )
    
    @staticmethod
    def development() -> 'SslConfig':
        """Create SSL config for development (trusts all certificates).
        
        Warning: This configuration is insecure and should only be used in development.
        """
        config = SslConfig.__new__(SslConfig)
        config._config = _core.SslConfig.development()
        return config
    
    @staticmethod
    def with_ca_certificate(ca_cert_path: str) -> 'SslConfig':
        """Create SSL config for production with custom CA certificate.
        
        Args:
            ca_cert_path: Path to CA certificate file (.pem, .crt, or .der)
        """
        config = SslConfig.__new__(SslConfig)
        config._config = _core.SslConfig.with_ca_certificate(ca_cert_path)
        return config
    
    @staticmethod
    def login_only() -> 'SslConfig':
        """Create SSL config that only encrypts login (legacy mode)."""
        config = SslConfig.__new__(SslConfig)
        config._config = _core.SslConfig.login_only()
        return config
    
    @staticmethod
    def disabled() -> 'SslConfig':
        """Create SSL config with no encryption (not recommended)."""
        config = SslConfig.__new__(SslConfig)
        config._config = _core.SslConfig.disabled()
        return config
    
    @property
    def encryption_level(self) -> str:
        """Get the encryption level."""
        return str(self._config.encryption_level)
    
    @property
    def trust_server_certificate(self) -> bool:
        """Get trust server certificate setting."""
        return self._config.trust_server_certificate
    
    @property
    def ca_certificate_path(self) -> Optional[str]:
        """Get CA certificate path."""
        return self._config.ca_certificate_path
    
    @property
    def enable_sni(self) -> bool:
        """Get SNI setting."""
        return self._config.enable_sni
    
    @property
    def server_name(self) -> Optional[str]:
        """Get custom server name."""
        return self._config.server_name
    
    def __repr__(self) -> str:
        return f"SslConfig(encryption={self.encryption_level}, trust_cert={self.trust_server_certificate})"


class Connection:
    """Async connection to Microsoft SQL Server database with enhanced type support."""
    
    def __init__(
        self, 
        connection_string: Optional[str] = None, 
        pool_config: Optional[PoolConfig] = None,
        ssl_config: Optional['SslConfig'] = None,
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
            ssl_config: Optional SSL/TLS configuration
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
        py_ssl_config = ssl_config._config if ssl_config else None
        self._conn = _core.Connection(
            connection_string, 
            py_pool_config, 
            py_ssl_config,
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
    
    async def execute(self, sql: str, parameters: Optional[Union[List[Any], Parameters, Iterable[Any]]] = None) -> ExecutionResult:
        """Execute a query asynchronously and return enhanced results.
        
        Args:
            sql: SQL query to execute (must be non-empty)
            parameters: Optional parameters - can be:
                       - List of values for @P1 placeholders
                       - Parameters object for more control
                       - Any iterable of values (tuple, set, generator, etc.)
            
        Returns:
            ExecutionResult object with rows or affected row count
            
        Raises:
            RuntimeError: If not connected to database
            ValueError: If sql is empty or None
            
        Examples:
            # Simple list of parameters
            result = await conn.execute("SELECT * FROM users WHERE age > @P1 AND name = @P2", [18, "John"])
            
            # Using tuple
            result = await conn.execute("SELECT * FROM users WHERE age > @P1 AND name = @P2", (18, "John"))
            
            # Automatic IN clause expansion (handled by Rust for performance)
            result = await conn.execute("SELECT * FROM users WHERE id IN (@P1)", [[1, 2, 3, 4]])
            # Rust automatically expands to: WHERE id IN (@P1, @P2, @P3, @P4)
            
            # Using Parameters with automatic IN clause expansion
            params = Parameters([1, 2, 3, 4], "John")
            result = await conn.execute("SELECT * FROM users WHERE id IN (@P1) AND name = @P2", params)
            # Rust expands the list automatically
            
            # Using Parameters with type hints
            params = Parameters(Parameter([1, 2, 3, 4], "INT"), "John")
            result = await conn.execute("SELECT * FROM users WHERE id IN (@P1) AND name = @P2", params)
            
            # Method chaining with iterables
            params = Parameters().add(18, "INT").add(["admin", "user"])
            result = await conn.execute("SELECT * FROM users WHERE age > @P1 AND role IN (@P2)", params)
        """
        if not sql or not sql.strip():
            raise ValueError("SQL query cannot be empty or None")
            
        if not self._connected:
            raise RuntimeError("Not connected to database. Call await conn.connect() first.")
        
        if parameters is None:
            py_result = await self._conn.execute(sql)
        elif isinstance(parameters, Parameters):
            # Convert Parameters object to list of values
            param_values = parameters.to_list()
            py_result = await self._conn.execute_with_python_params(sql, param_values)
        elif hasattr(parameters, '__iter__') and not isinstance(parameters, (str, bytes)):
            # Handle any iterable (list, tuple, set, generator, etc.)
            # Convert to list to ensure we can pass it to the Rust layer
            param_values = list(parameters)
            py_result = await self._conn.execute_with_python_params(sql, param_values)
        else:
            # Single value - wrap in list
            py_result = await self._conn.execute_with_python_params(sql, [parameters])
        
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
    'Connection',       # Main connection class
    'Parameter',        # Individual parameter with optional type
    'Parameters',       # Parameter container for execute()
    'Row', 
    'ExecutionResult',
    'PoolConfig',
    'SslConfig',        # SSL/TLS configuration
    'version',          # Version function
    # Core types for advanced usage
    'RustConnection',
    'RustQuery',
    'PyRow',
    'PyValue', 
    'PyExecutionResult',
    'PyQuery'
]