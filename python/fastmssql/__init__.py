"""
FastMSSQL - High-Performance Microsoft SQL Server Driver for Python
===================================================================

This library provides direct access to high-performance Rust implementations
with minimal Python overhead for maximum performance. Built on top of the
tiberius crate, it offers both synchronous and asynchronous database operations
with advanced features like connection pooling, SSL/TLS configuration, and
efficient parameter handling.

Key Features:
    - High-performance Rust backend with Python bindings
    - Async/await support for non-blocking operations
    - Connection pooling with configurable parameters
    - SSL/TLS encryption with certificate validation
    - Parameterized queries with automatic type conversion
    - Memory-efficient result iteration
    - Comprehensive error handling and logging

Basic Usage (Async):
    >>> import asyncio
    >>> from fastmssql import Connection
    >>> 
    >>> async def main():
    ...     async with Connection("Server=localhost;Database=test;Trusted_Connection=yes") as conn:
    ...         # SELECT queries - use query() method
    ...         result = await conn.query("SELECT * FROM users")
    ...         async for row in result:
    ...             print(f"User: {row['name']}, Age: {row['age']}")
    ...         
    ...         # Parameterized SELECT query
    ...         result = await conn.query(
    ...             "SELECT * FROM users WHERE age > @P1 AND city = @P2", 
    ...             [18, "New York"]
    ...         )
    ...         rows = await result.fetchall()
    ...         print(f"Found {len(rows)} users")
    ...         
    ...         # INSERT/UPDATE/DELETE - use execute() method
    ...         affected = await conn.execute(
    ...             "INSERT INTO users (name, email, age) VALUES (@P1, @P2, @P3)",
    ...             ["John Doe", "john@example.com", 30]
    ...         )
    ...         print(f"Inserted {affected} rows")
    >>> 
    >>> asyncio.run(main())

Basic Usage (Sync):
    >>> from fastmssql import Connection
    >>> 
    >>> with Connection("Server=localhost;Database=test;Trusted_Connection=yes") as conn:
    ...     # For SELECT queries, you would typically use the async API
    ...     # Sync usage is primarily for simple operations
    ...     pass  # Connection established and will be closed on exit

Advanced Configuration:
    >>> from fastmssql import Connection, PoolConfig, SslConfig, EncryptionLevel
    >>> 
    >>> # Configure connection pool
    >>> pool_config = PoolConfig(
    ...     max_connections=20,
    ...     min_connections=2,
    ...     acquire_timeout_seconds=30,
    ...     idle_timeout_seconds=600
    ... )
    >>> 
    >>> # Configure SSL/TLS
    >>> ssl_config = SslConfig(
    ...     encryption_level=EncryptionLevel.Required,
    ...     trust_server_certificate=False,
    ...     certificate_path="/path/to/cert.pem"
    ... )
    >>> 
    >>> conn = Connection(
    ...     server="myserver.database.windows.net",
    ...     database="mydatabase",
    ...     username="myuser",
    ...     password="mypassword",
    ...     pool_config=pool_config,
    ...     ssl_config=ssl_config
    ... )

Performance Considerations:
    - Use parameterized queries to prevent SQL injection and improve performance
    - Leverage connection pooling for applications with multiple concurrent operations
    - Use async methods for I/O-bound applications to improve throughput
    - Consider batch operations for bulk data manipulation
    - Monitor connection pool statistics to optimize pool configuration

Thread Safety:
    This library is thread-safe and can be used in multi-threaded applications.
    Each Connection instance maintains its own connection pool and can be safely
    shared across threads when using async methods.
"""

# Import from the maturin-generated module
from .fastmssql import Connection as _RustConnection
from .fastmssql import PoolConfig
from .fastmssql import SslConfig  
from .fastmssql import FastExecutionResult
from .fastmssql import version, EncryptionLevel, Parameter, Parameters

# Wrapper class to handle async execution result conversion
class Connection:
    """
    High-performance connection to Microsoft SQL Server.
    
    This class provides a Python wrapper around the Rust-based connection implementation,
    offering both synchronous and asynchronous database operations with advanced features
    like connection pooling, SSL/TLS configuration, and efficient parameter handling.
    
    The Connection class supports multiple initialization patterns:
    1. Connection string-based initialization
    2. Individual parameter initialization 
    3. Advanced configuration with pool and SSL settings
    
    Connection Patterns:
        # Using connection string
        conn = Connection("Server=localhost;Database=test;Trusted_Connection=yes")
        
        # Using individual parameters
        conn = Connection(
            server="localhost", 
            database="test", 
            trusted_connection=True
        )
        
        # Using username/password authentication
        conn = Connection(
            server="myserver.database.windows.net",
            database="mydatabase", 
            username="myuser",
            password="mypassword"
        )
    
    Thread Safety:
        This class is thread-safe and maintains an internal connection pool that can
        be safely accessed from multiple threads when using async methods.
    
    Performance Notes:
        - Async methods are recommended for I/O-bound applications
        - Connection pooling is automatically managed for optimal resource usage
        - Parameterized queries provide better performance and security
        - Results are streamed efficiently to minimize memory usage
    
    Attributes:
        _conn: The underlying Rust connection implementation
    """
    
    def __init__(
        self, 
        connection_string=None, 
        pool_config=None, 
        ssl_config=None, 
        server=None, 
        database=None, 
        username=None, 
        password=None, 
        trusted_connection=None
    ):
        """
        Initialize a new SQL Server connection.
        
        Args:
            connection_string (str, optional): Complete ADO.NET-style connection string.
                Takes precedence over individual parameters if provided.
                Example: "Server=localhost;Database=test;Trusted_Connection=yes"
                
            pool_config (PoolConfig, optional): Configuration for the connection pool.
                Allows customization of pool size, timeouts, and behavior.
                
            ssl_config (SslConfig, optional): SSL/TLS configuration for secure connections.
                Required for encrypted connections to Azure SQL Database and other
                secure SQL Server instances.
                
            server (str, optional): SQL Server hostname or IP address.
                Can include instance name (e.g., "localhost\\SQLEXPRESS") or port
                (e.g., "localhost:1433").
                
            database (str, optional): Name of the database to connect to.
                If not specified, connects to the default database for the user.
                
            username (str, optional): Username for SQL Server authentication.
                Required when not using Windows Authentication.
                
            password (str, optional): Password for SQL Server authentication.
                Required when username is provided.
                
            trusted_connection (bool, optional): Whether to use Windows Authentication.
                When True, uses the current Windows user's credentials.
                Mutually exclusive with username/password.
        
        Raises:
            ValueError: If connection parameters are invalid or conflicting.
            ConnectionError: If unable to establish initial connection pool.
            
        Examples:
            # Connection string approach
            >>> conn = Connection("Server=localhost;Database=AdventureWorks;Trusted_Connection=yes")
            
            # Individual parameters
            >>> conn = Connection(
            ...     server="localhost",
            ...     database="AdventureWorks", 
            ...     trusted_connection=True
            ... )
            
            # SQL Server authentication
            >>> conn = Connection(
            ...     server="myserver.database.windows.net",
            ...     database="mydatabase",
            ...     username="myuser@mydomain.com",
            ...     password="SecurePassword123!"
            ... )
            
            # With advanced configuration
            >>> from fastmssql import PoolConfig, SslConfig, EncryptionLevel
            >>> pool_config = PoolConfig(max_connections=10, min_connections=2)
            >>> ssl_config = SslConfig(encryption_level=EncryptionLevel.Required)
            >>> conn = Connection(
            ...     server="secure-server.example.com",
            ...     database="production_db",
            ...     username="app_user",
            ...     password="app_password",
            ...     pool_config=pool_config,
            ...     ssl_config=ssl_config
            ... )
        """
        self._conn = _RustConnection(
            connection_string=connection_string,
            pool_config=pool_config,
            ssl_config=ssl_config,
            server=server,
            database=database,
            username=username,
            password=password,
            trusted_connection=trusted_connection
        )
    
    async def query(self, query, parameters=None):
        """
        Execute a SQL query that returns rows (SELECT statements) asynchronously.
        
        This method is specifically designed for SELECT queries and other statements
        that return result sets. It uses the optimized query() method internally
        for maximum performance when fetching data.
        
        Use this method for:
            - SELECT statements
            - Stored procedures that return result sets
            - SHOW commands
            - Any query that returns tabular data
        
        Parameter Binding:
            Parameters are bound using positional placeholders (@P1, @P2, etc.) in the
            query string. The parameter values are provided as a list in the same order.
            
        Supported Parameter Types:
            - None (NULL)
            - bool
            - int (32-bit and 64-bit)
            - float (32-bit and 64-bit)
            - str (varchar, nvarchar, text)
            - bytes (varbinary, image)
            - datetime.datetime (datetime, datetime2)
            - datetime.date (date)
            - datetime.time (time)
            - decimal.Decimal (decimal, money)
            - uuid.UUID (uniqueidentifier)
        
        Args:
            query (str): SQL SELECT query to execute. Use @P1, @P2, etc. for parameters.
                Example: "SELECT * FROM users WHERE age > @P1 AND city = @P2"
                
            parameters (list, optional): List of parameter values in order.
                Values are automatically converted to appropriate SQL types.
                Example: [18, "New York"]
        
        Returns:
            FastExecutionResult: An async iterable result object that provides:
                - Async iteration over result rows
                - fetchone(), fetchmany(), fetchall() methods
                - Row count and column metadata
                - Efficient memory usage for large result sets
        
        Raises:
            SqlError: If the SQL query contains syntax errors or constraint violations.
            ConnectionError: If the database connection is lost during execution.
            TimeoutError: If the query execution exceeds configured timeouts.
            ParameterError: If parameter types cannot be converted or are invalid.
            
        Examples:
            # Simple SELECT query
            >>> result = await conn.query("SELECT * FROM users")
            >>> async for row in result:
            ...     print(f"User ID: {row['id']}, Name: {row['name']}")
            
            # Parameterized query
            >>> result = await conn.query(
            ...     "SELECT * FROM orders WHERE created_date > @P1 AND amount > @P2",
            ...     [datetime(2023, 1, 1), 100.0]
            ... )
            >>> rows = await result.fetchall()
            >>> print(f"Found {len(rows)} orders")
            
            # Complex SELECT with joins
            >>> result = await conn.query(
            ...     \"\"\"SELECT u.name, u.email, COUNT(o.id) as order_count
            ...        FROM users u 
            ...        LEFT JOIN orders o ON u.id = o.user_id 
            ...        WHERE u.created_date > @P1
            ...        GROUP BY u.id, u.name, u.email
            ...        ORDER BY order_count DESC\"\"\",
            ...     [datetime(2023, 1, 1)]
            ... )
            >>> async for row in result:
            ...     print(f"{row['name']}: {row['order_count']} orders")
            
            # Stored procedure that returns data
            >>> result = await conn.query(
            ...     "EXEC GetUsersByDepartment @P1, @P2",
            ...     ["Engineering", True]  # department, active_only
            ... )
            >>> users = await result.fetchall()
        
        Performance Tips:
            - Use this method instead of execute() for SELECT queries for better performance
            - For large result sets, iterate asynchronously rather than calling fetchall()
            - Reuse Connection instances to benefit from connection pooling
            - Use appropriate indexes on filtered columns
        """
        return await self._conn.query(query, parameters)
    
    async def execute(self, query, parameters=None):
        """
        Execute a SQL command that doesn't return rows (INSERT/UPDATE/DELETE/DDL) asynchronously.
        
        This method is specifically designed for SQL commands that modify data or database
        structure but don't return result sets. It uses the optimized execute() method 
        internally for maximum performance when performing data modifications.
        
        Use this method for:
            - INSERT statements
            - UPDATE statements
            - DELETE statements
            - DDL commands (CREATE, ALTER, DROP)
            - Stored procedures that don't return result sets
            - MERGE statements
        
        Parameter Binding:
            Parameters are bound using positional placeholders (@P1, @P2, etc.) in the
            query string. The parameter values are provided as a list in the same order.
            
        Supported Parameter Types:
            - None (NULL)
            - bool
            - int (32-bit and 64-bit)
            - float (32-bit and 64-bit)
            - str (varchar, nvarchar, text)
            - bytes (varbinary, image)
            - datetime.datetime (datetime, datetime2)
            - datetime.date (date)
            - datetime.time (time)
            - decimal.Decimal (decimal, money)
            - uuid.UUID (uniqueidentifier)
        
        Args:
            query (str): SQL command to execute. Use @P1, @P2, etc. for parameters.
                Example: "INSERT INTO users (name, email, age) VALUES (@P1, @P2, @P3)"
                
            parameters (list, optional): List of parameter values in order.
                Values are automatically converted to appropriate SQL types.
                Example: ["John Doe", "john@example.com", 30]
        
        Returns:
            int: Number of rows affected by the command.
                - For INSERT: Number of rows inserted
                - For UPDATE: Number of rows updated
                - For DELETE: Number of rows deleted
                - For DDL: Usually 0 (structure changes don't affect rows)
        
        Raises:
            SqlError: If the SQL command contains syntax errors or constraint violations.
            ConnectionError: If the database connection is lost during execution.
            TimeoutError: If the command execution exceeds configured timeouts.
            ParameterError: If parameter types cannot be converted or are invalid.
            
        Examples:
            # INSERT with parameters
            >>> affected = await conn.execute(
            ...     "INSERT INTO users (name, email, age) VALUES (@P1, @P2, @P3)",
            ...     ["John Doe", "john@example.com", 30]
            ... )
            >>> print(f"Inserted {affected} row(s)")
            
            # UPDATE with conditions
            >>> affected = await conn.execute(
            ...     "UPDATE users SET age = @P1, updated_date = @P2 WHERE id = @P3",
            ...     [31, datetime.now(), 123]
            ... )
            >>> print(f"Updated {affected} user(s)")
            
            # DELETE with parameters
            >>> affected = await conn.execute(
            ...     "DELETE FROM users WHERE age < @P1 AND last_login < @P2",
            ...     [18, datetime(2020, 1, 1)]
            ... )
            >>> print(f"Deleted {affected} inactive users")
            
            # DDL commands
            >>> affected = await conn.execute(
            ...     \"\"\"CREATE TABLE temp_data (
            ...         id INT IDENTITY(1,1) PRIMARY KEY,
            ...         name NVARCHAR(100) NOT NULL,
            ...         created_date DATETIME2 DEFAULT GETDATE()
            ...     )\"\"\"
            ... )
            >>> print(f"Table created (affected rows: {affected})")
            
            # Stored procedure that modifies data
            >>> affected = await conn.execute(
            ...     "EXEC UpdateUserPreferences @P1, @P2",
            ...     [user_id, json.dumps(preferences)]
            ... )
            >>> print(f"Updated preferences for {affected} user(s)")
            
            # Batch operations
            >>> users_to_insert = [
            ...     ["Alice Johnson", "alice@example.com", 28],
            ...     ["Bob Smith", "bob@example.com", 32],
            ...     ["Carol Davis", "carol@example.com", 25]
            ... ]
            >>> total_affected = 0
            >>> for user_data in users_to_insert:
            ...     affected = await conn.execute(
            ...         "INSERT INTO users (name, email, age) VALUES (@P1, @P2, @P3)",
            ...         user_data
            ...     )
            ...     total_affected += affected
            >>> print(f"Inserted {total_affected} users total")
        
        Performance Tips:
            - Use this method instead of query() for data modification commands
            - For bulk operations, consider using batch processing or table-valued parameters
            - Use transactions for multiple related operations
            - Monitor the returned affected row count for validation
        
        Security Notes:
            - Always use parameterized queries to prevent SQL injection attacks
            - Validate affected row counts match expectations
            - Consider using transactions for data consistency
        """
        return await self._conn.execute(query, parameters)
    
    
    async def is_connected(self):
        """
        Check if the connection is active and available for queries.
        
        This method performs a lightweight check to determine if the underlying
        connection pool has active connections and can accept new queries.
        It's useful for health checks and connection validation in long-running
        applications.
        
        The check verifies:
            - Connection pool is initialized and operational
            - At least one connection in the pool is active
            - Network connectivity to the SQL Server instance
            - Authentication credentials are still valid
        
        Returns:
            bool: True if the connection is active and ready for queries,
                  False if the connection is closed, failed, or unavailable.
        
        Raises:
            ConnectionError: If there's an unexpected error checking connection status.
            
        Examples:
            # Basic connection check
            >>> if await conn.is_connected():
            ...     result = await conn.execute("SELECT COUNT(*) FROM users")
            ... else:
            ...     await conn.connect()  # Reconnect if needed
            
            # Health check in a web application
            >>> async def health_check():
            ...     try:
            ...         if await conn.is_connected():
            ...             return {"database": "healthy", "status": "connected"}
            ...         else:
            ...             return {"database": "unhealthy", "status": "disconnected"}
            ...     except Exception as e:
            ...         return {"database": "error", "status": str(e)}
            
            # Periodic connection monitoring
            >>> import asyncio
            >>> 
            >>> async def monitor_connection():
            ...     while True:
            ...         if await conn.is_connected():
            ...             print(f"{datetime.now()}: Database connection is healthy")
            ...         else:
            ...             print(f"{datetime.now()}: Database connection is down!")
            ...             # Attempt to reconnect
            ...             try:
            ...                 await conn.connect()
            ...                 print("Reconnection successful")
            ...             except Exception as e:
            ...                 print(f"Reconnection failed: {e}")
            ...         
            ...         await asyncio.sleep(60)  # Check every minute
        
        Performance Notes:
            - This is a lightweight operation that doesn't execute actual SQL
            - The check uses connection pool metadata and cached connection state
            - Suitable for frequent health checks without performance impact
            - Does not count against connection pool limits
        
        Use Cases:
            - Application startup validation
            - Periodic health monitoring
            - Circuit breaker pattern implementation
            - Load balancer health checks
            - Graceful degradation in microservices
        """
        return await self._conn.is_connected()
    
    async def pool_stats(self):
        """
        Get comprehensive connection pool statistics and health metrics.
        
        This method provides detailed information about the current state of the
        connection pool, including active connections, idle connections, and
        configuration parameters. It's essential for monitoring, debugging, and
        optimizing connection pool performance in production environments.
        
        The statistics help identify:
            - Connection pool utilization patterns
            - Potential connection leaks
            - Optimal pool sizing configuration
            - Performance bottlenecks
            - Resource contention issues
        
        Returns:
            dict: A dictionary containing pool statistics with the following keys:
                
                When connected:
                    - 'connections' (int): Total number of connections in the pool
                    - 'idle_connections' (int): Number of idle connections available
                    - 'active_connections' (int): Number of connections currently in use
                    - 'max_size' (int): Maximum allowed connections in the pool
                    - 'min_idle' (int): Minimum idle connections maintained
                    
                When disconnected:
                    - 'connected' (bool): False, indicating no active pool
        
        Raises:
            ConnectionError: If unable to retrieve pool statistics due to connection issues.
            
        Examples:
            # Basic pool monitoring
            >>> stats = await conn.pool_stats()
            >>> if stats.get('connected', True):  # Handle disconnected case
            ...     print(f"Active connections: {stats['active_connections']}")
            ...     print(f"Idle connections: {stats['idle_connections']}")
            ...     print(f"Pool utilization: {stats['active_connections']/stats['max_size']*100:.1f}%")
            
            # Comprehensive pool monitoring
            >>> async def monitor_pool():
            ...     stats = await conn.pool_stats()
            ...     
            ...     if not stats.get('connected', True):
            ...         print("❌ Connection pool is not active")
            ...         return
            ...     
            ...     total = stats['connections']
            ...     active = stats['active_connections']
            ...     idle = stats['idle_connections']
            ...     max_size = stats['max_size']
            ...     min_idle = stats['min_idle']
            ...     
            ...     utilization = (active / max_size) * 100
            ...     
            ...     print(f"📊 Connection Pool Statistics:")
            ...     print(f"   Total connections: {total}")
            ...     print(f"   Active connections: {active}")
            ...     print(f"   Idle connections: {idle}")
            ...     print(f"   Max pool size: {max_size}")
            ...     print(f"   Min idle: {min_idle}")
            ...     print(f"   Utilization: {utilization:.1f}%")
            ...     
            ...     # Health assessment
            ...     if utilization > 90:
            ...         print("⚠️  High pool utilization - consider increasing max_size")
            ...     elif idle < min_idle:
            ...         print("⚠️  Low idle connections - pool may be under pressure")
            ...     elif utilization < 10 and total > min_idle * 2:
            ...         print("ℹ️  Low utilization - consider reducing max_size")
            ...     else:
            ...         print("✅ Pool appears healthy")
            
            # Pool statistics for alerting
            >>> async def check_pool_health():
            ...     stats = await conn.pool_stats()
            ...     
            ...     if not stats.get('connected', True):
            ...         return {"status": "critical", "message": "Pool disconnected"}
            ...     
            ...     utilization = stats['active_connections'] / stats['max_size']
            ...     idle_ratio = stats['idle_connections'] / stats['max_size']
            ...     
            ...     if utilization > 0.9:
            ...         return {
            ...             "status": "warning", 
            ...             "message": f"High utilization: {utilization:.1%}",
            ...             "stats": stats
            ...         }
            ...     elif idle_ratio < 0.1:
            ...         return {
            ...             "status": "warning",
            ...             "message": f"Low idle connections: {stats['idle_connections']}",
            ...             "stats": stats
            ...         }
            ...     else:
            ...         return {"status": "healthy", "stats": stats}
            
            # Logging pool metrics
            >>> import logging
            >>> 
            >>> async def log_pool_metrics():
            ...     stats = await conn.pool_stats()
            ...     if stats.get('connected', True):
            ...         logging.info(
            ...             "Pool metrics: active=%d, idle=%d, total=%d, utilization=%.1f%%",
            ...             stats['active_connections'],
            ...             stats['idle_connections'], 
            ...             stats['connections'],
            ...             (stats['active_connections'] / stats['max_size']) * 100
            ...         )
        
        Monitoring Best Practices:
            - Monitor pool utilization during peak load periods
            - Set up alerts for utilization > 80% or idle connections < min_idle
            - Track connection acquisition times and pool exhaustion events
            - Use metrics for capacity planning and performance optimization
            - Log pool statistics periodically for historical analysis
        
        Performance Impact:
            - This operation has minimal performance overhead
            - Safe to call frequently for monitoring purposes
            - Does not affect active connections or pool operation
            - Recommended for inclusion in health check endpoints
        """
        result_tuple = await self._conn.pool_stats()
        
        # Convert tuple to dictionary
        connected, connections, idle_connections, max_size, min_idle = result_tuple
        
        if connected:
            return {
                'connections': connections,
                'idle_connections': idle_connections,
                'max_size': max_size,
                'min_idle': min_idle,
                'active_connections': connections - idle_connections,
            }
        else:
            return {'connected': False}
    
    async def connect(self):
        """Explicitly connect to the database."""
        return await self._conn.connect()
    
    async def disconnect(self):
        """Explicitly disconnect from the database."""
        return await self._conn.disconnect()
    
    async def __aenter__(self):
        await self._conn.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._conn.__aexit__(exc_type, exc_val, exc_tb)
        return None
    
    def __enter__(self):
        return self._conn.__enter__()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._conn.__exit__(exc_type, exc_val, exc_tb)

# Preserve module documentation
if hasattr(_RustConnection, "__doc__"):
    __doc__ = _RustConnection.__doc__

__all__ = ["Connection", "PoolConfig", "SslConfig", "FastExecutionResult", "version", "EncryptionLevel", "Parameter", "Parameters"]
