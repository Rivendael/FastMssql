"""FastMSSQL - High-Performance Microsoft SQL Server Driver for Python

High-performance Rust-backed Python driver for SQL Server with async/await support,
connection pooling, SSL/TLS encryption, and parameterized queries.
"""

# Import from the compiled Rust module
from .fastmssql import (
    Connection as _RustConnection,
    SingleConnection as _RustSingleConnection,
    PoolConfig,
    SslConfig,
    FastExecutionResult,
    FastRow,
    Parameter,
    Parameters,
    EncryptionLevel,
    version,
)

try:
    from enum import StrEnum
except ImportError:
    # Python 3.10 compatibility: StrEnum was added in Python 3.11
    from enum import Enum
    class StrEnum(str, Enum):
        pass

class ApplicationIntent(StrEnum):
    READ_ONLY = "ReadOnly"
    READ_WRITE = "ReadWrite"

class Connection:
    """Thin wrapper to fix async context manager behavior."""
    
    def __init__(self, *args, **kwargs):
        self._conn = _RustConnection(*args, **kwargs)
    
    def __getattr__(self, name):
        return getattr(self._conn, name)
    
    async def __aenter__(self):
        await self._conn.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._conn.__aexit__(exc_type, exc_val, exc_tb)
    
    async def pool_stats(self):
        """Get connection pool statistics.
        
        Returns a dict with keys: connected, connections, idle_connections, 
        active_connections, max_size, min_idle
        """
        return await self._conn.pool_stats()


class _ConnectionPoolDisabler:
    """Wrapper to prevent connection from checking out the pool during transaction.
    
    This is a workaround for SQL Server transactions which are per-connection.
    Since the connection pool returns different connections for each query(),
    we need to force all queries in a transaction to use the same connection.
    
    However, this is currently IMPOSSIBLE without Rust-level changes because:
    1. Each query() call checks out a NEW connection from the pool
    2. Different connections have separate transaction state
    3. SQL Server reports "Transaction count mismatch" errors
    
    This class attempts to work around it by storing the connection state,
    but it won't actually work without modifications to the Rust layer.
    """
    
    def __init__(self, rust_conn):
        """Initialize with the internal Rust connection object."""
        self._rust_conn = rust_conn
        self._in_transaction = False
        self._buffered_queries = []
    
    async def query(self, sql, params=None):
        """Attempt to execute a query."""
        # We can't actually prevent the Rust query() from checking out a new connection
        # So we just pass through to the Rust implementation
        # This is still broken, but at least the error message is clearer
        return await self._rust_conn.query(sql, params)
    
    async def execute(self, sql, params=None):
        """Attempt to execute a command."""
        return await self._rust_conn.execute(sql, params)
    
    def __getattr__(self, name):
        """Delegate all other attributes to the Rust connection."""
        return getattr(self._rust_conn, name)




class SingleConnection:
    """Single dedicated connection (non-pooled) for transaction support.
    
    This class wraps a direct connection that is NOT pooled, allowing SQL Server
    transactions (BEGIN/COMMIT/ROLLBACK) to work correctly since all operations
    happen on the same connection.
    
    Usage:
        async with SingleConnection(connection_string) as conn:
            async with conn.transaction():
                result = await conn.query("SELECT ...")
                await conn.execute("INSERT INTO ...")
    
    Or explicitly:
        conn = SingleConnection(
            server="localhost",
            database="mydb",
            username="sa",
            password="password"
        )
        
        try:
            await conn.query("BEGIN TRANSACTION")
            await conn.query("SELECT ...")
            await conn.query("COMMIT TRANSACTION")
        except Exception:
            await conn.query("ROLLBACK TRANSACTION")
        finally:
            await conn.close()
    """
    
    def __init__(self, connection_string=None, pool_config=None, ssl_config=None,
                 server=None, database=None, username=None, password=None,
                 application_intent=None, port=None, instance_name=None,
                 application_name=None):
        """Initialize a single dedicated connection (non-pooled).
        
        Args:
            connection_string: ADO.NET connection string
            server: Server hostname
            database: Database name
            username: SQL Server username
            password: SQL Server password
            pool_config: PoolConfig (ignored for single connections)
            ssl_config: SslConfig for encryption
            application_intent: "ReadOnly" or "ReadWrite"
            port: Server port (default 1433)
            instance_name: SQL Server instance name
            application_name: Application name for server tracking
        """
        self._rust_conn = _RustSingleConnection(
            connection_string=connection_string,
            pool_config=pool_config,
            ssl_config=ssl_config,
            server=server,
            database=database,
            username=username,
            password=password,
            application_intent=application_intent,
            port=port,
            instance_name=instance_name,
            application_name=application_name,
        )
    
    async def query(self, sql, params=None):
        """Execute a query that returns rows.
        
        All queries on this connection use the same underlying connection,
        making transactions safe.
        
        Args:
            sql: SQL query string with @P1, @P2, etc. placeholders
            params: Optional list or Parameters object
            
        Returns:
            FastExecutionResult with query results
        """
        return await self._rust_conn.query(sql, params)
    
    async def execute(self, sql, params=None):
        """Execute a command that doesn't return rows.
        
        Args:
            sql: SQL command string with @P1, @P2, etc. placeholders
            params: Optional list or Parameters object
            
        Returns:
            Number of affected rows
        """
        return await self._rust_conn.execute(sql, params)
    
    async def transaction(self):
        """Return an async context manager for transactions.
        
        Usage:
            async with conn.transaction():
                await conn.execute("INSERT INTO ...")
        """
        return _TransactionContextManager(self)
    
    async def close(self):
        """Close the connection."""
        return await self._rust_conn.close()
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
        return False


class _TransactionContextManager:
    """Helper for transaction() context manager."""
    
    def __init__(self, conn):
        self._conn = conn
        self._started = False
    
    async def __aenter__(self):
        """Begin transaction."""
        await self._conn.query("BEGIN TRANSACTION")
        self._started = True
        return self._conn
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Commit or rollback."""
        if not self._started:
            return False
        
        try:
            if exc_type is not None:
                try:
                    await self._conn.query("ROLLBACK TRANSACTION")
                except Exception:
                    pass
            else:
                await self._conn.query("COMMIT TRANSACTION")
        except Exception:
            try:
                await self._conn.query("ROLLBACK TRANSACTION")
            except Exception:
                pass
        
        return False


class Transaction:
    """Async context manager for database transactions.
    
    ⚠️ LIMITATION: Due to connection pooling in the Rust layer, transactions
    cannot be reliably implemented at this level. SQL Server transactions are
    per-connection, but the connection pool returns different connections for
    each query() call.
    
    You will see errors like: "Transaction count after EXECUTE indicates a 
    mismatching number of BEGIN and COMMIT statements."
    
    WORKAROUND OPTIONS:
    
    1. **Disable the pool for your transaction (RECOMMENDED):**
       Instead of using the pooled Connection class, create a direct connection
       and use it ONLY for transaction purposes. Currently not exposed in the API.
    
    2. **Use SQL Server's MultiActiveResultSets (MARS):**
       If supported by the driver, enable MARS to allow multiple active commands
       on a single connection. This won't help with pooling though.
    
    3. **Don't use transactions in this library yet:**
       Wait for Rust-level implementation that properly handles pooled transactions.
    
    4. **Implement transactions in your application layer:**
       Use SQL Server's transaction IDs or application-level locking instead.
    
    This class remains for future compatibility and as documentation of the issue.
    """
    
    def __init__(self, conn):
        """Initialize transaction with a connection.
        
        Args:
            conn: A Connection object (Python wrapper)
            
        Note:
            This will not work correctly due to connection pooling.
        """
        if isinstance(conn, Connection):
            self._conn = conn
            self._rust_conn = conn._conn  # Get the internal Rust connection
        else:
            # Assume it's the Rust connection
            self._rust_conn = conn
            
            # Create a wrapper for consistency
            class _ConnWrapper:
                def __init__(self, rust_conn):
                    self._conn = rust_conn
                def __getattr__(self, name):
                    return getattr(self._conn, name)
            self._conn = _ConnWrapper(conn)
        
        self._transaction_started = False
        self._active = False
    
    async def __aenter__(self):
        """Enter transaction context - execute BEGIN TRANSACTION.
        
        ⚠️ This will immediately fail with "Transaction count mismatch" error
        because the next query() call will use a different connection from the pool.
        """
        self._active = True
        try:
            # This will execute on connection A from the pool
            await self._conn.query("BEGIN TRANSACTION")
            self._transaction_started = True
            return self
        except Exception as e:
            self._active = False
            self._transaction_started = False
            raise
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit transaction context - commit or rollback.
        
        ⚠️ This will also fail because COMMIT/ROLLBACK happens on a 
        different connection than the BEGIN TRANSACTION.
        """
        self._active = False
        
        if not self._transaction_started:
            return False
        
        try:
            if exc_type is not None:
                # An exception occurred, rollback
                try:
                    await self._conn.query("ROLLBACK TRANSACTION")
                except Exception:
                    pass
            else:
                # No exception, commit
                await self._conn.query("COMMIT TRANSACTION")
        except Exception as e:
            # If commit fails, try to rollback
            try:
                await self._conn.query("ROLLBACK TRANSACTION")
            except Exception:
                pass
        
        return False  # Don't suppress exceptions
    
    async def query(self, sql: str, params=None):
        """Execute a query within the transaction.
        
        Args:
            sql: SQL query string with @P1, @P2, etc. placeholders
            params: Optional list or Parameters object
            
        Returns:
            FastExecutionResult with query results
            
        Note:
            This will use a different connection from the pool.
        """
        return await self._conn.query(sql, params)
    
    async def execute(self, sql: str, params=None):
        """Execute a command within the transaction.
        
        Args:
            sql: SQL command string with @P1, @P2, etc. placeholders
            params: Optional list or Parameters object
            
        Returns:
            Number of affected rows
            
        Note:
            This will use a different connection from the pool.
        """
        return await self._conn.execute(sql, params)


__all__ = [
    "Connection",
    "SingleConnection",
    "Transaction",
    "PoolConfig",
    "SslConfig",
    "FastExecutionResult",
    "FastRow",
    "Parameter",
    "Parameters",
    "EncryptionLevel",
    "version",
]
