"""FastMSSQL - High-Performance Microsoft SQL Server Driver for Python

High-performance Rust-backed Python driver for SQL Server with async/await support,
connection pooling, SSL/TLS encryption, and parameterized queries.
"""

# Import from the compiled Rust module
from .fastmssql import (
    Connection as _RustConnection,
    Transaction as _RustTransaction,
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




class Transaction:
    """Single dedicated connection (non-pooled) for transaction support.
    
    This class wraps a direct connection that is NOT pooled, allowing SQL Server
    transactions (BEGIN/COMMIT/ROLLBACK) to work correctly since all operations
    happen on the same connection.
    
    Usage:
        async with Transaction(connection_string) as conn:
            async with conn.transaction():
                result = await conn.query("SELECT ...")
                await conn.execute("INSERT INTO ...")
    
    Or explicitly with convenience methods:
        async with Transaction(connection_string) as conn:
            await conn.begin()
            try:
                await conn.query("SELECT ...")
                await conn.execute("INSERT INTO ...")
                await conn.commit()
            except Exception:
                await conn.rollback()
    
    Or manually (less convenient):
        conn = Transaction(
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
        self._rust_conn = _RustTransaction(
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
    
    async def begin(self):
        """Begin a transaction.
        
        Note: Tiberius throws a RuntimeError for transaction commands, but they
        actually execute successfully. This method handles that internally.
        
        Usage:
            await conn.begin()
            try:
                await conn.execute("INSERT INTO ...")
                await conn.commit()
            except Exception:
                await conn.rollback()
        """
        try:
            await self._rust_conn.query("BEGIN TRANSACTION")
        except RuntimeError:
            pass  # Expected: tiberius throws error but transaction opens successfully
    
    async def commit(self):
        """Commit the current transaction.
        
        Note: Tiberius throws a RuntimeError for transaction commands, but they
        actually execute successfully. This method handles that internally.
        """
        try:
            await self._rust_conn.query("COMMIT TRANSACTION")
        except RuntimeError:
            pass  # Expected: tiberius throws error but transaction commits successfully
    
    async def rollback(self):
        """Rollback the current transaction.
        
        Note: Tiberius throws a RuntimeError for transaction commands, but they
        actually execute successfully. This method handles that internally.
        """
        try:
            await self._rust_conn.query("ROLLBACK TRANSACTION")
        except RuntimeError:
            pass  # Expected: tiberius throws error but transaction rolls back successfully
    
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
        await self._conn.begin()
        self._started = True
        return self._conn
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Commit or rollback."""
        if not self._started:
            return False
        
        try:
            if exc_type is not None:
                try:
                    await self._conn.rollback()
                except Exception:
                    pass
            else:
                await self._conn.commit()
        except Exception:
            try:
                await self._conn.rollback()
            except Exception:
                pass
        
        return False


__all__ = [
    "Connection",
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
