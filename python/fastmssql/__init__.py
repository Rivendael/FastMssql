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


class Transaction:
    """Single dedicated connection for SQL Server transactions.
    
    Provides a non-pooled connection where all operations happen on the same
    underlying connection, ensuring transaction safety for BEGIN/COMMIT/ROLLBACK.
    
    Example:
        async with Transaction(server="localhost", database="mydb") as conn:
            async with conn.transaction():
                await conn.execute("INSERT INTO ...")
    """
    
    def __init__(self, connection_string=None, ssl_config=None,
                 server=None, database=None, username=None, password=None,
                 application_intent=None, port=None, instance_name=None,
                 application_name=None):
        """Initialize a dedicated non-pooled connection for transactions."""
        self._rust_conn = _RustTransaction(
            connection_string=connection_string,
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
        """Execute a SELECT query that returns rows."""
        return await self._rust_conn.query(sql, params)
    
    async def execute(self, sql, params=None):
        """Execute an INSERT/UPDATE/DELETE/DDL command."""
        return await self._rust_conn.execute(sql, params)
    
    async def begin(self):
        """Begin a transaction."""
        try:
            await self._rust_conn.query("BEGIN TRANSACTION")
        except RuntimeError:
            pass  # Expected: tiberius throws error but transaction opens successfully
    
    async def commit(self):
        """Commit the current transaction."""
        try:
            await self._rust_conn.query("COMMIT TRANSACTION")
        except RuntimeError:
            pass  # Expected: tiberius throws error but transaction commits successfully
    
    async def rollback(self):
        """Rollback the current transaction."""
        try:
            await self._rust_conn.query("ROLLBACK TRANSACTION")
        except RuntimeError:
            pass  # Expected: tiberius throws error but transaction rolls back successfully
    
    def transaction(self):
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
        """Async context manager entry - automatically BEGIN transaction."""
        await self.begin()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - automatically COMMIT or ROLLBACK."""
        try:
            if exc_type is not None:
                # An exception occurred - rollback
                try:
                    await self.rollback()
                except Exception:
                    pass
            else:
                # No exception - commit
                await self.commit()
        except Exception:
            # If commit fails, try to rollback
            try:
                await self.rollback()
            except Exception:
                pass
        
        return False  # Don't suppress exceptions


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
