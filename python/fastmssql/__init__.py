"""FastMSSQL - High-Performance Microsoft SQL Server Driver for Python

High-performance Rust-backed Python driver for SQL Server with async/await support,
connection pooling, SSL/TLS encryption, and parameterized queries.
"""

# Import from the compiled Rust module
from .fastmssql import (
    Connection as _RustConnection,
)
from .fastmssql import (
    EncryptionLevel,
    FastRow,
    Parameter,
    Parameters,
    PoolConfig,
    QueryStream,
    SslConfig,
    version,
)
from .fastmssql import (
    Transaction as _RustTransaction,
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
            await conn.execute("INSERT INTO ...")
    """

    def __init__(self, *args, **kwargs):
        """Initialize a dedicated non-pooled connection for transactions."""
        self._rust_conn = _RustTransaction(*args, **kwargs)
        self._TRANSACTION_BEGUN = False
        self._TRANSACTION_COMMITTED = False
        self._TRANSACTION_ROLLEDBACK = False

    def _reset_transaction_flags(self):
        """Reset the transaction state flags."""
        self._TRANSACTION_BEGUN = False
        self._TRANSACTION_COMMITTED = False
        self._TRANSACTION_ROLLEDBACK = False
    
    def _validate_transaction_flags(self):
        if not self._TRANSACTION_BEGUN:
            raise RuntimeError("Transaction has not begun")
        if self._TRANSACTION_COMMITTED:
            raise RuntimeError("Transaction has already been committed")
        if self._TRANSACTION_ROLLEDBACK:
            raise RuntimeError("Transaction has already been rolled back")

    async def query(self, sql, params=None):
        """Execute a SELECT query that returns rows."""
        return await self._rust_conn.query(sql, params)

    async def execute(self, sql, params=None):
        """Execute an INSERT/UPDATE/DELETE/DDL command."""
        return await self._rust_conn.execute(sql, params)

    async def begin(self):
        """Begin a transaction."""
        # If previous transaction completed, reset flags to allow reuse
        if self._TRANSACTION_COMMITTED or self._TRANSACTION_ROLLEDBACK:
            self._reset_transaction_flags()
        
        if self._TRANSACTION_BEGUN:
            raise RuntimeError("Transaction has already begun")
        await self._rust_conn.begin()
        self._TRANSACTION_BEGUN = True

    async def commit(self):
        """Commit the current transaction."""

        self._validate_transaction_flags()
        await self._rust_conn.commit()
        self._TRANSACTION_COMMITTED = True

    async def rollback(self):
        """Rollback the current transaction."""

        self._validate_transaction_flags()
        await self._rust_conn.rollback()
        self._TRANSACTION_ROLLEDBACK = True

    async def close(self):
        """Close the connection."""
        result = await self._rust_conn.close()
        self._reset_transaction_flags()
        return result

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

        self._reset_transaction_flags()
        return False  # Don't suppress exceptions


__all__ = [
    "Connection",
    "Transaction",
    "PoolConfig",
    "SslConfig",
    "QueryStream",
    "FastRow",
    "Parameter",
    "Parameters",
    "EncryptionLevel",
    "version",
]
