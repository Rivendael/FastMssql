"""FastMSSQL - High-Performance Microsoft SQL Server Driver for Python

High-performance Rust-backed Python driver for SQL Server with async/await support,
connection pooling, SSL/TLS encryption, and parameterized queries.
"""

# Import from the compiled Rust module
from .fastmssql import (
    Connection as _RustConnection,
    PoolConfig,
    SslConfig,
    FastExecutionResult,
    FastRow,
    Parameter,
    Parameters,
    EncryptionLevel,
    version,
)


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
        """Get connection pool statistics and convert tuple to dict."""
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


__all__ = [
    "Connection",
    "PoolConfig",
    "SslConfig",
    "FastExecutionResult",
    "FastRow",
    "Parameter",
    "Parameters",
    "EncryptionLevel",
    "version",
]
