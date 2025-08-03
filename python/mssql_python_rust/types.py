"""
Type definitions and utilities for mssql-python-rust

This module provides Python type definitions that complement the Rust types.
"""

from typing import Any, Dict, List, Optional, Union
from enum import Enum

class SqlDataType(Enum):
    """SQL Server data types."""
    BIGINT = "BIGINT"
    INT = "INT"
    SMALLINT = "SMALLINT"
    TINYINT = "TINYINT"
    BIT = "BIT"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    MONEY = "MONEY"
    SMALLMONEY = "SMALLMONEY"
    FLOAT = "FLOAT"
    REAL = "REAL"
    DATETIME = "DATETIME"
    DATETIME2 = "DATETIME2"
    SMALLDATETIME = "SMALLDATETIME"
    DATE = "DATE"
    TIME = "TIME"
    DATETIMEOFFSET = "DATETIMEOFFSET"
    CHAR = "CHAR"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    NCHAR = "NCHAR"
    NVARCHAR = "NVARCHAR"
    NTEXT = "NTEXT"
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    IMAGE = "IMAGE"
    UNIQUEIDENTIFIER = "UNIQUEIDENTIFIER"
    XML = "XML"

class ConnectionState(Enum):
    """Connection state enumeration."""
    CLOSED = "CLOSED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    DISCONNECTING = "DISCONNECTING"
    ERROR = "ERROR"

class Parameter:
    """Represents a SQL parameter."""
    
    def __init__(self, name: str, value: Any, sql_type: Optional[SqlDataType] = None):
        """Initialize a parameter.
        
        Args:
            name: Parameter name (without @)
            value: Parameter value
            sql_type: Optional SQL data type
        """
        self.name = name.lstrip('@')  # Remove @ if present
        self.value = value
        self.sql_type = sql_type
    
    def __repr__(self) -> str:
        return f"Parameter(name='@{self.name}', value={self.value!r}, sql_type={self.sql_type})"

class ConnectionInfo:
    """Information about a database connection."""
    
    def __init__(self, server: str, database: str, username: Optional[str] = None):
        """Initialize connection info.
        
        Args:
            server: Server name or address
            database: Database name
            username: Username (None for integrated security)
        """
        self.server = server
        self.database = database
        self.username = username
        self.state = ConnectionState.CLOSED
    
    def __repr__(self) -> str:
        return f"ConnectionInfo(server='{self.server}', database='{self.database}', username='{self.username}')"

class QueryResult:
    """Represents the result of a query."""
    
    def __init__(self, rows: List[Dict[str, Any]], rows_affected: int = 0):
        """Initialize query result.
        
        Args:
            rows: List of result rows
            rows_affected: Number of rows affected (for non-queries)
        """
        self.rows = rows
        self.rows_affected = rows_affected
        self.column_names = list(rows[0].keys()) if rows else []
    
    def __len__(self) -> int:
        return len(self.rows)
    
    def __iter__(self):
        return iter(self.rows)
    
    def __getitem__(self, index: int) -> Dict[str, Any]:
        return self.rows[index]
    
    def to_dicts(self) -> List[Dict[str, Any]]:
        """Convert to list of dictionaries."""
        return self.rows.copy()
    
    def __repr__(self) -> str:
        return f"QueryResult(rows={len(self.rows)}, columns={len(self.column_names)})"

# Exception classes
class MSSQLError(Exception):
    """Base exception for MSSQL operations."""
    pass

class ConnectionError(MSSQLError):
    """Raised when connection operations fail."""
    pass

class ParameterError(MSSQLError):
    """Raised when parameter operations fail."""
    pass