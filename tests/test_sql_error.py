"""
Tests for SqlError exception handling

This module tests the new SqlError exception that is raised when SQL Server
returns an error response, ensuring that error details are properly captured
and accessible via named attributes.
"""

import pytest
from conftest import Config

try:
    from fastmssql import Connection, SqlError, Transaction
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first")


class TestSqlErrorBasics:
    """Test basic SqlError exception behavior."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_raised_for_invalid_sql(self, test_config: Config):
        """SqlError should be raised for syntactically invalid SQL."""
        try:
            async with Connection(test_config.connection_string) as conn:
                with pytest.raises(SqlError):
                    await conn.query("INVALID SYNTAX HERE")
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_raised_for_nonexistent_table(self, test_config: Config):
        """SqlError should be raised when querying non-existent table."""
        try:
            async with Connection(test_config.connection_string) as conn:
                with pytest.raises(SqlError):
                    await conn.query("SELECT * FROM nonexistent_table_xyz")
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_raised_for_nonexistent_column(self, test_config: Config):
        """SqlError should be raised when selecting non-existent column."""
        try:
            async with Connection(test_config.connection_string) as conn:
                with pytest.raises(SqlError):
                    await conn.query("SELECT nonexistent_column FROM sys.databases")
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_raised_on_execute(self, test_config: Config):
        """SqlError should be raised on execute() for invalid SQL."""
        try:
            async with Connection(test_config.connection_string) as conn:
                with pytest.raises(SqlError):
                    await conn.execute("INSERT INTO nonexistent VALUES (1)")
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestSqlErrorAttributes:
    """Test that SqlError has proper named attributes."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_has_code_attribute(self, test_config: Config):
        """SqlError should have a 'code' attribute with the error number."""
        try:
            async with Connection(test_config.connection_string) as conn:
                try:
                    await conn.query("SELECT * FROM nonexistent_table_xyz")
                    pytest.fail("Expected SqlError to be raised")
                except SqlError as e:
                    assert hasattr(e, "code")
                    assert isinstance(e.code, int)
                    assert e.code > 0  # SQL Server error codes are positive
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_has_message_attribute(self, test_config: Config):
        """SqlError should have a 'message' attribute with the error message."""
        try:
            async with Connection(test_config.connection_string) as conn:
                try:
                    await conn.query("SELECT * FROM nonexistent_table_xyz")
                    pytest.fail("Expected SqlError to be raised")
                except SqlError as e:
                    assert hasattr(e, "message")
                    assert isinstance(e.message, str)
                    assert len(e.message) > 0
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_has_state_attribute(self, test_config: Config):
        """SqlError should have a 'state' attribute with the error state."""
        try:
            async with Connection(test_config.connection_string) as conn:
                try:
                    await conn.query("SELECT * FROM nonexistent_table_xyz")
                    pytest.fail("Expected SqlError to be raised")
                except SqlError as e:
                    assert hasattr(e, "state")
                    assert isinstance(e.state, int)
                    assert e.state >= 0  # State is a byte
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_str_representation(self, test_config: Config):
        """SqlError string representation should contain the message."""
        try:
            async with Connection(test_config.connection_string) as conn:
                try:
                    await conn.query("SELECT * FROM nonexistent_table_xyz")
                    pytest.fail("Expected SqlError to be raised")
                except SqlError as e:
                    error_str = str(e)
                    assert len(error_str) > 0
                    # The message should be part of the string representation
                    assert e.message in error_str or error_str in e.message
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_specific_error_codes(self, test_config: Config):
        """Test specific SQL Server error codes for different scenarios."""
        try:
            async with Connection(test_config.connection_string) as conn:
                # Error 208: Invalid object name (table doesn't exist)
                try:
                    await conn.query("SELECT * FROM nonexistent_table_xyz")
                    pytest.fail("Expected SqlError to be raised")
                except SqlError as e:
                    assert e.code == 208  # Object not found
                    assert "nonexistent_table_xyz" in e.message

                # Error 207: Invalid column name
                try:
                    await conn.query("SELECT invalid_col FROM sys.databases")
                    pytest.fail("Expected SqlError to be raised")
                except SqlError as e:
                    assert e.code == 207  # Invalid column name
                    assert "invalid_col" in e.message
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestSqlErrorInBatchOperations:
    """Test SqlError in batch query/execute operations."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_in_batch_query(self, test_config: Config):
        """SqlError should be raised in batch query operations."""
        try:
            async with Connection(test_config.connection_string) as conn:
                with pytest.raises(SqlError):
                    await conn.query_batch([
                        ("SELECT * FROM sys.databases", None),
                        ("SELECT * FROM nonexistent_table", None),  # This should fail
                    ])
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_in_batch_execute(self, test_config: Config):
        """SqlError should be raised in batch execute operations."""
        try:
            async with Connection(test_config.connection_string) as conn:
                with pytest.raises(SqlError):
                    await conn.execute_batch([
                        ("SELECT 1", None),
                        ("INSERT INTO nonexistent VALUES (1)", None),  # This should fail
                    ])
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestSqlErrorInTransactions:
    """Test SqlError in transaction operations."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_in_transaction_query(self, test_config: Config):
        """SqlError should be raised in transaction query operations."""
        try:
            async with Transaction(**test_config.asdict()) as trans:
                with pytest.raises(SqlError):
                    await trans.query("SELECT * FROM nonexistent_table_xyz")
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_in_transaction_execute(self, test_config: Config):
        """SqlError should be raised in transaction execute operations."""
        try:
            async with Transaction(**test_config.asdict()) as trans:
                with pytest.raises(SqlError):
                    await trans.execute("INSERT INTO nonexistent VALUES (1)")
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_in_transaction_batch_query(self, test_config: Config):
        """SqlError should be raised in batch query operations."""
        try:
            async with Connection(test_config.connection_string) as conn:
                with pytest.raises(SqlError):
                    await conn.query_batch([
                        ("SELECT 1", None),
                        ("SELECT * FROM nonexistent", None),  # This should fail
                    ])
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sql_error_in_transaction_batch_execute(self, test_config: Config):
        """SqlError should be raised in batch execute operations."""
        try:
            async with Connection(test_config.connection_string) as conn:
                with pytest.raises(SqlError):
                    await conn.execute_batch([
                        ("SELECT 1", None),
                        ("INSERT INTO nonexistent VALUES (1)", None),  # This should fail
                    ])
        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestSqlErrorHandling:
    """Test practical error handling patterns with SqlError."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_error_handling_pattern(self, test_config: Config):
        """Test common error handling pattern with SqlError."""
        try:
            async with Connection(test_config.connection_string) as conn:
                error_caught = False
                try:
                    await conn.query("SELECT * FROM nonexistent")
                except SqlError as e:
                    error_caught = True
                    # Verify we can access all attributes
                    assert e.code is not None
                    assert e.message is not None
                    assert e.state is not None
                    # Verify we can use them in conditionals
                    if e.code == 208:
                        pass  # Object not found - expected
                    # Verify we can log the error
                    error_details = f"Error {e.code}: {e.message} (state {e.state})"
                    assert len(error_details) > 0

                assert error_caught, "SqlError should have been caught"
        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_error_discrimination(self, test_config: Config):
        """Test discriminating between different error types."""
        try:
            async with Connection(test_config.connection_string) as conn:
                # Test object not found (208)
                try:
                    await conn.query("SELECT * FROM nonexistent_table")
                except SqlError as e:
                    assert e.code == 208

                # Test invalid column (207)
                try:
                    await conn.query("SELECT invalid_col FROM sys.databases")
                except SqlError as e:
                    assert e.code == 207

                # Test syntax error - error 156 (unexpected keyword)
                try:
                    await conn.query("SELECT FROM")
                except SqlError as e:
                    assert e.code == 156  # Incorrect syntax near keyword 'FROM'
        except Exception as e:
            pytest.fail(f"Database not available: {e}")
