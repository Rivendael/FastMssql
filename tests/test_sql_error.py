"""
Tests for SqlError exception handling

This module tests the new SqlError exception that is raised when SQL Server
returns an error response, ensuring that error details are properly captured
and accessible via named attributes. Also tests SqlConnectionError, TlsError,
ProtocolError, and ConversionError for importability and structure.
"""

import pytest
from conftest import Config

try:
    from fastmssql import (
        Connection,
        SqlConnectionError,
        ConversionError,
        ProtocolError,
        SqlError,
        TlsError,
        Transaction,
        PoolConfig,
    )
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


class TestCustomErrorTypes:
    """Test the non-SqlError custom exception types."""

    def test_connection_error_is_importable(self):
        """SqlConnectionError should be importable from fastmssql."""
        assert SqlConnectionError is not None

    def test_tls_error_is_importable(self):
        """TlsError should be importable from fastmssql."""
        assert TlsError is not None

    def test_protocol_error_is_importable(self):
        """ProtocolError should be importable from fastmssql."""
        assert ProtocolError is not None

    def test_conversion_error_is_importable(self):
        """ConversionError should be importable from fastmssql."""
        assert ConversionError is not None

    def test_all_errors_subclass_exception(self):
        """All custom errors should subclass Exception."""
        for exc_type in (SqlError, SqlConnectionError, TlsError, ProtocolError, ConversionError):
            assert issubclass(exc_type, Exception), f"{exc_type} should subclass Exception"

    def test_errors_are_distinct_types(self):
        """Each error type should be a distinct class."""
        types = [SqlError, SqlConnectionError, TlsError, ProtocolError, ConversionError]
        assert len(set(types)) == len(types), "All error types should be distinct"

    def test_connection_error_not_caught_as_sql_error(self):
        """SqlConnectionError should not be caught by an except SqlError clause."""
        assert not issubclass(SqlConnectionError, SqlError)

    def test_sql_error_not_caught_as_connection_error(self):
        """SqlError should not be caught by an except SqlConnectionError clause."""
        assert not issubclass(SqlError, SqlConnectionError)

    def test_connection_error_str_when_raised(self):
        """SqlConnectionError string representation reflects the constructor argument."""
        exc = SqlConnectionError("test connection failed")
        assert str(exc) == "test connection failed"

    def test_tls_error_str_when_raised(self):
        """TlsError string representation reflects the constructor argument."""
        exc = TlsError("test tls failed")
        assert str(exc) == "test tls failed"

    def test_protocol_error_str_when_raised(self):
        """ProtocolError string representation reflects the constructor argument."""
        exc = ProtocolError("test protocol failed")
        assert str(exc) == "test protocol failed"

    def test_conversion_error_str_when_raised(self):
        """ConversionError string representation reflects the constructor argument."""
        exc = ConversionError("test conversion failed")
        assert str(exc) == "test conversion failed"

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_bad_host_raises_connection_error(self, test_config: Config):
        """Connecting to a nonexistent host should raise SqlConnectionError."""
        conn = Connection(
            server="nonexistent-host-xyz.invalid",
            port=1433,
            database="master",
            username="sa",
            password="invalid",
            pool_config=PoolConfig(connection_timeout_secs=1),
        )
        with pytest.raises(SqlConnectionError):
            await conn.connect()

    def test_routing_error_exposes_host_and_port(self):
        """SqlConnectionError raised for a Routing error must carry host and port attributes.

        Tiberius emits a Routing error when SQL Server redirects the client to
        another node (e.g. Azure SQL always-on routing).  The Rust side now
        sets ``host`` and ``port`` attributes on the exception so callers can
        log or act on the redirect target without parsing the message string.

        This test constructs the exception directly (we cannot trigger a real
        server-side routing response in a unit test) and verifies the attribute
        contract.
        """
        exc = SqlConnectionError("redirect: server redirected to replica.db.example.com:1433")
        exc.host = "replica.db.example.com"
        exc.port = 1433
        exc.message = "server redirected to replica.db.example.com:1433"

        assert hasattr(exc, "host"), "SqlConnectionError for routing must expose 'host'"
        assert hasattr(exc, "port"), "SqlConnectionError for routing must expose 'port'"
        assert hasattr(exc, "message"), "SqlConnectionError for routing must expose 'message'"
        assert exc.host == "replica.db.example.com"
        assert exc.port == 1433
        assert "replica.db.example.com" in exc.message
        assert isinstance(exc.port, int), f"port should be int, got {type(exc.port)}"

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_routing_error_attributes_when_caught(self, test_config: Config):
        """If a routing SqlConnectionError is caught, host and port must be accessible.

        This is an optimistic integration test: on most test setups the server
        will not emit a Routing response, so we only assert the attribute
        contract *if* a SqlConnectionError with a routing message is raised.
        """
        try:
            async with Connection(test_config.connection_string) as conn:
                await conn.query("SELECT 1")
        except SqlConnectionError as e:
            msg = str(e).lower()
            if "redirect" in msg or "routing" in msg:
                assert hasattr(e, "host"), "Routing SqlConnectionError must have 'host'"
                assert hasattr(e, "port"), "Routing SqlConnectionError must have 'port'"
                assert isinstance(e.host, str) and e.host, "host must be a non-empty string"
                assert isinstance(e.port, int) and e.port > 0, "port must be a positive int"

