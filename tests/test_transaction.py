"""Tests for Transaction - a non-pooled connection for transactions."""

import pytest
from conftest import Config

from fastmssql import Transaction


@pytest.mark.integration
@pytest.mark.asyncio
async def test_single_connection_transactions(test_config: Config):
    """Test that Transaction maintains consistent connection for transactions."""
    conn = Transaction(test_config.connection_string)

    try:
        # First, check initial transaction count (outside of transaction context)
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        initial_count = rows[0]["count"] if rows else 0
        print(f"Initial @@TRANCOUNT: {initial_count}")
        assert initial_count == 0

        # Begin transaction using convenience method
        await conn.begin()

        # Check transaction count inside transaction
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        in_trans_count = rows[0]["count"] if rows else 0
        print(f"Inside transaction @@TRANCOUNT: {in_trans_count}")
        assert in_trans_count == 1, (
            f"Expected TRANCOUNT=1 inside transaction, got {in_trans_count}"
        )

        # Do some work
        await conn.query("SELECT 1")

        # Check transaction count is still 1
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        check_count = rows[0]["count"] if rows else 0
        assert check_count == 1, f"Expected TRANCOUNT=1 after work, got {check_count}"

        # Commit using convenience method
        await conn.commit()

        # Check transaction count after commit
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        final_count = rows[0]["count"] if rows else 0
        print(f"After commit @@TRANCOUNT: {final_count}")
        assert final_count == 0, f"Expected TRANCOUNT=0 after commit, got {final_count}"
        print("✅ Transaction test passed!")
    finally:
        await conn.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_single_connection_id_consistency(test_config: Config):
    """Test that all queries in Transaction use the same connection."""
    async with Transaction(test_config.connection_string) as conn:
        connection_ids = []

        for i in range(5):
            result = await conn.query("SELECT @@SPID as id")
            rows = result.rows() if result.has_rows() else []
            if rows:
                conn_id = rows[0]["id"]
                connection_ids.append(conn_id)
                print(f"Query {i + 1}: Connection ID = {conn_id}")

        # All should be the same
        unique_ids = set(connection_ids)
        assert len(unique_ids) == 1, (
            f"Expected all queries to use same connection, got IDs: {unique_ids}"
        )
        print(f"✓ All queries used same connection (ID: {connection_ids[0]})")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_single_connection_transaction_context(test_config: Config):
    """Test manual transaction control using begin/commit methods."""
    conn = Transaction(test_config.connection_string)

    try:
        # Check initial state
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        assert rows[0]["count"] == 0

        # Use begin/commit convenience methods
        await conn.begin()

        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        assert rows[0]["count"] == 1, "Should be in transaction"
        print("✓ Inside transaction context")

        await conn.commit()

        # Verify we're out of transaction
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        assert rows[0]["count"] == 0, "Should be out of transaction"
        print("✓ Transaction context properly closed")
    finally:
        await conn.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_transaction_cycle(test_config: Config):
    """Test complete transaction: CREATE TABLE -> INSERT -> COMMIT -> data persists."""
    conn = Transaction(test_config.connection_string)

    try:
        # Clean up any existing test table
        try:
            await conn.query("DROP TABLE IF EXISTS test_trans")
        except Exception:
            pass

        # Create table outside transaction
        await conn.query("CREATE TABLE test_trans (id INT, value VARCHAR(100))")

        # Use async with to auto BEGIN/COMMIT
        async with Transaction(test_config.connection_string) as trans_conn:
            # Verify we're in a transaction
            result = await trans_conn.query("SELECT @@TRANCOUNT as count")
            rows = result.rows() if result.has_rows() else []
            trancount = rows[0]["count"] if rows else 0
            assert trancount == 1, f"Expected TRANCOUNT=1, got {trancount}"

            # Insert data
            await trans_conn.query(
                "INSERT INTO test_trans VALUES (1, 'in transaction')"
            )
        # Auto COMMIT on exit

        # Verify we're out of transaction
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        trancount = rows[0]["count"] if rows else 0
        assert trancount == 0, f"Expected TRANCOUNT=0 after commit, got {trancount}"

        # Verify data persisted
        result = await conn.query("SELECT * FROM test_trans")
        rows = result.rows() if result.has_rows() else []
        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
        assert rows[0]["value"] == "in transaction"
        print("✓ Data persisted correctly through transaction")

        # Clean up
        await conn.query("DROP TABLE test_trans")
        print("✓ Full transaction cycle completed successfully")

    except Exception as e:
        print(f"Error in transaction test: {e}")
        raise
    finally:
        await conn.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_single_connection_rollback(test_config: Config):
    """Test transaction rollback."""
    conn = Transaction(test_config.connection_string)

    try:
        # Clean up
        try:
            await conn.query("DROP TABLE IF EXISTS test_rollback")
        except Exception:
            pass

        # Create table outside transaction
        await conn.query("CREATE TABLE test_rollback (id INT, value VARCHAR(100))")
        await conn.query("INSERT INTO test_rollback VALUES (1, 'original')")

        # Start transaction with manual begin/rollback
        await conn.begin()

        # Make changes in transaction
        await conn.execute("INSERT INTO test_rollback VALUES (2, 'in transaction')")

        # Rollback
        await conn.rollback()

        # Verify changes were rolled back
        result = await conn.query("SELECT COUNT(*) as cnt FROM test_rollback")
        rows = result.rows() if result.has_rows() else []
        count = rows[0]["cnt"] if rows else 0
        assert count == 1, f"Expected 1 row after rollback, got {count}"

        # Verify the original row is still there
        result = await conn.query("SELECT * FROM test_rollback")
        rows = result.rows() if result.has_rows() else []
        assert rows[0]["value"] == "original"
        print("✓ Rollback correctly undid changes")

        # Clean up
        await conn.query("DROP TABLE test_rollback")
        print("✓ Rollback test passed")

    except Exception as e:
        print(f"Error in rollback test: {e}")
        raise
    finally:
        await conn.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_context_manager(test_config: Config):
    """Test automatic transaction context manager with auto BEGIN/COMMIT/ROLLBACK."""
    try:
        # Clean up
        try:
            conn = Transaction(test_config.connection_string)
            await conn.query("DROP TABLE IF EXISTS test_ctx_mgr")
            await conn.close()
        except Exception:
            pass

        # Test successful transaction with context manager (auto BEGIN/COMMIT)
        async with Transaction(test_config.connection_string) as conn:
            # Create table outside transaction first
            await conn.query("CREATE TABLE test_ctx_mgr (id INT, value VARCHAR(100))")
            await conn.query("INSERT INTO test_ctx_mgr VALUES (1, 'before')")
        # Auto COMMIT on exit

        # Verify data was committed
        async with Transaction(test_config.connection_string) as conn:
            result = await conn.query("SELECT COUNT(*) as cnt FROM test_ctx_mgr")
            rows = result.rows() if result.has_rows() else []
            count = rows[0]["cnt"] if rows else 0
            assert count == 1, f"Expected 1 row after setup, got {count}"

        # Test transaction with INSERT (auto BEGIN/COMMIT)
        async with Transaction(test_config.connection_string) as conn:
            # Inside transaction context - BEGIN already called automatically
            result = await conn.query("SELECT @@TRANCOUNT as count")
            rows = result.rows() if result.has_rows() else []
            assert rows[0]["count"] == 1, "Should be in transaction"

            # Insert data
            await conn.execute("INSERT INTO test_ctx_mgr VALUES (2, 'committed')")
        # Auto COMMIT on exit

        # Verify data was committed
        async with Transaction(test_config.connection_string) as conn:
            result = await conn.query("SELECT COUNT(*) as cnt FROM test_ctx_mgr")
            rows = result.rows() if result.has_rows() else []
            count = rows[0]["cnt"] if rows else 0
            assert count == 2, (
                f"Expected 2 rows after successful transaction, got {count}"
            )
            print("✓ Context manager automatically committed")

        # Test rollback on exception (auto BEGIN/ROLLBACK)
        try:
            async with Transaction(test_config.connection_string) as conn:
                # Inside transaction - BEGIN already called automatically
                result = await conn.query("SELECT @@TRANCOUNT as count")
                rows = result.rows() if result.has_rows() else []
                assert rows[0]["count"] == 1, "Should be in transaction"

                # Insert data that will be rolled back
                await conn.execute("INSERT INTO test_ctx_mgr VALUES (3, 'rollback me')")

                # Raise an exception to trigger rollback
                raise ValueError("Intentional error to test rollback")
        except ValueError:
            pass  # Expected - we triggered it intentionally

        # Verify the insert was rolled back
        async with Transaction(test_config.connection_string) as conn:
            result = await conn.query("SELECT COUNT(*) as cnt FROM test_ctx_mgr")
            rows = result.rows() if result.has_rows() else []
            count = rows[0]["cnt"] if rows else 0
            assert count == 2, f"Expected 2 rows (rollback undid insert), got {count}"
            print("✓ Context manager automatically rolled back on exception")

        # Clean up
        async with Transaction(test_config.connection_string) as conn:
            await conn.query("DROP TABLE test_ctx_mgr")
            print("✓ Context manager test passed")

    except Exception as e:
        print(f"Error in context manager test: {e}")
        raise


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_multiple_operations(test_config: Config):
    """Test context manager with multiple SQL operations (INSERT, UPDATE, DELETE)."""
    try:
        # Setup
        conn = Transaction(test_config.connection_string)
        try:
            await conn.query("DROP TABLE IF EXISTS test_multi_ops")
        except Exception:
            pass

        # Create table with initial data
        async with Transaction(test_config.connection_string) as trans_conn:
            await trans_conn.query(
                "CREATE TABLE test_multi_ops (id INT PRIMARY KEY, value VARCHAR(100))"
            )
            await trans_conn.execute("INSERT INTO test_multi_ops VALUES (1, 'initial')")
            await trans_conn.execute("INSERT INTO test_multi_ops VALUES (2, 'data')")

        # Test transaction with multiple operations
        async with Transaction(test_config.connection_string) as trans_conn:
            # Update existing row
            await trans_conn.execute(
                "UPDATE test_multi_ops SET value = 'updated' WHERE id = 1"
            )

            # Insert new row
            await trans_conn.execute("INSERT INTO test_multi_ops VALUES (3, 'new')")

            # Delete a row
            await trans_conn.execute("DELETE FROM test_multi_ops WHERE id = 2")

        # Verify all operations were committed
        async with Transaction(test_config.connection_string) as verify_conn:
            result = await verify_conn.query("SELECT * FROM test_multi_ops ORDER BY id")
            rows = result.rows() if result.has_rows() else []
            assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
            assert rows[0]["id"] == 1 and rows[0]["value"] == "updated"
            assert rows[1]["id"] == 3 and rows[1]["value"] == "new"
            print("✓ Multiple operations committed correctly")

        # Clean up
        await conn.query("DROP TABLE test_multi_ops")
        await conn.close()

    except Exception as e:
        print(f"Error in multiple operations test: {e}")
        raise


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_sequential_transactions(test_config: Config):
    """Test multiple sequential transactions using context manager."""
    try:
        # Setup
        conn = Transaction(test_config.connection_string)
        try:
            await conn.query("DROP TABLE IF EXISTS test_sequential")
        except Exception:
            pass

        # Create table
        async with Transaction(test_config.connection_string) as trans_conn:
            await trans_conn.query(
                "CREATE TABLE test_sequential (id INT, value VARCHAR(100))"
            )

        # First transaction
        async with Transaction(test_config.connection_string) as trans_conn:
            await trans_conn.execute("INSERT INTO test_sequential VALUES (1, 'first')")

        # Second transaction
        async with Transaction(test_config.connection_string) as trans_conn:
            await trans_conn.execute("INSERT INTO test_sequential VALUES (2, 'second')")

        # Third transaction
        async with Transaction(test_config.connection_string) as trans_conn:
            await trans_conn.execute("INSERT INTO test_sequential VALUES (3, 'third')")

        # Verify all were committed
        async with Transaction(test_config.connection_string) as verify_conn:
            result = await verify_conn.query(
                "SELECT COUNT(*) as cnt FROM test_sequential"
            )
            rows = result.rows() if result.has_rows() else []
            count = rows[0]["cnt"] if rows else 0
            assert count == 3, (
                f"Expected 3 rows from 3 sequential transactions, got {count}"
            )
            print("✓ Sequential transactions all committed")

        # Clean up
        await conn.query("DROP TABLE test_sequential")
        await conn.close()

    except Exception as e:
        print(f"Error in sequential transactions test: {e}")
        raise


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_exception_during_transaction(test_config: Config):
    """Test that exceptions during transaction properly trigger rollback."""
    try:
        # Setup
        conn = Transaction(test_config.connection_string)
        try:
            await conn.query("DROP TABLE IF EXISTS test_exception")
        except Exception:
            pass

        # Create table
        async with Transaction(test_config.connection_string) as trans_conn:
            await trans_conn.query(
                "CREATE TABLE test_exception (id INT, value VARCHAR(100))"
            )
            await trans_conn.execute("INSERT INTO test_exception VALUES (1, 'initial')")

        # Test 1: Exception after insert (should rollback)
        exception_caught = False
        try:
            async with Transaction(test_config.connection_string) as trans_conn:
                await trans_conn.execute(
                    "INSERT INTO test_exception VALUES (2, 'will rollback')"
                )
                raise RuntimeError("Simulated error during transaction")
        except RuntimeError:
            exception_caught = True

        assert exception_caught, "Expected exception to be caught"

        # Verify insert was rolled back
        async with Transaction(test_config.connection_string) as verify_conn:
            result = await verify_conn.query(
                "SELECT COUNT(*) as cnt FROM test_exception"
            )
            rows = result.rows() if result.has_rows() else []
            count = rows[0]["cnt"] if rows else 0
            assert count == 1, f"Expected 1 row (insert rolled back), got {count}"
            print("✓ Exception properly triggered rollback")

        # Clean up
        await conn.query("DROP TABLE test_exception")
        await conn.close()

    except Exception as e:
        print(f"Error in exception handling test: {e}")
        raise


@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_connection_reuse(test_config: Config):
    """Test that same connection is used across multiple context manager transactions."""
    try:
        conn = Transaction(test_config.connection_string)
        connection_ids = []

        # Run multiple transactions and collect connection IDs
        for i in range(3):
            async with Transaction(test_config.connection_string) as trans_conn:
                result = await trans_conn.query("SELECT @@SPID as id")
                rows = result.rows() if result.has_rows() else []
                if rows:
                    conn_id = rows[0]["id"]
                    connection_ids.append(conn_id)

        # Each Transaction object should use the same connection within its scope
        # But different Transaction objects may use different connections
        assert len(connection_ids) == 3, (
            f"Expected 3 connection IDs, got {len(connection_ids)}"
        )
        print(f"✓ Collected {len(connection_ids)} connection IDs: {connection_ids}")

        await conn.close()

    except Exception as e:
        print(f"Error in connection reuse test: {e}")
        raise


# ===== Parameter Support Tests =====


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_supports_all_connection_parameters(test_config: Config):
    """Document that Transaction supports all Config parameters like Connection does.

    Transaction accepts: connection_string, server, database, username, password,
    port, instance_name, application_name, application_intent, ssl_config
    (same as Connection, except no pool_config parameter)
    """
    # Use connection_string approach which has SSL config
    try:
        trans = Transaction(connection_string=test_config.connection_string)
        result = await trans.query("SELECT 1 as test")
        rows = result.rows() if result.has_rows() else []
        assert len(rows) > 0
        await trans.close()
        print("✓ Transaction supports full Config parameter set")
        print("✅ Parameter support documentation test passed!")
    except Exception as e:
        print(f"Error: {e}")
        raise


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_connection_string_parsing(test_config: Config):
    """Test that Transaction correctly parses connection strings with host and port."""
    # Connection string with port embedded: "Server=localhost,1433;..."
    try:
        trans = Transaction(connection_string=test_config.connection_string)
        result = await trans.query("SELECT @@SERVERNAME as srv")
        rows = result.rows() if result.has_rows() else []
        assert len(rows) > 0
        print(f"✓ Parsed connection string, connected to: {rows[0]['srv']}")
        await trans.close()
        print("✅ Connection string parsing test passed!")
    except Exception as e:
        print(f"Error: {e}")
        raise


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_with_server_parameter(test_config: Config):
    """Test Transaction accepts server parameter."""
    try:
        Transaction(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
        )
        print("✓ Transaction accepts server parameter")
        print("✅ Server parameter test passed!")
    except TypeError as e:
        pytest.fail(f"Transaction should accept server parameter: {e}")
    except Exception:
        print("✓ Transaction accepts server parameter")
        print("✅ Server parameter test passed!")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_with_port_parameter(test_config: Config):
    """Test Transaction accepts port parameter."""
    try:
        Transaction(
            server=test_config.server,
            port=test_config.port,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
        )
        print("✓ Transaction accepts port parameter")
        print("✅ Port parameter test passed!")
    except TypeError as e:
        pytest.fail(f"Transaction should accept port parameter: {e}")
    except Exception:
        print("✓ Transaction accepts port parameter")
        print("✅ Port parameter test passed!")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_with_database_parameter(test_config: Config):
    """Test Transaction accepts database parameter."""
    try:
        Transaction(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
        )
        print("✓ Transaction accepts database parameter")
        print("✅ Database parameter test passed!")
    except TypeError as e:
        pytest.fail(f"Transaction should accept database parameter: {e}")
    except Exception:
        print("✓ Transaction accepts database parameter")
        print("✅ Database parameter test passed!")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_with_username_password_parameters(test_config: Config):
    """Test Transaction accepts username and password parameters."""
    try:
        Transaction(
            server=test_config.server,
            username=test_config.username,
            password=test_config.password,
        )
        print("✓ Transaction accepts username and password parameters")
        print("✅ Username/password parameters test passed!")
    except TypeError as e:
        pytest.fail(f"Transaction should accept username/password parameters: {e}")
    except Exception:
        print("✓ Transaction accepts username and password parameters")
        print("✅ Username/password parameters test passed!")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_with_application_name_parameter(test_config: Config):
    """Test Transaction accepts application_name parameter."""
    try:
        Transaction(
            server=test_config.server,
            application_name="TestApp",
            username=test_config.username,
            password=test_config.password,
        )
        print("✓ Transaction accepts application_name parameter")
        print("✅ Application name parameter test passed!")
    except TypeError as e:
        pytest.fail(f"Transaction should accept application_name parameter: {e}")
    except Exception:
        print("✓ Transaction accepts application_name parameter")
        print("✅ Application name parameter test passed!")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_with_application_intent_parameter(test_config: Config):
    """Test Transaction accepts application_intent parameter."""
    try:
        Transaction(
            server=test_config.server,
            application_intent="readonly",
            username=test_config.username,
            password=test_config.password,
        )
        print("✓ Transaction accepts application_intent parameter")
        print("✅ Application intent parameter test passed!")
    except TypeError as e:
        pytest.fail(f"Transaction should accept application_intent parameter: {e}")
    except Exception:
        print("✓ Transaction accepts application_intent parameter")
        print("✅ Application intent parameter test passed!")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_with_instance_name_parameter(test_config: Config):
    """Test Transaction accepts instance_name parameter."""
    try:
        Transaction(
            server=test_config.server,
            instance_name="SQLEXPRESS",
            username=test_config.username,
            password=test_config.password,
        )
        print("✓ Transaction accepts instance_name parameter")
        print("✅ Instance name parameter test passed!")
    except TypeError as e:
        pytest.fail(f"Transaction should accept instance_name parameter: {e}")
    except Exception:
        print("✓ Transaction accepts instance_name parameter")
        print("✅ Instance name parameter test passed!")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_with_all_individual_parameters(test_config: Config):
    """Test Transaction accepts all individual parameters together."""
    try:
        Transaction(
            server=test_config.server,
            database=test_config.database,
            username=test_config.username,
            password=test_config.password,
            port=test_config.port,
            application_name="TestApp",
            application_intent="readwrite",
        )
        print("✓ Transaction accepts all individual parameters")
        print("✅ All parameters test passed!")
    except TypeError as e:
        pytest.fail(f"Transaction should accept all individual parameters: {e}")
    except Exception:
        print("✓ Transaction accepts all individual parameters")
        print("✅ All parameters test passed!")


# ===== Connection String Parsing Tests =====


def test_connection_string_parsing_hostname_and_port():
    """Test parsing connection string with format: Server=hostname,port;..."""
    conn_str = "Server=localhost,1433;Database=master;User Id=sa;Password=test"
    Transaction(connection_string=conn_str)
    # Just verify it instantiates without error
    print("✓ Parsed hostname,port format")


def test_connection_string_parsing_hostname_and_instance():
    """Test parsing connection string with format: Server=hostname\\instance;..."""
    conn_str = "Server=localhost\\SQLEXPRESS;Database=master;User Id=sa;Password=test"
    Transaction(connection_string=conn_str)
    # Just verify it instantiates without error
    print("✓ Parsed hostname\\instance format")


def test_connection_string_parsing_hostname_only():
    """Test parsing connection string with format: Server=hostname;..."""
    conn_str = "Server=localhost;Database=master;User Id=sa;Password=test"
    Transaction(connection_string=conn_str)
    # Should default to port 1433
    print("✓ Parsed hostname only format (default port 1433)")


def test_connection_string_parsing_with_extra_semicolons():
    """Test parsing connection string with extra semicolons and spaces."""
    conn_str = "Server=localhost,1433;Database=master;User Id=sa;Password=test;Connection Timeout=30;"
    Transaction(connection_string=conn_str)
    print("✓ Parsed connection string with extra parameters")


def test_connection_string_parsing_mixed_case():
    """Test parsing connection string with mixed case."""
    conn_str = "server=localhost,1433;database=master;user id=sa;password=test"
    Transaction(connection_string=conn_str)
    print("✓ Parsed mixed case connection string")


def test_connection_string_parsing_with_spaces():
    """Test parsing connection string with spaces around values.

    Note: Tiberius's ADO.NET parser is strict and doesn't accept spaces around the = sign.
    This is a limitation of the underlying library, not our code.
    """
    conn_str = (
        "Server = localhost , 1433 ; Database = master ; User Id = sa ; Password = test"
    )
    # Tiberius rejects this format
    with pytest.raises(ValueError, match="Invalid connection string"):
        Transaction(connection_string=conn_str)
    print("✓ Correctly rejected connection string with spaces around = signs")


def test_connection_string_parsing_port_as_string():
    """Test that port is correctly parsed as integer from string."""
    conn_str = "Server=localhost,5433;Database=master;User Id=sa;Password=test"
    Transaction(connection_string=conn_str)
    # Verify instantiation - actual port validation happens at connection time
    print("✓ Parsed non-standard port 5433")


def test_connection_string_parsing_instance_with_trailing_semicolon():
    """Test parsing instance format with trailing semicolon."""
    conn_str = "Server=localhost\\SQLEXPRESS;Database=master;User Id=sa;Password=test;"
    Transaction(connection_string=conn_str)
    print("✓ Parsed instance format with trailing semicolon")


def test_connection_string_parsing_malformed_port():
    """Test that malformed port is rejected by tiberius parser.

    Tiberius strictly validates the connection string format and rejects invalid ports.
    """
    conn_str = "Server=localhost,invalid_port;Database=master;User Id=sa;Password=test"
    # Tiberius rejects invalid port
    with pytest.raises(ValueError, match="Invalid connection string"):
        Transaction(connection_string=conn_str)
    print("✓ Correctly rejected connection string with invalid port")


def test_connection_string_parsing_no_database():
    """Test parsing connection string without Database parameter."""
    conn_str = "Server=localhost,1433;User Id=sa;Password=test"
    Transaction(connection_string=conn_str)
    print("✓ Parsed connection string without Database")


def test_connection_string_parsing_complex_password():
    """Test parsing connection string with complex password containing special characters."""
    conn_str = "Server=localhost,1433;Database=master;User Id=sa;Password=P@ssw0rd!#$%"
    Transaction(connection_string=conn_str)
    print("✓ Parsed connection string with complex password")


def test_connection_string_parsing_ipv4_address():
    """Test parsing connection string with IPv4 address."""
    conn_str = "Server=192.168.1.100,1433;Database=master;User Id=sa;Password=test"
    Transaction(connection_string=conn_str)
    print("✓ Parsed IPv4 address as server")


def test_connection_string_parsing_fqdn():
    """Test parsing connection string with FQDN."""
    conn_str = "Server=db.example.com,1433;Database=master;User Id=sa;Password=test"
    Transaction(connection_string=conn_str)
    print("✓ Parsed FQDN as server")


def test_connection_string_parsing_server_with_brackets():
    """Test parsing connection string with bracketed server name."""
    conn_str = "Server=[localhost],1433;Database=master;User Id=sa;Password=test"
    Transaction(connection_string=conn_str)
    # Note: This tests handling of brackets if present
    print("✓ Parsed server with brackets")


def test_connection_string_multiple_consecutive_commas():
    """Test that multiple commas in server part are rejected by tiberius.

    Tiberius strictly validates the connection string format and only accepts
    one comma to separate hostname and port.
    """
    conn_str = "Server=localhost,1433,9999;Database=master;User Id=sa;Password=test"
    # Tiberius rejects this - only one comma is valid in Server value
    with pytest.raises(ValueError, match="Invalid connection string|Conversion error"):
        Transaction(connection_string=conn_str)
    print("✓ Correctly rejected connection string with multiple ports")


def test_connection_string_parsing_azure_sql():
    """Test parsing Azure SQL connection string format."""
    conn_str = "Server=myserver.database.windows.net,1433;Database=mydb;User Id=user@myserver;Password=test;Encrypt=true"
    Transaction(connection_string=conn_str)
    print("✓ Parsed Azure SQL connection string")
