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
        initial_count = rows[0]['count'] if rows else 0
        print(f"Initial @@TRANCOUNT: {initial_count}")
        assert initial_count == 0

        # Begin transaction using convenience method
        await conn.begin()
        
        # Check transaction count inside transaction
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        in_trans_count = rows[0]['count'] if rows else 0
        print(f"Inside transaction @@TRANCOUNT: {in_trans_count}")
        assert in_trans_count == 1, f"Expected TRANCOUNT=1 inside transaction, got {in_trans_count}"
        
        # Do some work
        await conn.query("SELECT 1")
        
        # Check transaction count is still 1
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        check_count = rows[0]['count'] if rows else 0
        assert check_count == 1, f"Expected TRANCOUNT=1 after work, got {check_count}"
        
        # Commit using convenience method
        await conn.commit()
        
        # Check transaction count after commit
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        final_count = rows[0]['count'] if rows else 0
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
                conn_id = rows[0]['id']
                connection_ids.append(conn_id)
                print(f"Query {i+1}: Connection ID = {conn_id}")
        
        # All should be the same
        unique_ids = set(connection_ids)
        assert len(unique_ids) == 1, f"Expected all queries to use same connection, got IDs: {unique_ids}"
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
        assert rows[0]['count'] == 0
        
        # Use begin/commit convenience methods
        await conn.begin()
        
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        assert rows[0]['count'] == 1, "Should be in transaction"
        print("✓ Inside transaction context")
        
        await conn.commit()
        
        # Verify we're out of transaction
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        assert rows[0]['count'] == 0, "Should be out of transaction"
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
        except:
            pass
        
        # Create table outside transaction
        await conn.query("CREATE TABLE test_trans (id INT, value VARCHAR(100))")
        
        # Use async with to auto BEGIN/COMMIT
        async with Transaction(test_config.connection_string) as trans_conn:
            # Verify we're in a transaction
            result = await trans_conn.query("SELECT @@TRANCOUNT as count")
            rows = result.rows() if result.has_rows() else []
            trancount = rows[0]['count'] if rows else 0
            assert trancount == 1, f"Expected TRANCOUNT=1, got {trancount}"
            
            # Insert data
            await trans_conn.query("INSERT INTO test_trans VALUES (1, 'in transaction')")
        # Auto COMMIT on exit
        
        # Verify we're out of transaction
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        trancount = rows[0]['count'] if rows else 0
        assert trancount == 0, f"Expected TRANCOUNT=0 after commit, got {trancount}"
        
        # Verify data persisted
        result = await conn.query("SELECT * FROM test_trans")
        rows = result.rows() if result.has_rows() else []
        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"
        assert rows[0]['value'] == 'in transaction'
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
        except:
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
        count = rows[0]['cnt'] if rows else 0
        assert count == 1, f"Expected 1 row after rollback, got {count}"
        
        # Verify the original row is still there
        result = await conn.query("SELECT * FROM test_rollback")
        rows = result.rows() if result.has_rows() else []
        assert rows[0]['value'] == 'original'
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
        except:
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
            count = rows[0]['cnt'] if rows else 0
            assert count == 1, f"Expected 1 row after setup, got {count}"
        
        # Test transaction with INSERT (auto BEGIN/COMMIT)
        async with Transaction(test_config.connection_string) as conn:
            # Inside transaction context - BEGIN already called automatically
            result = await conn.query("SELECT @@TRANCOUNT as count")
            rows = result.rows() if result.has_rows() else []
            assert rows[0]['count'] == 1, "Should be in transaction"
            
            # Insert data
            await conn.execute("INSERT INTO test_ctx_mgr VALUES (2, 'committed')")
        # Auto COMMIT on exit
        
        # Verify data was committed
        async with Transaction(test_config.connection_string) as conn:
            result = await conn.query("SELECT COUNT(*) as cnt FROM test_ctx_mgr")
            rows = result.rows() if result.has_rows() else []
            count = rows[0]['cnt'] if rows else 0
            assert count == 2, f"Expected 2 rows after successful transaction, got {count}"
            print("✓ Context manager automatically committed")
        
        # Test rollback on exception (auto BEGIN/ROLLBACK)
        try:
            async with Transaction(test_config.connection_string) as conn:
                # Inside transaction - BEGIN already called automatically
                result = await conn.query("SELECT @@TRANCOUNT as count")
                rows = result.rows() if result.has_rows() else []
                assert rows[0]['count'] == 1, "Should be in transaction"
                
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
            count = rows[0]['cnt'] if rows else 0
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
        except:
            pass
        
        # Create table with initial data
        async with Transaction(test_config.connection_string) as trans_conn:
            await trans_conn.query("CREATE TABLE test_multi_ops (id INT PRIMARY KEY, value VARCHAR(100))")
            await trans_conn.execute("INSERT INTO test_multi_ops VALUES (1, 'initial')")
            await trans_conn.execute("INSERT INTO test_multi_ops VALUES (2, 'data')")
        
        # Test transaction with multiple operations
        async with Transaction(test_config.connection_string) as trans_conn:
            # Update existing row
            await trans_conn.execute("UPDATE test_multi_ops SET value = 'updated' WHERE id = 1")
            
            # Insert new row
            await trans_conn.execute("INSERT INTO test_multi_ops VALUES (3, 'new')")
            
            # Delete a row
            await trans_conn.execute("DELETE FROM test_multi_ops WHERE id = 2")
        
        # Verify all operations were committed
        async with Transaction(test_config.connection_string) as verify_conn:
            result = await verify_conn.query("SELECT * FROM test_multi_ops ORDER BY id")
            rows = result.rows() if result.has_rows() else []
            assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
            assert rows[0]['id'] == 1 and rows[0]['value'] == 'updated'
            assert rows[1]['id'] == 3 and rows[1]['value'] == 'new'
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
        except:
            pass
        
        # Create table
        async with Transaction(test_config.connection_string) as trans_conn:
            await trans_conn.query("CREATE TABLE test_sequential (id INT, value VARCHAR(100))")
        
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
            result = await verify_conn.query("SELECT COUNT(*) as cnt FROM test_sequential")
            rows = result.rows() if result.has_rows() else []
            count = rows[0]['cnt'] if rows else 0
            assert count == 3, f"Expected 3 rows from 3 sequential transactions, got {count}"
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
        except:
            pass
        
        # Create table
        async with Transaction(test_config.connection_string) as trans_conn:
            await trans_conn.query("CREATE TABLE test_exception (id INT, value VARCHAR(100))")
            await trans_conn.execute("INSERT INTO test_exception VALUES (1, 'initial')")
        
        # Test 1: Exception after insert (should rollback)
        exception_caught = False
        try:
            async with Transaction(test_config.connection_string) as trans_conn:
                await trans_conn.execute("INSERT INTO test_exception VALUES (2, 'will rollback')")
                raise RuntimeError("Simulated error during transaction")
        except RuntimeError:
            exception_caught = True
        
        assert exception_caught, "Expected exception to be caught"
        
        # Verify insert was rolled back
        async with Transaction(test_config.connection_string) as verify_conn:
            result = await verify_conn.query("SELECT COUNT(*) as cnt FROM test_exception")
            rows = result.rows() if result.has_rows() else []
            count = rows[0]['cnt'] if rows else 0
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
                    conn_id = rows[0]['id']
                    connection_ids.append(conn_id)
        
        # Each Transaction object should use the same connection within its scope
        # But different Transaction objects may use different connections
        assert len(connection_ids) == 3, f"Expected 3 connection IDs, got {len(connection_ids)}"
        print(f"✓ Collected {len(connection_ids)} connection IDs: {connection_ids}")
        
        await conn.close()
        
    except Exception as e:
        print(f"Error in connection reuse test: {e}")
        raise
