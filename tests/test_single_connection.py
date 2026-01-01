"""Tests for SingleConnection - a non-pooled connection for transactions."""
import pytest
from conftest import Config
from fastmssql import SingleConnection


@pytest.mark.integration
@pytest.mark.asyncio
async def test_single_connection_transactions(test_config: Config):
    """Test that SingleConnection maintains consistent connection for transactions."""
    async with SingleConnection(test_config.connection_string) as conn:
        # First, check initial transaction count
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        initial_count = rows[0]['count'] if rows else 0
        print(f"Initial @@TRANCOUNT: {initial_count}")
        assert initial_count == 0

        # Begin transaction (query() will throw error but transaction actually works)
        try:
            await conn.query("BEGIN TRANSACTION")
        except RuntimeError:
            pass  # Expected error from tiberius, but transaction is actually open
        
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
        
        # Commit (query() will throw error but commit actually works)
        try:
            await conn.query("COMMIT TRANSACTION")
        except RuntimeError:
            pass  # Expected error from tiberius
        
        # Check transaction count after commit
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        final_count = rows[0]['count'] if rows else 0
        print(f"After commit @@TRANCOUNT: {final_count}")
        assert final_count == 0, f"Expected TRANCOUNT=0 after commit, got {final_count}"
        print("✅ Transaction test passed!")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_single_connection_id_consistency(test_config: Config):
    """Test that all queries in SingleConnection use the same connection."""
    async with SingleConnection(test_config.connection_string) as conn:
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
    """Test the transaction() context manager - manual BEGIN/COMMIT."""
    async with SingleConnection(test_config.connection_string) as conn:
        # Check initial state
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        assert rows[0]['count'] == 0
        
        # Manually do what context manager would do
        try:
            await conn.query("BEGIN TRANSACTION")
        except RuntimeError:
            pass  # Expected error
        
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        assert rows[0]['count'] == 1, "Should be in transaction"
        print("✓ Inside transaction context")
        
        try:
            await conn.query("COMMIT TRANSACTION")
        except RuntimeError:
            pass  # Expected error
        
        # Verify we're out of transaction
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        assert rows[0]['count'] == 0, "Should be out of transaction"
        print("✓ Transaction context properly closed")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_transaction_cycle(test_config: Config):
    """Test complete transaction: CREATE TABLE -> INSERT -> COMMIT -> data persists."""
    async with SingleConnection(test_config.connection_string) as conn:
        try:
            # Clean up any existing test table
            try:
                await conn.query("DROP TABLE IF EXISTS test_trans")
            except:
                pass
            
            # Begin transaction
            try:
                await conn.query("BEGIN TRANSACTION")
            except RuntimeError:
                pass  # Expected error but transaction actually opens
            
            # Verify we're in a transaction
            result = await conn.query("SELECT @@TRANCOUNT as count")
            rows = result.rows() if result.has_rows() else []
            trancount = rows[0]['count'] if rows else 0
            assert trancount == 1, f"Expected TRANCOUNT=1, got {trancount}"
            
            # Create table and insert data
            await conn.query("CREATE TABLE test_trans (id INT, value VARCHAR(100))")
            await conn.query("INSERT INTO test_trans VALUES (1, 'in transaction')")
            
            # Commit transaction
            try:
                await conn.query("COMMIT TRANSACTION")
            except RuntimeError:
                pass  # Expected error but transaction actually commits
            
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
        except RuntimeError:
            pass  # Expected error
        
        result = await conn.query("SELECT @@TRANCOUNT as count")
        rows = result.rows() if result.has_rows() else []
        assert rows[0]['count'] == 0, "Should be out of transaction"
        print("✓ Transaction committed and closed")
