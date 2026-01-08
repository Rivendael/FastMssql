
import pytest
from fastmssql import Transaction


@pytest.mark.integration
@pytest.mark.asyncio
async def test_double_begin_raises(test_config):
    t = Transaction(test_config.connection_string)
    await t.begin()
    with pytest.raises(RuntimeError, match="Transaction has already begun"):
        await t.begin()
    await t.rollback()
    await t.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_commit_without_begin_raises(test_config):
    t = Transaction(test_config.connection_string)
    with pytest.raises(RuntimeError, match="Transaction has not begun"):
        await t.commit()
    await t.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_rollback_without_begin_raises(test_config):
    t = Transaction(test_config.connection_string)
    with pytest.raises(RuntimeError, match="Transaction has not begun"):
        await t.rollback()
    await t.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_double_commit_raises(test_config):
    t = Transaction(test_config.connection_string)
    await t.begin()
    await t.commit()
    with pytest.raises(RuntimeError, match="Transaction has already been committed"):
        await t.commit()
    await t.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_double_rollback_raises(test_config):
    t = Transaction(test_config.connection_string)
    await t.begin()
    await t.rollback()
    with pytest.raises(RuntimeError, match="Transaction has already been rolled back"):
        await t.rollback()
    await t.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_commit_after_rollback_raises(test_config):
    t = Transaction(test_config.connection_string)
    await t.begin()
    await t.rollback()
    with pytest.raises(RuntimeError, match="Transaction has already been rolled back"):
        await t.commit()
    await t.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_rollback_after_commit_raises(test_config):
    t = Transaction(test_config.connection_string)
    await t.begin()
    await t.commit()
    with pytest.raises(RuntimeError, match="Transaction has already been committed"):
        await t.rollback()
    await t.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_reuse(test_config):
    t = Transaction(test_config.connection_string)
    # First transaction
    await t.begin()
    await t.commit()
    # Should be able to reuse after commit
    await t.begin()
    await t.rollback()
    # Should be able to reuse after rollback
    await t.begin()
    await t.commit()
    await t.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_prevents_double_begin(test_config):
    """Test that __aenter__ (which calls begin) prevents manual begin()."""
    async with Transaction(test_config.connection_string) as t:
        with pytest.raises(RuntimeError, match="Transaction has already begun"):
            await t.begin()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_context_manager_reuse(test_config):
    """Test that Transaction object can be reused with context manager."""
    t = Transaction(test_config.connection_string)
    
    # First context manager usage
    async with t:
        result = await t.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1
    
    # Should be able to reuse in another context manager
    async with t:
        result = await t.query("SELECT 2 as val")
        assert result.rows()[0]["val"] == 2
    
    await t.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_manual_commit_in_context_manager(test_config):
    """Test that manual commit within context manager prevents auto-commit."""
    t = Transaction(test_config.connection_string)
    
    async with t:
        await t.query("SELECT 1")
        await t.commit()
        # __aexit__ should handle already-committed transaction gracefully
    
    await t.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_manual_rollback_in_context_manager(test_config):
    """Test that manual rollback within context manager prevents auto-commit."""
    t = Transaction(test_config.connection_string)
    
    async with t:
        await t.query("SELECT 1")
        await t.rollback()
        # __aexit__ should handle already-rolled-back transaction gracefully
    
    await t.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_close_allows_reuse(test_config):
    """Test that close() resets flags allowing reuse."""
    t = Transaction(test_config.connection_string)
    
    await t.begin()
    await t.commit()
    await t.close()
    
    # After close, should be able to begin again
    await t.begin()
    await t.rollback()
    await t.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_begin_after_context_manager_exit(test_config):
    """Test that begin() works after context manager exits."""
    t = Transaction(test_config.connection_string)
    
    async with t:
        await t.query("SELECT 1")
    # Context manager should have committed and reset flags
    
    # Should be able to begin manually after context manager
    await t.begin()
    await t.commit()
    await t.close()