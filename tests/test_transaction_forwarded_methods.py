"""Tests for Transaction methods that were previously missing from the Python wrapper:
execute_batch, query_batch, and is_connected (finding #13).
"""

import pytest

from fastmssql import Transaction


# ---------------------------------------------------------------------------
# is_connected
# ---------------------------------------------------------------------------

def test_is_connected_before_connect(test_config):
    """is_connected returns False before any database call has been made."""
    t = Transaction(test_config.connection_string)
    assert t.is_connected() is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_is_connected_after_query(test_config):
    """is_connected returns True once a query has established the connection."""
    t = Transaction(test_config.connection_string)
    try:
        await t.begin()
        assert t.is_connected() is True
    finally:
        await t.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_is_connected_after_close(test_config):
    """is_connected returns False after the connection has been closed."""
    t = Transaction(test_config.connection_string)
    await t.begin()
    await t.rollback()
    await t.close()
    assert t.is_connected() is False


# ---------------------------------------------------------------------------
# execute_batch
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_batch_returns_row_counts(test_config):
    """execute_batch forwards to _rust_conn and returns a list of affected-row counts."""
    t = Transaction(test_config.connection_string)
    try:
        await t.begin()
        results = await t.execute_batch([
            ("SELECT 1", None),
            ("SELECT 2", None),
        ])
        # execute() on SELECT returns 0 affected rows; the important thing is we
        # get back a list of the right length without AttributeError.
        assert isinstance(results, list)
        assert len(results) == 2
        await t.commit()
    finally:
        await t.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_execute_batch_attribute_accessible(test_config):
    """Calling execute_batch does not raise AttributeError (regression guard)."""
    t = Transaction(test_config.connection_string)
    assert callable(getattr(t, "execute_batch", None)), (
        "Transaction.execute_batch is missing — finding #13 regression"
    )
    await t.close()


# ---------------------------------------------------------------------------
# query_batch
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_batch_returns_results(test_config):
    """query_batch forwards to _rust_conn and returns a list of QueryStream objects."""
    t = Transaction(test_config.connection_string)
    try:
        await t.begin()
        results = await t.query_batch([
            ("SELECT 1 AS val", None),
            ("SELECT 2 AS val", None),
        ])
        assert isinstance(results, list)
        assert len(results) == 2
        first_row = list(results[0])[0]
        assert first_row["val"] == 1
        second_row = list(results[1])[0]
        assert second_row["val"] == 2
        await t.commit()
    finally:
        await t.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_query_batch_attribute_accessible(test_config):
    """Calling query_batch does not raise AttributeError (regression guard)."""
    t = Transaction(test_config.connection_string)
    assert callable(getattr(t, "query_batch", None)), (
        "Transaction.query_batch is missing — finding #13 regression"
    )
    await t.close()
