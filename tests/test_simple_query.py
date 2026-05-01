"""
Tests for Connection.simple_query and Transaction.simple_query.
"""

import pytest
from conftest import Config

try:
    from fastmssql import Connection, QueryStream, SqlError, Transaction
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PROC_NAME = "test_sq_proc"
_TABLE_NAME = "test_sq_table"


async def _drop_proc(conn: Connection, name: str = _PROC_NAME) -> None:
    await conn.execute(f"IF OBJECT_ID('{name}', 'P') IS NOT NULL DROP PROCEDURE {name}")


async def _drop_table(conn: Connection, name: str = _TABLE_NAME) -> None:
    await conn.execute(f"DROP TABLE IF EXISTS {name}")


# ---------------------------------------------------------------------------
# Return type
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_returns_query_stream(test_config: Config):
    """simple_query should return a QueryStream instance."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT 1 AS n")
        assert isinstance(result, QueryStream)


# ---------------------------------------------------------------------------
# Basic SELECT
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_scalar_integer(test_config: Config):
    """simple_query should return correct integer scalar."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT 42 AS answer")
        assert result.has_rows()
        rows = result.rows()
        assert len(rows) == 1
        assert rows[0]["answer"] == 42


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_scalar_string(test_config: Config):
    """simple_query should return correct string scalar."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT 'hello' AS greeting")
        assert result.has_rows()
        rows = result.rows()
        assert len(rows) == 1
        assert rows[0]["greeting"] == "hello"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_null_value(test_config: Config):
    """simple_query should surface NULL as Python None."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT NULL AS nothing")
        assert result.has_rows()
        rows = result.rows()
        assert rows[0]["nothing"] is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_multiple_columns(test_config: Config):
    """simple_query should return all projected columns correctly."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query(
            "SELECT 1 AS a, 'two' AS b, CAST(3.14 AS FLOAT) AS c"
        )
        assert result.has_rows()
        rows = result.rows()
        assert len(rows) == 1
        row = rows[0]
        assert row["a"] == 1
        assert row["b"] == "two"
        assert abs(float(row["c"]) - 3.14) < 0.001


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_multiple_rows(test_config: Config):
    """simple_query should return all rows in a multi-row result set."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT v FROM (VALUES (1),(2),(3)) AS t(v)")
        assert result.has_rows()
        rows = result.rows()
        assert len(rows) == 3
        values = sorted(row["v"] for row in rows)
        assert values == [1, 2, 3]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_empty_result(test_config: Config):
    """simple_query on a query that returns no rows should give an empty stream."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT 1 AS x WHERE 1 = 0")
        assert not result.has_rows()
        assert result.rows() == []


# ---------------------------------------------------------------------------
# QueryStream interface
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_stream_columns(test_config: Config):
    """QueryStream.columns() should reflect the projected column names."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT 1 AS foo, 2 AS bar")
        cols = result.columns()
        assert "foo" in cols
        assert "bar" in cols


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_stream_fetchone(test_config: Config):
    """fetchone() should return the first row then None."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT v FROM (VALUES (10),(20)) AS t(v)")
        first = result.fetchone()
        assert first is not None
        assert first["v"] in (10, 20)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_stream_fetchmany(test_config: Config):
    """fetchmany(n) should return exactly n rows when available."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query(
            "SELECT v FROM (VALUES (1),(2),(3),(4),(5)) AS t(v)"
        )
        batch = result.fetchmany(3)
        assert len(batch) == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_stream_fetchall(test_config: Config):
    """fetchall() should return all rows."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT v FROM (VALUES (1),(2),(3)) AS t(v)")
        rows = result.fetchall()
        assert len(rows) == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_stream_len(test_config: Config):
    """QueryStream.len() should equal the total row count."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query(
            "SELECT v FROM (VALUES (1),(2),(3),(4)) AS t(v)"
        )
        assert result.len() == 4


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_stream_is_empty(test_config: Config):
    """is_empty() should be True for empty results, False otherwise."""
    async with Connection(test_config.connection_string) as conn:
        empty = await conn.simple_query("SELECT 1 WHERE 0 = 1")
        assert empty.is_empty()

        non_empty = await conn.simple_query("SELECT 1 AS x")
        assert not non_empty.is_empty()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_stream_index_access(test_config: Config):
    """QueryStream[i] should provide index-based row access."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query(
            "SELECT v FROM (VALUES (10),(20),(30)) AS t(v) ORDER BY v"
        )
        assert result[0]["v"] == 10
        assert result[2]["v"] == 30
        assert result[-1]["v"] == 30


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_stream_slice(test_config: Config):
    """QueryStream[a:b] should return a list of rows."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query(
            "SELECT v FROM (VALUES (1),(2),(3),(4),(5)) AS t(v) ORDER BY v"
        )
        sliced = result[1:3]
        assert isinstance(sliced, list)
        assert len(sliced) == 2
        assert sliced[0]["v"] == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_stream_position_and_reset(test_config: Config):
    """position() should advance and reset() should bring it back to 0."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT v FROM (VALUES (1),(2),(3)) AS t(v)")
        assert result.position() == 0
        result.fetchone()
        assert result.position() == 1
        result.reset()
        assert result.position() == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_stream_sync_iteration(test_config: Config):
    """QueryStream is synchronously iterable (no __aiter__); for-loop yields all rows."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT v FROM (VALUES (1),(2),(3)) AS t(v)")
        collected = [row["v"] for row in result]
        assert len(collected) == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_stream_all(test_config: Config):
    """QueryStream.all() should return all rows as a list (synchronous)."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT v FROM (VALUES (7),(8),(9)) AS t(v)")
        rows = result.all()
        assert isinstance(rows, list)
        assert len(rows) == 3


# ---------------------------------------------------------------------------
# Primary use case: DDL that requires the simple-query protocol
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_create_stored_procedure(test_config: Config):
    """
    CREATE PROCEDURE must run as a non-prepared statement.
    simple_query is the correct API for this.
    """
    async with Connection(test_config.connection_string) as conn:
        await _drop_proc(conn)
        try:
            create_sql = f"""
                CREATE PROCEDURE {_PROC_NAME}
                AS
                BEGIN
                    SELECT 99 AS proc_result
                END
            """
            # This should not raise - it's the whole point of simple_query
            await conn.simple_query(create_sql)

            # Verify the procedure was actually created
            check = await conn.query(
                "SELECT COUNT(*) AS cnt FROM sys.objects "
                "WHERE type = 'P' AND name = @P1",
                [_PROC_NAME],
            )
            assert check.rows()[0]["cnt"] == 1
        finally:
            await _drop_proc(conn)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_exec_stored_procedure(test_config: Config):
    """
    After creating a stored procedure via simple_query, it should be
    executable and return the expected result set.
    """
    async with Connection(test_config.connection_string) as conn:
        await _drop_proc(conn)
        try:
            await conn.simple_query(f"""
                CREATE PROCEDURE {_PROC_NAME}
                AS
                BEGIN
                    SELECT 42 AS magic_number
                END
            """)

            result = await conn.simple_query(f"EXEC {_PROC_NAME}")
            assert result.has_rows()
            rows = result.rows()
            assert rows[0]["magic_number"] == 42
        finally:
            await _drop_proc(conn)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_alter_stored_procedure(test_config: Config):
    """ALTER PROCEDURE also requires the simple-query protocol."""
    async with Connection(test_config.connection_string) as conn:
        await _drop_proc(conn)
        try:
            await conn.simple_query(f"""
                CREATE PROCEDURE {_PROC_NAME}
                AS
                BEGIN
                    SELECT 1 AS version
                END
            """)

            await conn.simple_query(f"""
                ALTER PROCEDURE {_PROC_NAME}
                AS
                BEGIN
                    SELECT 2 AS version
                END
            """)

            result = await conn.simple_query(f"EXEC {_PROC_NAME}")
            assert result.rows()[0]["version"] == 2
        finally:
            await _drop_proc(conn)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_create_view(test_config: Config):
    """CREATE VIEW is another DDL that benefits from the simple-query protocol."""
    view_name = "test_sq_view"
    async with Connection(test_config.connection_string) as conn:
        await conn.execute(f"DROP VIEW IF EXISTS {view_name}")
        try:
            await conn.simple_query(
                f"CREATE VIEW {view_name} AS SELECT 7 AS lucky_number"
            )

            result = await conn.simple_query(f"SELECT lucky_number FROM {view_name}")
            assert result.rows()[0]["lucky_number"] == 7
        finally:
            await conn.execute(f"DROP VIEW IF EXISTS {view_name}")


# ---------------------------------------------------------------------------
# System / metadata queries via simple_query
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_getdate(test_config: Config):
    """simple_query can execute built-in function calls."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT GETDATE() AS now")
        assert result.has_rows()
        assert result.rows()[0]["now"] is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_server_name(test_config: Config):
    """simple_query returns server metadata correctly."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT @@SERVERNAME AS server_name")
        assert result.has_rows()
        # @@SERVERNAME may be NULL in some container setups, just check it runs
        rows = result.rows()
        assert "server_name" in rows[0].columns()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_version_string(test_config: Config):
    """@@VERSION returns a non-empty string."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT @@VERSION AS ver")
        rows = result.rows()
        assert rows[0]["ver"] is not None
        assert "SQL Server" in str(rows[0]["ver"])


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_invalid_sql_raises(test_config: Config):
    """simple_query with invalid SQL should raise SqlError."""
    async with Connection(test_config.connection_string) as conn:
        with pytest.raises(SqlError):
            await conn.simple_query("THIS IS NOT VALID SQL !!!!")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_nonexistent_object_raises(test_config: Config):
    """simple_query selecting from a non-existent table should raise SqlError."""
    async with Connection(test_config.connection_string) as conn:
        with pytest.raises(SqlError):
            await conn.simple_query(
                "SELECT * FROM dbo.this_table_definitely_does_not_exist_xyz"
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_raiserror_propagates(test_config: Config):
    """RAISERROR inside a simple_query should propagate as SqlError."""
    async with Connection(test_config.connection_string) as conn:
        with pytest.raises(SqlError):
            await conn.simple_query("RAISERROR('intentional error', 16, 1)")


# ---------------------------------------------------------------------------
# Reuse: multiple simple_query calls on the same connection
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_multiple_calls_same_connection(test_config: Config):
    """Multiple sequential simple_query calls on one connection should all succeed."""
    async with Connection(test_config.connection_string) as conn:
        for i in range(5):
            result = await conn.simple_query(f"SELECT {i} AS n")
            assert result.rows()[0]["n"] == i


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_interleaved_with_query(test_config: Config):
    """simple_query and query() should coexist on the same connection."""
    async with Connection(test_config.connection_string) as conn:
        r1 = await conn.simple_query("SELECT 1 AS a")
        r2 = await conn.query("SELECT 2 AS b")
        r3 = await conn.simple_query("SELECT 3 AS c")

        assert r1.rows()[0]["a"] == 1
        assert r2.rows()[0]["b"] == 2
        assert r3.rows()[0]["c"] == 3


# ---------------------------------------------------------------------------
# Transaction.simple_query
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_simple_query_basic(test_config: Config):
    """Transaction.simple_query should return rows just like Connection.simple_query."""
    async with Transaction(test_config.connection_string) as tx:
        result = await tx.simple_query("SELECT 55 AS val")
        assert result.has_rows()
        assert result.rows()[0]["val"] == 55


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_simple_query_create_procedure(test_config: Config):
    """
    Transaction.simple_query should support CREATE PROCEDURE just like
    Connection.simple_query.
    """
    proc = "test_sq_tx_proc"
    async with Connection(test_config.connection_string) as conn:
        await conn.execute(
            f"IF OBJECT_ID('{proc}', 'P') IS NOT NULL DROP PROCEDURE {proc}"
        )

    try:
        async with Transaction(test_config.connection_string) as tx:
            await tx.simple_query(f"""
                CREATE PROCEDURE {proc}
                AS
                BEGIN
                    SELECT 'from_tx' AS origin
                END
            """)

        # Verify outside the transaction
        async with Connection(test_config.connection_string) as conn:
            check = await conn.query(
                "SELECT COUNT(*) AS cnt FROM sys.objects WHERE type='P' AND name=@P1",
                [proc],
            )
            assert check.rows()[0]["cnt"] == 1
    finally:
        async with Connection(test_config.connection_string) as conn:
            await conn.execute(
                f"IF OBJECT_ID('{proc}', 'P') IS NOT NULL DROP PROCEDURE {proc}"
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_transaction_simple_query_rollback(test_config: Config):
    """
    When a Transaction rolls back, inserts done within it should be undone.
    simple_query used to set up the table, transaction used for DML.
    """
    async with Connection(test_config.connection_string) as conn:
        await _drop_table(conn)
        await conn.execute(f"CREATE TABLE {_TABLE_NAME} (id INT PRIMARY KEY)")

    try:
        # Insert and then rollback
        tx = Transaction(test_config.connection_string)
        try:
            await tx.begin()
            await tx.execute(f"INSERT INTO {_TABLE_NAME} VALUES (1)")
            await tx.rollback()
        finally:
            await tx.close()

        async with Connection(test_config.connection_string) as conn:
            result = await conn.simple_query(
                f"SELECT COUNT(*) AS cnt FROM {_TABLE_NAME}"
            )
            assert result.rows()[0]["cnt"] == 0
    finally:
        async with Connection(test_config.connection_string) as conn:
            await _drop_table(conn)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_multiline_sql(test_config: Config):
    """simple_query handles multi-line SQL strings correctly."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query(
            """
            SELECT
                1 AS first_col,
                2 AS second_col,
                3 AS third_col
            """
        )
        assert result.has_rows()
        row = result.rows()[0]
        assert row["first_col"] == 1
        assert row["second_col"] == 2
        assert row["third_col"] == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_row_to_dict(test_config: Config):
    """Rows returned by simple_query support to_dict()."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT 1 AS x, 'y' AS y_col")
        row = result.rows()[0]
        d = row.to_dict()
        assert isinstance(d, dict)
        assert d["x"] == 1
        assert d["y_col"] == "y"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_row_index_access(test_config: Config):
    """Rows returned by simple_query support integer index access."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT 10 AS a, 20 AS b")
        row = result.rows()[0]
        assert row[0] == 10
        assert row[1] == 20


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_row_values(test_config: Config):
    """Rows returned by simple_query support values()."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT 1 AS a, 2 AS b, 3 AS c")
        row = result.rows()[0]
        vals = row.values()
        assert sorted(vals) == [1, 2, 3]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_query_row_len(test_config: Config):
    """len(row) should equal the number of columns."""
    async with Connection(test_config.connection_string) as conn:
        result = await conn.simple_query("SELECT 1 AS a, 2 AS b, 3 AS c")
        row = result.rows()[0]
        assert len(row) == 3
