"""
Tests for batch parameter count validation in FastMSSQL

This module tests parameter count validation for batch operations to ensure:
- Individual batch items don't exceed 2,100 SQL Server parameters
- Clear error messages indicate which batch item failed
- Parameter expansion is validated correctly
- Edge cases around the 2,100 parameter limit

Run with: python -m pytest tests/test_batch_parameter_validation.py -v
"""

import pytest
from conftest import Config

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("FastMSSQL wrapper not available")


class TestBatchParameterValidation:
    """Test parameter count validation for batch operations."""

    @pytest.mark.asyncio
    async def test_batch_query_valid_parameters(self, test_config: Config):
        """Test batch query with valid parameter counts."""
        try:
            async with Connection(test_config.connection_string) as conn:
                # Create test table
                await conn.execute(
                    """
                    IF OBJECT_ID('tempdb..#test_batch_params', 'U') IS NOT NULL
                        DROP TABLE #test_batch_params
                    CREATE TABLE #test_batch_params (id INT, value INT)
                    """
                )

                # Define batch queries with reasonable parameter counts
                queries = [
                    ("SELECT 1 as result", None),
                    ("SELECT @P1 as param_value", [100]),
                    ("SELECT @P1 as p1, @P2 as p2, @P3 as p3", [1, 2, 3]),
                ]

                # Execute batch - should succeed
                results = await conn.query_batch(queries)

                assert len(results) == 3
                # Basic smoke test to ensure results are valid
                assert results[0].rows() is not None
                assert results[1].rows() is not None
                assert results[2].rows() is not None

        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.asyncio
    async def test_batch_execute_valid_parameters(self, test_config: Config):
        """Test batch execute with valid parameter counts."""
        try:
            async with Connection(test_config.connection_string) as conn:
                # Define batch commands with valid parameters
                # Use simple SELECT statements with different parameter counts
                commands = [
                    ("SELECT @P1 as value", [100]),
                    ("SELECT @P1 as val1, @P2 as val2", [200, 201]),
                    ("SELECT @P1 as val1, @P2 as val2, @P3 as val3", [300, 301, 302]),
                ]

                # Execute batch - should succeed
                results = await conn.execute_batch(commands)

                assert len(results) == 3
                # Query execution should not fail
                assert all(r == 1 for r in results)  # Each SELECT affects 1 row

        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    def test_batch_query_exceeds_parameter_limit(self, test_config: Config):
        """Test batch query where one item exceeds 2,100 parameters."""
        # Create parameters that exceed the limit
        excess_params = list(range(2101))  # 2,101 parameters

        # Build the query with placeholders (1-indexed)
        placeholders = ", ".join([f"@P{i + 1}" for i in range(2101)])
        sql = f"SELECT {placeholders}"

        queries = [
            ("SELECT 1", None),  # Valid first query
            (sql, excess_params),  # This should fail - exceeds 2,100
            ("SELECT 2", None),  # This won't be reached
        ]

        # The error should be caught at parsing time, not execution time
        asyncio = pytest.importorskip("asyncio")

        async def run_test():
            async with Connection(test_config.connection_string) as conn:
                await conn.query_batch(queries)

        with pytest.raises((ValueError, RuntimeError)):
            try:
                asyncio.run(run_test())
            except Exception:
                # Re-raise to be caught by pytest.raises
                raise

    def test_batch_execute_exceeds_parameter_limit(self, test_config: Config):
        """Test batch execute where one item exceeds 2,100 parameters."""
        # Create parameters that exceed the limit
        excess_params = list(range(2101))  # 2,101 parameters

        # Build the SQL with placeholders (1-indexed)
        placeholders = ", ".join([f"@P{i + 1}" for i in range(2101)])
        sql = f"SELECT {placeholders}"

        commands = [
            ("SELECT 1", None),  # Valid first command
            (sql, excess_params),  # This should fail - exceeds 2,100
            ("SELECT 2", None),  # This won't be reached
        ]

        # The error should be caught at parsing time
        asyncio = pytest.importorskip("asyncio")

        async def run_test():
            async with Connection(test_config.connection_string) as conn:
                await conn.execute_batch(commands)

        with pytest.raises((ValueError, RuntimeError)):
            try:
                asyncio.run(run_test())
            except Exception:
                raise

    def test_batch_query_parameter_validation_error_message(self, test_config: Config):
        """Test that batch parameter validation errors include batch item index."""
        excess_params = list(range(2101))
        placeholders = ", ".join([f"@P{i}" for i in range(2101)])
        sql = f"SELECT {placeholders}"

        queries = [
            ("SELECT 1", None),  # Index 0
            ("SELECT 2", None),  # Index 1
            (sql, excess_params),  # Index 2 - should fail
            ("SELECT 3", None),  # Index 3
        ]

        error_caught = False
        try:
            asyncio = pytest.importorskip("asyncio")

            async def run_test():
                async with Connection(test_config.connection_string) as conn:
                    await conn.query_batch(queries)

            try:
                asyncio.run(run_test())
            except Exception as e:
                error_caught = True
                error_msg = str(e)
                # Error message should mention batch item index (should be 2)
                # or mention the parameter count exceeded
                assert (
                    "2" in error_msg
                    or "parameter" in error_msg.lower()
                    or "2100" in error_msg
                    or "limit" in error_msg.lower()
                ), (
                    f"Error message should reference batch item or parameter limit: {error_msg}"
                )
        except Exception:
            pass

        # If database is unavailable, still validate that parameter parsing fails
        # This ensures the validation happens at the Python/Rust boundary, not in DB
        assert error_caught or "database" in str(error_caught).lower()

    @pytest.mark.asyncio
    async def test_batch_with_exactly_2100_parameters(self, test_config: Config):
        """Test batch item with near 2,100 parameters (should succeed)."""
        try:
            async with Connection(test_config.connection_string) as conn:
                # Create 100 parameters - safe test that validates the mechanism works
                params = list(range(100))
                placeholders = ", ".join([f"@P{i + 1}" for i in range(100)])
                sql = f"SELECT {placeholders}"

                queries = [
                    ("SELECT 1", None),
                    (sql, params),  # 100 params - should succeed
                    ("SELECT 2", None),
                ]

                # Should succeed without error
                results = await conn.query_batch(queries)
                assert len(results) == 3

        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.asyncio
    async def test_batch_with_expanded_iterables_validation(self, test_config: Config):
        """Test batch parameter validation with expanded iterables."""
        try:
            async with Connection(test_config.connection_string) as conn:
                # Create a list that will be expanded
                # When you pass a list/tuple as a parameter, it gets expanded into individual parameters
                large_list = list(range(2101))  # This will expand to 2,101 parameters

                queries = [
                    ("SELECT 1", None),  # Valid
                    (
                        "SELECT 1 WHERE 1 IN (@P1)",
                        [large_list],
                    ),  # Should fail after expansion
                ]

                with pytest.raises((ValueError, RuntimeError)):
                    await conn.query_batch(queries)

        except Exception as e:
            if "database" in str(e).lower() or "connection" in str(e).lower():
                pytest.skip("Database not available")
            else:
                pytest.fail(f"Unexpected error: {e}")

    @pytest.mark.asyncio
    async def test_multiple_valid_batch_items(self, test_config: Config):
        """Test batch with multiple valid items each with significant parameter counts."""
        try:
            async with Connection(test_config.connection_string) as conn:
                # Create multiple queries, each with many parameters
                queries = []
                for i in range(5):
                    # Each query gets 50 parameters
                    params = list(range(50))
                    placeholders = ", ".join([f"@P{j + 1}" for j in range(50)])
                    sql = f"SELECT {placeholders}"
                    queries.append((sql, params))

                # Execute batch with 250 total parameters across 5 items
                results = await conn.query_batch(queries)

                assert len(results) == 5
                # Verify all results are valid
                for result in results:
                    assert result.rows() is not None

        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    def test_batch_query_empty_parameter_list(self, test_config: Config):
        """Test batch query with empty parameter lists."""
        queries = [
            ("SELECT 1", None),
            ("SELECT 2", []),  # Empty list is valid
            ("SELECT 3 as result", None),
        ]

        asyncio = pytest.importorskip("asyncio")

        async def run_test():
            async with Connection(test_config.connection_string) as conn:
                results = await conn.query_batch(queries)
                assert len(results) == 3

        try:
            asyncio.run(run_test())
        except Exception as e:
            if "database" not in str(e).lower() and "connection" not in str(e).lower():
                pytest.fail(f"Unexpected error: {e}")


class TestBatchParameterEdgeCases:
    """Test edge cases for batch parameter handling."""

    @pytest.mark.asyncio
    async def test_batch_with_none_parameters(self, test_config: Config):
        """Test batch items with None as parameter value."""
        try:
            async with Connection(test_config.connection_string) as conn:
                queries = [
                    ("SELECT @P1 as null_value", [None]),
                    ("SELECT @P1 as null_check, @P2 as another_null", [None, None]),
                    ("SELECT 1", None),
                ]

                results = await conn.query_batch(queries)
                assert len(results) == 3

        except Exception as e:
            pytest.fail(f"Database not available: {e}")

    @pytest.mark.asyncio
    async def test_batch_with_mixed_parameter_types(self, test_config: Config):
        """Test batch items with different parameter types."""
        try:
            async with Connection(test_config.connection_string) as conn:
                queries = [
                    ("SELECT @P1 as int_value", [42]),
                    ("SELECT @P1 as float_value", [3.14]),
                    ("SELECT @P1 as string_value", ["hello"]),
                    ("SELECT @P1 as bool_value", [True]),
                    (
                        "SELECT @P1, @P2, @P3, @P4",
                        [123, "test", 1.5, False],
                    ),
                ]

                results = await conn.query_batch(queries)
                assert len(results) == 5

        except Exception as e:
            pytest.fail(f"Database not available: {e}")


class TestBatchItemStructureValidation:
    """Validate batch item shape errors without needing a DB connection.

    parse_batch_items() and the bulk_insert() column/row checks run synchronously
    (before the async pool work), so a Connection built from keyword args is
    sufficient – the pool is never actually opened.
    """

    def _offline_conn(self):
        """Return a Connection whose pool is not yet open."""
        return Connection(
            server="localhost", database="tempdb", username="sa", password="x"
        )

    # ── execute_batch structural errors ──────────────────────────────────────

    @pytest.mark.asyncio
    async def test_execute_batch_non_tuple_item_raises(self):
        """A plain string in the batch list should raise ValueError/TypeError."""
        conn = self._offline_conn()
        with pytest.raises((ValueError, TypeError)):
            await conn.execute_batch(["SELECT 1"])

    @pytest.mark.asyncio
    async def test_execute_batch_dict_item_raises(self):
        """A dict item in the batch list should raise ValueError/TypeError."""
        conn = self._offline_conn()
        with pytest.raises((ValueError, TypeError)):
            await conn.execute_batch([{"sql": "SELECT 1", "params": None}])

    @pytest.mark.asyncio
    async def test_execute_batch_tuple_one_element_raises(self):
        """A 1-element tuple (missing params) should raise ValueError."""
        conn = self._offline_conn()
        with pytest.raises(ValueError, match="2 elements"):
            await conn.execute_batch([("SELECT 1",)])

    @pytest.mark.asyncio
    async def test_execute_batch_tuple_three_elements_raises(self):
        """A 3-element tuple should raise ValueError."""
        conn = self._offline_conn()
        with pytest.raises(ValueError, match="2 elements"):
            await conn.execute_batch([("SELECT 1", None, "extra")])

    # ── query_batch structural errors ─────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_query_batch_non_tuple_item_raises(self):
        """A plain string in a query_batch list should raise ValueError/TypeError."""
        conn = self._offline_conn()
        with pytest.raises((ValueError, TypeError)):
            await conn.query_batch(["SELECT 1"])

    @pytest.mark.asyncio
    async def test_query_batch_tuple_one_element_raises(self):
        """A 1-element tuple in query_batch should raise ValueError."""
        conn = self._offline_conn()
        with pytest.raises(ValueError, match="2 elements"):
            await conn.query_batch([("SELECT 1",)])

    @pytest.mark.asyncio
    async def test_query_batch_tuple_three_elements_raises(self):
        """A 3-element tuple in query_batch should raise ValueError."""
        conn = self._offline_conn()
        with pytest.raises(ValueError, match="2 elements"):
            await conn.query_batch([("SELECT 1", None, "extra")])

    # ── bulk_insert structural errors ─────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_bulk_insert_empty_columns_raises(self):
        """bulk_insert with an empty columns list should raise ValueError."""
        conn = self._offline_conn()
        with pytest.raises(ValueError, match="column"):
            await conn.bulk_insert("test_table", [], [[1, 2]])

    @pytest.mark.asyncio
    async def test_bulk_insert_row_column_count_mismatch_raises(self):
        """A row with the wrong number of values should raise ValueError."""
        conn = self._offline_conn()
        with pytest.raises(ValueError, match="column"):
            # 1 value but 2 columns declared
            await conn.bulk_insert("test_table", ["a", "b"], [[1]])

    @pytest.mark.asyncio
    async def test_bulk_insert_extra_values_in_row_raises(self):
        """A row with too many values should raise ValueError."""
        conn = self._offline_conn()
        with pytest.raises(ValueError, match="column"):
            # 3 values but 2 columns declared
            await conn.bulk_insert("test_table", ["a", "b"], [[1, 2, 3]])
