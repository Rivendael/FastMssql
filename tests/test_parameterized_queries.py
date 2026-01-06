"""
Test parameterized queries functionality
"""

import pytest
from conftest import Config

try:
    from fastmssql import Connection
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simple_parameterized_query(test_config: Config):
    """Test executing a simple parameterized query."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT @P1 + @P2 as sum_result", [10, 5])
            assert result.has_rows(), "Query should return rows"
            rows = result.rows()
            assert len(rows) == 1
            sum_result = rows[0]["sum_result"]
            assert sum_result == 15
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parameter_types(test_config: Config):
    """Test different parameter types."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query(
                """
                SELECT 
                    @P1 as string_param,
                    @P2 as int_param,
                    @P3 as float_param,
                    @P4 as bool_param,
                    @P5 as null_param
            """,
                ["test string", 42, 3.14159, True, None],
            )

            assert result.has_rows(), "Query should return rows"
            rows = result.rows()
            assert len(rows) == 1
            row = rows[0]

            assert row["string_param"] == "test string"
            assert row["int_param"] == 42
            assert abs(row["float_param"] - 3.14159) < 0.00001
            assert row["bool_param"]
            assert row["null_param"] is None
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_string_sql_injection_protection(test_config: Config):
    """Test that parameterized queries protect against SQL injection."""
    try:
        async with Connection(test_config.connection_string) as conn:
            # This should be safe from SQL injection
            malicious_input = "'; DROP TABLE users; --"

            result = await conn.query("SELECT @P1 as safe_string", [malicious_input])

            assert result.has_rows(), "Query should return rows"
            rows = result.rows()
            assert len(rows) == 1
            assert rows[0]["safe_string"] == malicious_input
    except Exception as e:
        pytest.fail(f"Database not available: {e}")
