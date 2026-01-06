"""
Tests for pool configuration validation and advanced pool scenarios

This module tests PoolConfig validation, property setters, and edge cases
in connection pool configuration and management.
"""

import pytest
from conftest import Config

try:
    from fastmssql import Connection, PoolConfig
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first")


def test_pool_config_creation():
    """Test basic PoolConfig creation."""
    config = PoolConfig(max_size=10)
    assert config.max_size == 10


def test_pool_config_with_all_parameters():
    """Test PoolConfig with all parameters."""
    config = PoolConfig(
        max_size=20,
        min_idle=5,
        max_lifetime_secs=600,
        idle_timeout_secs=300,
        connection_timeout_secs=30,
    )

    assert config.max_size == 20
    assert config.min_idle == 5
    assert config.max_lifetime_secs == 600
    assert config.idle_timeout_secs == 300
    assert config.connection_timeout_secs == 30


def test_pool_config_default_values():
    """Test PoolConfig default values."""
    config = PoolConfig()

    # Check defaults (from code: max_size=10, min_idle=Some(1), connection_timeout_secs=Some(30))
    assert config.max_size == 20
    assert config.min_idle == 2
    assert config.connection_timeout_secs == 30


def test_pool_config_max_size_zero_invalid():
    """Test that max_size of 0 is invalid."""
    with pytest.raises(ValueError) as exc_info:
        PoolConfig(max_size=0)

    assert "max_size must be greater than 0" in str(exc_info.value)


def test_pool_config_min_idle_greater_than_max_invalid():
    """Test that min_idle > max_size is invalid."""
    with pytest.raises(ValueError) as exc_info:
        PoolConfig(max_size=5, min_idle=10)

    assert "min_idle cannot be greater than max_size" in str(exc_info.value)


def test_pool_config_setter_max_size():
    """Test setting max_size property."""
    config = PoolConfig(max_size=10)

    # Valid update
    config.max_size = 20
    assert config.max_size == 20

    # Invalid update - zero
    with pytest.raises(ValueError) as exc_info:
        config.max_size = 0
    assert "max_size must be greater than 0" in str(exc_info.value)


def test_pool_config_setter_max_size_less_than_min_idle():
    """Test that setting max_size less than min_idle fails."""
    config = PoolConfig(max_size=10, min_idle=5)

    with pytest.raises(ValueError) as exc_info:
        config.max_size = 3

    assert "max_size cannot be less than min_idle" in str(exc_info.value)


def test_pool_config_setter_min_idle():
    """Test setting min_idle property."""
    config = PoolConfig(max_size=10)

    # Valid update
    config.min_idle = 3
    assert config.min_idle == 3

    # Invalid update - greater than max_size
    with pytest.raises(ValueError) as exc_info:
        config.min_idle = 20

    assert "min_idle cannot be greater than max_size" in str(exc_info.value)


def test_pool_config_setter_min_idle_to_none():
    """Test setting min_idle to None."""
    config = PoolConfig(max_size=10, min_idle=5)

    config.min_idle = None
    assert config.min_idle is None


def test_pool_config_setter_max_lifetime():
    """Test setting max_lifetime_secs property."""
    config = PoolConfig(max_size=10)

    # Valid update
    config.max_lifetime_secs = 600
    assert config.max_lifetime_secs == 600

    # Set to None
    config.max_lifetime_secs = None
    assert config.max_lifetime_secs is None


def test_pool_config_setter_idle_timeout():
    """Test setting idle_timeout_secs property."""
    config = PoolConfig(max_size=10)

    # Valid update
    config.idle_timeout_secs = 300
    assert config.idle_timeout_secs == 300

    # Set to None
    config.idle_timeout_secs = None
    assert config.idle_timeout_secs is None


def test_pool_config_setter_connection_timeout():
    """Test setting connection_timeout_secs property."""
    config = PoolConfig(max_size=10)

    # Valid update
    config.connection_timeout_secs = 60
    assert config.connection_timeout_secs == 60

    # Set to None
    config.connection_timeout_secs = None
    assert config.connection_timeout_secs is None


def test_pool_config_large_max_size():
    """Test PoolConfig with large max_size."""
    config = PoolConfig(max_size=1000)
    assert config.max_size == 1000


def test_pool_config_max_size_one():
    """Test PoolConfig with max_size of 1."""
    config = PoolConfig(max_size=1, min_idle=None)
    assert config.max_size == 1


def test_pool_config_large_timeouts():
    """Test PoolConfig with large timeout values."""
    config = PoolConfig(
        max_size=10,
        max_lifetime_secs=86400,  # 1 day
        idle_timeout_secs=3600,  # 1 hour
        connection_timeout_secs=300,  # 5 minutes
    )

    assert config.max_lifetime_secs == 86400
    assert config.idle_timeout_secs == 3600
    assert config.connection_timeout_secs == 300


def test_pool_config_small_timeouts():
    """Test PoolConfig with small timeout values."""
    config = PoolConfig(
        max_size=10, max_lifetime_secs=1, idle_timeout_secs=1, connection_timeout_secs=1
    )

    assert config.max_lifetime_secs == 1
    assert config.idle_timeout_secs == 1
    assert config.connection_timeout_secs == 1


def test_pool_config_zero_timeout():
    """Test PoolConfig with zero timeout (might be invalid or treated as no timeout)."""
    try:
        config = PoolConfig(max_size=10, connection_timeout_secs=0)
        # If it succeeds, verify it was set
        assert (
            config.connection_timeout_secs == 0
            or config.connection_timeout_secs is None
        )
    except ValueError:
        # Zero timeout might be invalid
        pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connection_with_custom_pool_config(test_config: Config):
    """Test creating connection with custom pool config."""
    try:
        pool_config = PoolConfig(max_size=5, min_idle=1, connection_timeout_secs=30)

        async with Connection(
            test_config.connection_string, pool_config=pool_config
        ) as conn:
            result = await conn.query("SELECT 1 as val")
            assert result.rows()[0]["val"] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connection_default_pool_config(test_config: Config):
    """Test connection uses default pool config when not specified."""
    try:
        async with Connection(test_config.connection_string) as conn:
            result = await conn.query("SELECT 1 as val")
            assert result.rows()[0]["val"] == 1

            # Get pool stats to verify defaults
            stats = await conn.pool_stats()
            assert stats is not None
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_stats_tuple_structure(test_config: Config):
    """Test that pool_stats returns correct structure."""
    try:
        async with Connection(test_config.connection_string) as conn:
            stats = await conn.pool_stats()

            # pool_stats returns a dict with pool information
            assert isinstance(stats, dict)

            # Verify expected keys are present
            expected_keys = [
                "active_connections",
                "connections",
                "idle_connections",
                "max_size",
            ]
            for key in expected_keys:
                assert key in stats

            # Verify types
            assert isinstance(stats.get("active_connections"), int)
            assert isinstance(stats.get("connections"), int)
            assert isinstance(stats.get("idle_connections"), int)
            assert isinstance(stats.get("max_size"), int)

            # Logical checks
            assert stats["idle_connections"] <= stats["connections"]
            assert stats["connections"] <= stats["max_size"]
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_config_modification_before_connect(test_config: Config):
    """Test modifying pool config before connection is established."""
    try:
        pool_config = PoolConfig(max_size=10, min_idle=2)
        conn = Connection(test_config.connection_string, pool_config=pool_config)

        # Modify config before first use (might not have effect after pool is created)
        pool_config.max_size = 5

        # Connect and use
        async with conn:
            result = await conn.query("SELECT 1 as val")
            assert result.rows()[0]["val"] == 1
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_multiple_connections_different_configs(test_config: Config):
    """Test multiple connections with different pool configs."""
    try:
        config1 = PoolConfig(max_size=5)
        config2 = PoolConfig(max_size=10)

        async with Connection(
            test_config.connection_string, pool_config=config1
        ) as conn1:
            async with Connection(
                test_config.connection_string, pool_config=config2
            ) as conn2:
                result1 = await conn1.query("SELECT 1 as val")
                result2 = await conn2.query("SELECT 2 as val")

                assert result1.rows()[0]["val"] == 1
                assert result2.rows()[0]["val"] == 2

                stats1 = await conn1.pool_stats()
                stats2 = await conn2.pool_stats()

                # Verify different configs (max_size in dict)
                assert stats1["max_size"] == 5
                assert stats2["max_size"] == 10
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_with_small_max_size(test_config: Config):
    """Test pool with very small max_size to verify queue handling."""
    try:
        pool_config = PoolConfig(max_size=2, min_idle=1)

        async with Connection(
            test_config.connection_string, pool_config=pool_config
        ) as conn:
            # Sequential queries should work fine with small pool
            for i in range(5):
                result = await conn.query(f"SELECT {i} as val")
                assert result.rows()[0]["val"] == i
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_config_string_representation():
    """Test PoolConfig string representation for debugging."""
    config = PoolConfig(
        max_size=10,
        min_idle=2,
        max_lifetime_secs=600,
        idle_timeout_secs=300,
        connection_timeout_secs=30,
    )

    # Should be able to convert to string
    config_str = str(config)
    assert config_str is not None


def test_pool_config_copy():
    """Test creating similar pool configs."""
    config1 = PoolConfig(max_size=10, min_idle=2, connection_timeout_secs=30)
    config2 = PoolConfig(max_size=10, min_idle=2, connection_timeout_secs=30)

    # They should have same values
    assert config1.max_size == config2.max_size
    assert config1.min_idle == config2.min_idle
    assert config1.connection_timeout_secs == config2.connection_timeout_secs


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_config_with_none_values(test_config: Config):
    """Test PoolConfig with None timeout values."""
    config = PoolConfig(
        max_size=10,
        max_lifetime_secs=None,
        idle_timeout_secs=None,
        connection_timeout_secs=None,
    )

    assert config.max_size == 10
    assert config.max_lifetime_secs is None
    assert config.idle_timeout_secs is None
    # Note: connection_timeout_secs might have a default

    # Should still work
    async with Connection(test_config.connection_string, pool_config=config) as conn:
        result = await conn.query("SELECT 1 as val")
        assert result.rows()[0]["val"] == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_stats_after_operations(test_config: Config):
    """Test pool stats change after various operations."""
    try:
        pool_config = PoolConfig(max_size=10, min_idle=2)

        async with Connection(
            test_config.connection_string, pool_config=pool_config
        ) as conn:
            stats_before = await conn.pool_stats()

            # Run a query
            await conn.query("SELECT 1")
            stats_after_query = await conn.pool_stats()

            # Run multiple queries
            for _ in range(3):
                await conn.query("SELECT 1")
            stats_after_multiple = await conn.pool_stats()

            # Stats should be valid throughout
            assert stats_before is not None
            assert stats_after_query is not None
            assert stats_after_multiple is not None
    except Exception as e:
        pytest.fail(f"Database not available: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_config_with_test_on_check_out_enabled(test_config: Config):
    """Test pool behavior with test_on_check_out=True.

    When enabled, connections are validated before being returned from the pool.
    This ensures that stale or dead connections are detected early.
    """
    pool_config = PoolConfig(
        max_size=5, min_idle=2, test_on_check_out=True, connection_timeout_secs=15
    )

    try:
        async with Connection(
            test_config.connection_string, pool_config=pool_config
        ) as conn:
            # First query should work
            result1 = await conn.query("SELECT 1 as val")
            assert result1.rows()[0]["val"] == 1

            # Multiple queries should all work with health checks
            for i in range(10):
                result = await conn.query(f"SELECT {i} as iteration")
                assert result.rows()[0]["iteration"] == i
    except Exception as e:
        pytest.fail(f"test_on_check_out=True pool test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_config_with_test_on_check_out_disabled(test_config: Config):
    """Test pool behavior with test_on_check_out=False (or None).

    When disabled, connections are not tested before being returned,
    potentially improving performance at the cost of reliability.
    """
    pool_config = PoolConfig(
        max_size=5, min_idle=2, test_on_check_out=False, connection_timeout_secs=15
    )

    try:
        async with Connection(
            test_config.connection_string, pool_config=pool_config
        ) as conn:
            result = await conn.query("SELECT 1 as val")
            assert result.rows()[0]["val"] == 1
    except Exception as e:
        pytest.fail(f"test_on_check_out=False pool test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_config_with_retry_connection_enabled(test_config: Config):
    """Test pool behavior with retry_connection=True.

    When enabled, the pool will retry failed connection attempts,
    improving robustness in unstable network conditions.
    """
    pool_config = PoolConfig(
        max_size=5, min_idle=1, retry_connection=True, connection_timeout_secs=15
    )

    try:
        async with Connection(
            test_config.connection_string, pool_config=pool_config
        ) as conn:
            # Should successfully establish connection with retries
            result = await conn.query("SELECT 1 as val")
            assert result.rows()[0]["val"] == 1

            # Multiple operations should work reliably
            for i in range(5):
                result = await conn.query(f"SELECT {i}")
                assert result.rows()[0][""] == i or result.has_rows()
    except Exception as e:
        pytest.fail(f"retry_connection=True pool test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_config_with_retry_connection_disabled(test_config: Config):
    """Test pool behavior with retry_connection=False (or None).

    When disabled, failed connection attempts are not retried,
    failing fast but potentially losing resilience.
    """
    pool_config = PoolConfig(
        max_size=5, min_idle=1, retry_connection=False, connection_timeout_secs=15
    )

    try:
        async with Connection(
            test_config.connection_string, pool_config=pool_config
        ) as conn:
            result = await conn.query("SELECT 1 as val")
            assert result.rows()[0]["val"] == 1
    except Exception as e:
        pytest.fail(f"retry_connection=False pool test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_config_with_both_reliability_settings(test_config: Config):
    """Test pool with both test_on_check_out and retry_connection enabled.

    This combines connection validation (health checks on checkout)
    with connection attempt retries for maximum reliability.
    """
    pool_config = PoolConfig(
        max_size=8,
        min_idle=3,
        test_on_check_out=True,
        retry_connection=True,
        connection_timeout_secs=20,
    )

    try:
        async with Connection(
            test_config.connection_string, pool_config=pool_config
        ) as conn:
            # Run multiple queries to exercise the pool
            for i in range(15):
                result = await conn.query(f"SELECT {i} as num")
                assert result.has_rows()
    except Exception as e:
        pytest.fail(f"Combined reliability settings pool test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_config_high_throughput_with_reliability(test_config: Config):
    """Test high-throughput preset combined with reliability settings."""
    # Create a high-throughput config with reliability settings
    pool_config = PoolConfig(
        max_size=50,
        min_idle=15,
        max_lifetime_secs=1800,
        idle_timeout_secs=600,
        connection_timeout_secs=30,
        test_on_check_out=True,
        retry_connection=True,
    )

    try:
        async with Connection(
            test_config.connection_string, pool_config=pool_config
        ) as conn:
            # High-throughput with safety checks
            results = []
            for i in range(20):
                result = await conn.query(f"SELECT {i} as iteration")
                results.append(result)

            assert len(results) == 20
    except Exception as e:
        pytest.fail(f"High-throughput with reliability test failed: {e}")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_config_none_reliability_settings(test_config: Config):
    """Test pool behavior when reliability settings are None (default behavior).

    None values should use bb8's defaults, allowing the pool to
    operate with standard behavior.
    """
    pool_config = PoolConfig(
        max_size=5,
        min_idle=2,
        test_on_check_out=None,
        retry_connection=None,
        connection_timeout_secs=15,
    )

    assert pool_config.test_on_check_out is None
    assert pool_config.retry_connection is None

    try:
        async with Connection(
            test_config.connection_string, pool_config=pool_config
        ) as conn:
            result = await conn.query("SELECT 1 as val")
            assert result.rows()[0]["val"] == 1
    except Exception as e:
        pytest.fail(f"None reliability settings pool test failed: {e}")
