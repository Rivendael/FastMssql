import pytest

try:
    from fastmssql import PoolConfig
except ImportError:
    raise pytest.skip("fastmssql module not available - run 'maturin develop' first")


@pytest.mark.skipif(PoolConfig is None, reason="fastmssql module not available")
class TestPoolConfigConstructor:
    """Test PoolConfig constructor and basic validation."""

    def test_default_constructor(self):
        """Test creating PoolConfig with default values."""
        config = PoolConfig()
        assert config.max_size == 20
        assert config.min_idle == 2
        assert config.max_lifetime_secs is None
        assert config.idle_timeout_secs is None
        assert config.connection_timeout_secs == 30

    def test_custom_values(self):
        """Test creating PoolConfig with custom values."""
        config = PoolConfig(
            max_size=20,
            min_idle=5,
            max_lifetime_secs=1800,
            idle_timeout_secs=600,
            connection_timeout_secs=45,
        )
        assert config.max_size == 20
        assert config.min_idle == 5
        assert config.max_lifetime_secs == 1800
        assert config.idle_timeout_secs == 600
        assert config.connection_timeout_secs == 45

    def test_none_values(self):
        """Test creating PoolConfig with None values."""
        config = PoolConfig(
            max_size=15,
            min_idle=None,
            max_lifetime_secs=None,
            idle_timeout_secs=None,
            connection_timeout_secs=None,
        )
        assert config.max_size == 15
        assert config.min_idle is None
        assert config.max_lifetime_secs is None
        assert config.idle_timeout_secs is None
        assert config.connection_timeout_secs is None

    def test_max_size_zero_invalid(self):
        """Test that max_size of 0 raises an error."""
        with pytest.raises(ValueError, match="max_size must be greater than 0"):
            PoolConfig(max_size=0)

    def test_min_idle_greater_than_max_size_invalid(self):
        """Test that min_idle > max_size raises an error."""
        with pytest.raises(
            ValueError, match="min_idle cannot be greater than max_size"
        ):
            PoolConfig(max_size=10, min_idle=15)

    def test_min_idle_equal_max_size(self):
        """Test that min_idle can equal max_size."""
        config = PoolConfig(max_size=10, min_idle=10)
        assert config.max_size == 10
        assert config.min_idle == 10

    def test_min_idle_zero(self):
        """Test that min_idle can be 0."""
        config = PoolConfig(max_size=10, min_idle=0)
        assert config.min_idle == 0


@pytest.mark.skipif(PoolConfig is None, reason="fastmssql module not available")
class TestPoolConfigGetters:
    """Test PoolConfig getter methods."""

    def test_get_max_size(self):
        """Test getting max_size."""
        config = PoolConfig(max_size=25)
        assert config.max_size == 25

    def test_get_min_idle(self):
        """Test getting min_idle."""
        config = PoolConfig(min_idle=5)
        assert config.min_idle == 5

    def test_get_min_idle_none(self):
        """Test getting min_idle when None."""
        config = PoolConfig(min_idle=None)
        assert config.min_idle is None

    def test_get_max_lifetime_secs(self):
        """Test getting max_lifetime_secs."""
        config = PoolConfig(max_lifetime_secs=3600)
        assert config.max_lifetime_secs == 3600

    def test_get_max_lifetime_secs_none(self):
        """Test getting max_lifetime_secs when None."""
        config = PoolConfig(max_lifetime_secs=None)
        assert config.max_lifetime_secs is None

    def test_get_idle_timeout_secs(self):
        """Test getting idle_timeout_secs."""
        config = PoolConfig(idle_timeout_secs=1200)
        assert config.idle_timeout_secs == 1200

    def test_get_idle_timeout_secs_none(self):
        """Test getting idle_timeout_secs when None."""
        config = PoolConfig(idle_timeout_secs=None)
        assert config.idle_timeout_secs is None

    def test_get_connection_timeout_secs(self):
        """Test getting connection_timeout_secs."""
        config = PoolConfig(connection_timeout_secs=60)
        assert config.connection_timeout_secs == 60

    def test_get_connection_timeout_secs_none(self):
        """Test getting connection_timeout_secs when None."""
        config = PoolConfig(connection_timeout_secs=None)
        assert config.connection_timeout_secs is None


@pytest.mark.skipif(PoolConfig is None, reason="fastmssql module not available")
class TestPoolConfigSetters:
    """Test PoolConfig setter methods."""

    def test_set_max_size(self):
        """Test setting max_size."""
        config = PoolConfig()
        config.max_size = 50
        assert config.max_size == 50

    def test_set_max_size_zero_invalid(self):
        """Test that setting max_size to 0 raises an error."""
        config = PoolConfig()
        with pytest.raises(ValueError, match="max_size must be greater than 0"):
            config.max_size = 0

    def test_set_max_size_less_than_min_idle_invalid(self):
        """Test that setting max_size less than min_idle raises an error."""
        config = PoolConfig(min_idle=5)
        with pytest.raises(ValueError, match="max_size cannot be less than min_idle"):
            config.max_size = 3

    def test_set_min_idle(self):
        """Test setting min_idle."""
        config = PoolConfig()
        config.min_idle = 8
        assert config.min_idle == 8

    def test_set_min_idle_none(self):
        """Test setting min_idle to None."""
        config = PoolConfig(min_idle=5)
        config.min_idle = None
        assert config.min_idle is None

    def test_set_min_idle_greater_than_max_size_invalid(self):
        """Test that setting min_idle > max_size raises an error."""
        config = PoolConfig(max_size=10)
        with pytest.raises(
            ValueError, match="min_idle cannot be greater than max_size"
        ):
            config.min_idle = 15

    def test_set_max_lifetime_secs(self):
        """Test setting max_lifetime_secs."""
        config = PoolConfig()
        config.max_lifetime_secs = 3600
        assert config.max_lifetime_secs == 3600

    def test_set_max_lifetime_secs_none(self):
        """Test setting max_lifetime_secs to None."""
        config = PoolConfig(max_lifetime_secs=1800)
        config.max_lifetime_secs = None
        assert config.max_lifetime_secs is None

    def test_set_idle_timeout_secs(self):
        """Test setting idle_timeout_secs."""
        config = PoolConfig()
        config.idle_timeout_secs = 900
        assert config.idle_timeout_secs == 900

    def test_set_idle_timeout_secs_none(self):
        """Test setting idle_timeout_secs to None."""
        config = PoolConfig(idle_timeout_secs=600)
        config.idle_timeout_secs = None
        assert config.idle_timeout_secs is None

    def test_set_connection_timeout_secs(self):
        """Test setting connection_timeout_secs."""
        config = PoolConfig()
        config.connection_timeout_secs = 120
        assert config.connection_timeout_secs == 120

    def test_set_connection_timeout_secs_none(self):
        """Test setting connection_timeout_secs to None."""
        config = PoolConfig(connection_timeout_secs=30)
        config.connection_timeout_secs = None
        assert config.connection_timeout_secs is None


@pytest.mark.skipif(PoolConfig is None, reason="fastmssql module not available")
class TestPoolConfigPresets:
    """Test PoolConfig preset configurations."""

    def test_high_throughput(self):
        """Test high_throughput preset."""
        config = PoolConfig.high_throughput()
        assert config.max_size == 25
        assert config.min_idle == 8
        assert config.max_lifetime_secs == 1800
        assert config.idle_timeout_secs == 600
        assert config.connection_timeout_secs == 30

    def test_one(self):
        """Test one preset for single connection."""
        config = PoolConfig.one()
        assert config.max_size == 1
        assert config.min_idle == 1
        assert config.max_lifetime_secs == 1800
        assert config.idle_timeout_secs == 300
        assert config.connection_timeout_secs == 30

    def test_low_resource(self):
        """Test low_resource preset."""
        config = PoolConfig.low_resource()
        assert config.max_size == 3
        assert config.min_idle == 1
        assert config.max_lifetime_secs == 900
        assert config.idle_timeout_secs == 300
        assert config.connection_timeout_secs == 15

    def test_development(self):
        """Test development preset."""
        config = PoolConfig.development()
        assert config.max_size == 5
        assert config.min_idle == 1
        assert config.max_lifetime_secs == 600
        assert config.idle_timeout_secs == 180
        assert config.connection_timeout_secs == 10

    def test_performance(self):
        """Test performance preset."""
        config = PoolConfig.performance()
        assert config.max_size == 30
        assert config.min_idle == 10
        assert config.max_lifetime_secs == 7200
        assert config.idle_timeout_secs == 1800
        assert config.connection_timeout_secs == 10


@pytest.mark.skipif(PoolConfig is None, reason="fastmssql module not available")
class TestPoolConfigAdaptive:
    """Test PoolConfig adaptive() method for dynamic pool sizing."""

    def test_adaptive_small_concurrency(self):
        """Test adaptive with small number of workers (5)."""
        config = PoolConfig.adaptive(5)
        # Formula: ceil(5 * 1.2) + 5 = ceil(6) + 5 = 11
        assert config.max_size == 11
        # min_idle = max_size / 3, max(2)
        assert config.min_idle == 3
        assert config.max_lifetime_secs == 1800
        assert config.idle_timeout_secs == 600
        assert config.connection_timeout_secs == 30

    def test_adaptive_medium_concurrency(self):
        """Test adaptive with medium number of workers (20)."""
        config = PoolConfig.adaptive(20)
        # Formula: ceil(20 * 1.2) + 5 = ceil(24) + 5 = 29
        assert config.max_size == 29
        # min_idle = 29 / 3 = 9
        assert config.min_idle == 9
        assert config.max_lifetime_secs == 1800
        assert config.idle_timeout_secs == 600

    def test_adaptive_high_concurrency(self):
        """Test adaptive with high number of workers (50)."""
        config = PoolConfig.adaptive(50)
        # Formula: ceil(50 * 1.2) + 5 = ceil(60) + 5 = 65
        assert config.max_size == 65
        # min_idle = 65 / 3 = 21
        assert config.min_idle == 21

    def test_adaptive_very_high_concurrency(self):
        """Test adaptive with very high number of workers (100)."""
        config = PoolConfig.adaptive(100)
        # Formula: ceil(100 * 1.2) + 5 = ceil(120) + 5 = 125
        assert config.max_size == 125
        # min_idle = 125 / 3 = 41
        assert config.min_idle == 41

    def test_adaptive_single_worker(self):
        """Test adaptive with single worker."""
        config = PoolConfig.adaptive(1)
        # Formula: ceil(1 * 1.2) + 5 = ceil(1.2) + 5 = 7
        assert config.max_size == 7
        # min_idle = max(7 / 3, 2) = max(2, 2) = 2
        assert config.min_idle == 2

    def test_adaptive_zero_workers(self):
        """Test adaptive with zero workers (edge case)."""
        config = PoolConfig.adaptive(0)
        # Formula: max(ceil(0 * 1.2) + 5, 5) = max(5, 5) = 5
        assert config.max_size == 5
        # min_idle = max(5 / 3, 2) = max(1, 2) = 2
        assert config.min_idle == 2

    def test_adaptive_typical_scenarios(self):
        """Test adaptive with typical real-world concurrency levels."""
        # Typical FastAPI with 4 workers
        config_4 = PoolConfig.adaptive(4)
        assert 9 <= config_4.max_size <= 11  # Should be around 10

        # Typical FastAPI with 8 workers
        config_8 = PoolConfig.adaptive(8)
        assert 14 <= config_8.max_size <= 16  # Should be around 15

        # High-load API with 32 workers
        config_32 = PoolConfig.adaptive(32)
        assert 43 <= config_32.max_size <= 45  # Should be around 44

    def test_adaptive_formula_consistency(self):
        """Test that adaptive formula is consistent and predictable."""
        # For each concurrency level, verify formula: ceil(n * 1.2) + 5
        import math

        for workers in [1, 2, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100]:
            config = PoolConfig.adaptive(workers)
            expected_max = max(math.ceil(workers * 1.2) + 5, 5)
            assert config.max_size == expected_max, (
                f"For {workers} workers, expected max_size={expected_max}, got {config.max_size}"
            )

    def test_adaptive_min_idle_ratio(self):
        """Test that min_idle maintains reasonable ratio to max_size."""
        # min_idle should be roughly 1/3 of max_size (but at least 2)
        for workers in [5, 10, 20, 30, 50]:
            config = PoolConfig.adaptive(workers)
            # min_idle should be between 20% and 40% of max_size
            ratio = config.min_idle / config.max_size
            assert 0.2 <= ratio <= 0.4, (
                f"For {workers} workers, min_idle ratio {ratio:.2f} out of range"
            )

    def test_adaptive_prevents_oversized_pools(self):
        """Test that adaptive produces smaller pools than old presets for typical loads."""
        # For 20 workers, adaptive should be much smaller than old performance() (was 100)
        config = PoolConfig.adaptive(20)
        assert config.max_size < 35, (
            f"Adaptive pool too large: {config.max_size} for 20 workers"
        )

    def test_adaptive_scales_linearly(self):
        """Test that pool size scales roughly linearly with worker count."""
        config_10 = PoolConfig.adaptive(10)
        config_20 = PoolConfig.adaptive(20)
        config_40 = PoolConfig.adaptive(40)

        # Doubling workers should roughly double pool size
        ratio_10_20 = config_20.max_size / config_10.max_size
        ratio_20_40 = config_40.max_size / config_20.max_size

        # Both ratios should be close to 2.0 (within 20% tolerance)
        assert 1.6 <= ratio_10_20 <= 2.4, f"Scaling ratio 10→20: {ratio_10_20:.2f}"
        assert 1.6 <= ratio_20_40 <= 2.4, f"Scaling ratio 20→40: {ratio_20_40:.2f}"

    def test_adaptive_all_settings_present(self):
        """Test that adaptive() sets all expected configuration values."""
        config = PoolConfig.adaptive(15)

        assert config.max_size > 0
        assert config.min_idle is not None
        assert config.min_idle >= 2
        assert config.max_lifetime_secs == 1800
        assert config.idle_timeout_secs == 600
        assert config.connection_timeout_secs == 30
        # These should be None (use defaults)
        assert config.test_on_check_out is None
        assert config.retry_connection is None


@pytest.mark.skipif(PoolConfig is None, reason="fastmssql module not available")
class TestPoolConfigRepresentation:
    """Test PoolConfig string representation."""

    def test_repr(self):
        """Test __repr__ output."""
        config = PoolConfig(
            max_size=20,
            min_idle=5,
            max_lifetime_secs=1800,
            idle_timeout_secs=600,
            connection_timeout_secs=45,
        )
        repr_str = repr(config)
        assert "PoolConfig" in repr_str
        assert "max_size=20" in repr_str
        assert "min_idle=Some(5)" in repr_str or "min_idle=5" in repr_str

    def test_repr_with_none_values(self):
        """Test __repr__ with None values."""
        config = PoolConfig(
            max_size=10,
            min_idle=None,
            max_lifetime_secs=None,
            idle_timeout_secs=None,
            connection_timeout_secs=None,
        )
        repr_str = repr(config)
        assert "PoolConfig" in repr_str
        assert "max_size=10" in repr_str


@pytest.mark.skipif(PoolConfig is None, reason="fastmssql module not available")
class TestPoolConfigEdgeCases:
    """Test edge cases and special scenarios."""

    def test_large_max_size(self):
        """Test with large max_size value."""
        config = PoolConfig(max_size=1000)
        assert config.max_size == 1000

    def test_large_timeout_values(self):
        """Test with large timeout values."""
        config = PoolConfig(
            max_lifetime_secs=86400,  # 1 day
            idle_timeout_secs=43200,  # 12 hours
            connection_timeout_secs=300,
        )
        assert config.max_lifetime_secs == 86400
        assert config.idle_timeout_secs == 43200
        assert config.connection_timeout_secs == 300

    def test_small_timeout_values(self):
        """Test with small timeout values."""
        config = PoolConfig(
            max_lifetime_secs=1, idle_timeout_secs=1, connection_timeout_secs=1
        )
        assert config.max_lifetime_secs == 1
        assert config.idle_timeout_secs == 1
        assert config.connection_timeout_secs == 1

    def test_config_immutability_independence(self):
        """Test that modifying one config doesn't affect another."""
        config1 = PoolConfig(max_size=10)
        config2 = PoolConfig(max_size=20)

        config1.max_size = 30
        assert config1.max_size == 30
        assert config2.max_size == 20

    def test_preset_independence(self):
        """Test that modifying a preset config doesn't affect others."""
        config1 = PoolConfig.development()
        config2 = PoolConfig.development()

        config1.max_size = 100
        assert config1.max_size == 100
        assert config2.max_size == 5  # Should still be default

    def test_sequential_setter_calls(self):
        """Test multiple sequential setter calls."""
        config = PoolConfig()
        config.max_size = 50
        config.min_idle = 10
        config.max_lifetime_secs = 3600
        config.idle_timeout_secs = 1200
        config.connection_timeout_secs = 60

        assert config.max_size == 50
        assert config.min_idle == 10
        assert config.max_lifetime_secs == 3600
        assert config.idle_timeout_secs == 1200
        assert config.connection_timeout_secs == 60


@pytest.mark.skipif(PoolConfig is None, reason="fastmssql module not available")
class TestPoolConfigTestOnCheckOut:
    """Test PoolConfig test_on_check_out configuration."""

    def test_test_on_check_out_default(self):
        """Test that test_on_check_out defaults to None."""
        config = PoolConfig()
        assert config.test_on_check_out is None

    def test_test_on_check_out_enabled(self):
        """Test creating PoolConfig with test_on_check_out enabled."""
        config = PoolConfig(test_on_check_out=True)
        assert config.test_on_check_out is True

    def test_test_on_check_out_disabled(self):
        """Test creating PoolConfig with test_on_check_out disabled."""
        config = PoolConfig(test_on_check_out=False)
        assert config.test_on_check_out is False

    def test_test_on_check_out_explicit_none(self):
        """Test creating PoolConfig with test_on_check_out explicitly None."""
        config = PoolConfig(test_on_check_out=None)
        assert config.test_on_check_out is None

    def test_test_on_check_out_with_other_settings(self):
        """Test test_on_check_out combined with other pool settings."""
        config = PoolConfig(
            max_size=20,
            min_idle=5,
            max_lifetime_secs=1800,
            idle_timeout_secs=600,
            connection_timeout_secs=30,
            test_on_check_out=True,
        )
        assert config.max_size == 20
        assert config.min_idle == 5
        assert config.max_lifetime_secs == 1800
        assert config.idle_timeout_secs == 600
        assert config.connection_timeout_secs == 30
        assert config.test_on_check_out is True

    def test_test_on_check_out_getter(self):
        """Test getting test_on_check_out value."""
        config = PoolConfig(test_on_check_out=True)
        assert config.test_on_check_out is True


@pytest.mark.skipif(PoolConfig is None, reason="fastmssql module not available")
class TestPoolConfigRetryConnection:
    """Test PoolConfig retry_connection configuration."""

    def test_retry_connection_default(self):
        """Test that retry_connection defaults to None."""
        config = PoolConfig()
        assert config.retry_connection is None

    def test_retry_connection_enabled(self):
        """Test creating PoolConfig with retry_connection enabled."""
        config = PoolConfig(retry_connection=True)
        assert config.retry_connection is True

    def test_retry_connection_disabled(self):
        """Test creating PoolConfig with retry_connection disabled."""
        config = PoolConfig(retry_connection=False)
        assert config.retry_connection is False

    def test_retry_connection_explicit_none(self):
        """Test creating PoolConfig with retry_connection explicitly None."""
        config = PoolConfig(retry_connection=None)
        assert config.retry_connection is None

    def test_retry_connection_with_other_settings(self):
        """Test retry_connection combined with other pool settings."""
        config = PoolConfig(
            max_size=20,
            min_idle=5,
            max_lifetime_secs=1800,
            idle_timeout_secs=600,
            connection_timeout_secs=30,
            retry_connection=True,
        )
        assert config.max_size == 20
        assert config.min_idle == 5
        assert config.max_lifetime_secs == 1800
        assert config.idle_timeout_secs == 600
        assert config.connection_timeout_secs == 30
        assert config.retry_connection is True

    def test_retry_connection_getter(self):
        """Test getting retry_connection value."""
        config = PoolConfig(retry_connection=True)
        assert config.retry_connection is True


@pytest.mark.skipif(PoolConfig is None, reason="fastmssql module not available")
class TestPoolConfigCombinedNewSettings:
    """Test combinations of test_on_check_out and retry_connection."""

    def test_both_settings_true(self):
        """Test with both test_on_check_out and retry_connection True."""
        config = PoolConfig(max_size=15, test_on_check_out=True, retry_connection=True)
        assert config.test_on_check_out is True
        assert config.retry_connection is True

    def test_both_settings_false(self):
        """Test with both test_on_check_out and retry_connection False."""
        config = PoolConfig(
            max_size=15, test_on_check_out=False, retry_connection=False
        )
        assert config.test_on_check_out is False
        assert config.retry_connection is False

    def test_mixed_settings_true_false(self):
        """Test with test_on_check_out True and retry_connection False."""
        config = PoolConfig(test_on_check_out=True, retry_connection=False)
        assert config.test_on_check_out is True
        assert config.retry_connection is False

    def test_mixed_settings_false_true(self):
        """Test with test_on_check_out False and retry_connection True."""
        config = PoolConfig(test_on_check_out=False, retry_connection=True)
        assert config.test_on_check_out is False
        assert config.retry_connection is True

    def test_both_settings_none(self):
        """Test with both settings as None."""
        config = PoolConfig(test_on_check_out=None, retry_connection=None)
        assert config.test_on_check_out is None
        assert config.retry_connection is None

    def test_all_settings_combined(self):
        """Test all settings together including new reliability settings."""
        config = PoolConfig(
            max_size=30,
            min_idle=8,
            max_lifetime_secs=2400,
            idle_timeout_secs=800,
            connection_timeout_secs=45,
            test_on_check_out=True,
            retry_connection=True,
        )
        assert config.max_size == 30
        assert config.min_idle == 8
        assert config.max_lifetime_secs == 2400
        assert config.idle_timeout_secs == 800
        assert config.connection_timeout_secs == 45
        assert config.test_on_check_out is True
        assert config.retry_connection is True
