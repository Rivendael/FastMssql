import pytest

try:
    from fastmssql import PoolConfig
except ImportError:
    PoolConfig = None


@pytest.mark.skipif(PoolConfig is None, reason="fastmssql module not available")
class TestPoolConfigConstructor:
    """Test PoolConfig constructor and basic validation."""

    def test_default_constructor(self):
        """Test creating PoolConfig with default values."""
        config = PoolConfig()
        assert config.max_size == 10
        assert config.min_idle == 1
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
            connection_timeout_secs=45
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
            connection_timeout_secs=None
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
        with pytest.raises(ValueError, match="min_idle cannot be greater than max_size"):
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
        with pytest.raises(ValueError, match="min_idle cannot be greater than max_size"):
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
        assert config.max_size == 50
        assert config.min_idle == 15
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
        assert config.max_size == 100
        assert config.min_idle == 30
        assert config.max_lifetime_secs == 7200
        assert config.idle_timeout_secs == 1800
        assert config.connection_timeout_secs == 10

    def test_load_test_worker(self):
        """Test load_test_worker preset."""
        config = PoolConfig.load_test_worker()
        assert config.max_size == 12
        assert config.min_idle == 4
        assert config.max_lifetime_secs == 3600
        assert config.idle_timeout_secs == 600
        assert config.connection_timeout_secs == 5


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
            connection_timeout_secs=45
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
            connection_timeout_secs=None
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
            connection_timeout_secs=300
        )
        assert config.max_lifetime_secs == 86400
        assert config.idle_timeout_secs == 43200
        assert config.connection_timeout_secs == 300

    def test_small_timeout_values(self):
        """Test with small timeout values."""
        config = PoolConfig(
            max_lifetime_secs=1,
            idle_timeout_secs=1,
            connection_timeout_secs=1
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
