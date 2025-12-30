import asyncio
import pytest

try:
    from fastmssql import Connection, PoolConfig
except ImportError:
    Connection = None
    PoolConfig = None


@pytest.mark.skipif(Connection is None or PoolConfig is None, reason="fastmssql module not available")
class TestConnectionPoolBasics:
    """Test basic connection pool creation and configuration."""

    @pytest.mark.asyncio
    async def test_connection_with_default_pool_config(self, test_connection_string):
        """Test creating a connection with default pool configuration."""
        async with Connection(test_connection_string) as conn:
            result = await conn.query("SELECT 1 as test_val")
            assert result.has_rows()
            rows = result.rows()
            assert len(rows) > 0
            assert rows[0]["test_val"] == 1

    @pytest.mark.asyncio
    async def test_connection_with_custom_pool_config(self, test_connection_string):
        """Test creating a connection with custom pool configuration."""
        pool_config = PoolConfig(
            max_size=5,
            min_idle=1,
            connection_timeout_secs=15
        )
        async with Connection(test_connection_string, pool_config) as conn:
            result = await conn.query("SELECT 1 as test_val")
            assert result.has_rows()
            rows = result.rows()
            assert rows[0]["test_val"] == 1

    @pytest.mark.asyncio
    async def test_connection_with_high_throughput_config(self, test_connection_string):
        """Test creating a connection with high_throughput preset."""
        pool_config = PoolConfig.high_throughput()
        async with Connection(test_connection_string, pool_config) as conn:
            result = await conn.query("SELECT 1 as test_val")
            assert result.has_rows()

    @pytest.mark.asyncio
    async def test_connection_with_development_config(self, test_connection_string):
        """Test creating a connection with development preset."""
        pool_config = PoolConfig.development()
        async with Connection(test_connection_string, pool_config) as conn:
            result = await conn.query("SELECT 1 as test_val")
            assert result.has_rows()

    @pytest.mark.asyncio
    async def test_connection_with_low_resource_config(self, test_connection_string):
        """Test creating a connection with low_resource preset."""
        pool_config = PoolConfig.low_resource()
        async with Connection(test_connection_string, pool_config) as conn:
            result = await conn.query("SELECT 1 as test_val")
            assert result.has_rows()

    @pytest.mark.asyncio
    async def test_connection_with_single_pool_config(self, test_connection_string):
        """Test creating a connection with single connection pool."""
        pool_config = PoolConfig.one()
        async with Connection(test_connection_string, pool_config) as conn:
            result = await conn.query("SELECT 1 as test_val")
            assert result.has_rows()


@pytest.mark.skipif(Connection is None or PoolConfig is None, reason="fastmssql module not available")
class TestConnectionPoolReuse:
    """Test that connection pool is reused across multiple operations."""

    @pytest.mark.asyncio
    async def test_sequential_queries_reuse_pool(self, test_connection_string):
        """Test that sequential queries reuse the same pool."""
        pool_config = PoolConfig(max_size=3, min_idle=1)
        async with Connection(test_connection_string, pool_config) as conn:
            # Multiple sequential queries should reuse connections from the pool
            for i in range(5):
                result = await conn.query(f"SELECT {i} as test_val")
                rows = result.rows()
                assert rows[0]["test_val"] == i

    @pytest.mark.asyncio
    async def test_multiple_concurrent_queries(self, test_connection_string):
        """Test multiple concurrent queries within a single connection context."""
        pool_config = PoolConfig(max_size=5, min_idle=2)
        async with Connection(test_connection_string, pool_config) as conn:
            # Create multiple concurrent queries
            tasks = [
                conn.query(f"SELECT {i} as test_val")
                for i in range(3)
            ]
            results = await asyncio.gather(*tasks)
            assert len(results) == 3
            for i, result in enumerate(results):
                rows = result.rows()
                assert rows[0]["test_val"] == i

    @pytest.mark.asyncio
    async def test_consecutive_connections_same_config(self, test_connection_string):
        """Test that consecutive connections can reuse pool resources."""
        pool_config = PoolConfig(max_size=3, min_idle=1)
        
        # First connection
        async with Connection(test_connection_string, pool_config) as conn:
            result1 = await conn.query("SELECT 1 as test_val")
            rows1 = result1.rows()
            assert rows1[0]["test_val"] == 1
        
        # Second connection - should be able to reuse pool
        async with Connection(test_connection_string, pool_config) as conn:
            result2 = await conn.query("SELECT 2 as test_val")
            rows2 = result2.rows()
            assert rows2[0]["test_val"] == 2


@pytest.mark.skipif(Connection is None or PoolConfig is None, reason="fastmssql module not available")
class TestConnectionPoolUnderLoad:
    """Test connection pool behavior under concurrent load."""

    @pytest.mark.asyncio
    async def test_pool_handles_concurrent_connections(self, test_connection_string):
        """Test that pool properly handles multiple concurrent connections."""
        pool_config = PoolConfig(max_size=10, min_idle=2)
        
        async def query_task(conn, task_id):
            result = await conn.query(f"SELECT {task_id} as task_id")
            rows = result.rows()
            return rows[0]["task_id"]
        
        async with Connection(test_connection_string, pool_config) as conn:
            tasks = [query_task(conn, i) for i in range(5)]
            results = await asyncio.gather(*tasks)
            assert sorted(results) == [0, 1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_pool_with_high_throughput_load(self, test_connection_string):
        """Test pool performance with high throughput configuration."""
        pool_config = PoolConfig.high_throughput()
        
        async def simple_query(conn):
            result = await conn.query("SELECT 1 as val")
            rows = result.rows()
            return rows[0]["val"]
        
        async with Connection(test_connection_string, pool_config) as conn:
            tasks = [simple_query(conn) for _ in range(20)]
            results = await asyncio.gather(*tasks)
            assert all(r == 1 for r in results)
            assert len(results) == 20

    @pytest.mark.asyncio
    async def test_pool_with_development_config_load(self, test_connection_string):
        """Test pool behavior with development configuration under moderate load."""
        pool_config = PoolConfig.development()
        
        async def query_with_data(conn, query_id):
            result = await conn.query(f"SELECT {query_id} as id")
            rows = result.rows()
            return rows[0]["id"]
        
        async with Connection(test_connection_string, pool_config) as conn:
            # Development config has max_size=5, so we test with moderate concurrent load
            tasks = [query_with_data(conn, i) for i in range(8)]
            results = await asyncio.gather(*tasks)
            assert len(results) == 8


@pytest.mark.skipif(Connection is None or PoolConfig is None, reason="fastmssql module not available")
class TestConnectionPoolEdgeCases:
    """Test edge cases and special scenarios in connection pooling."""

    @pytest.mark.asyncio
    async def test_pool_with_minimal_size(self, test_connection_string):
        """Test pool with minimal configuration (single connection)."""
        pool_config = PoolConfig.one()
        async with Connection(test_connection_string, pool_config) as conn:
            result = await conn.query("SELECT 1 as test")
            assert result.has_rows()

    @pytest.mark.asyncio
    async def test_pool_with_max_size_one(self, test_connection_string):
        """Test that max_size=1 pool still works correctly."""
        pool_config = PoolConfig(max_size=1, min_idle=1)
        async with Connection(test_connection_string, pool_config) as conn:
            for i in range(5):
                result = await conn.query(f"SELECT {i} as val")
                rows = result.rows()
                assert rows[0]["val"] == i

    @pytest.mark.asyncio
    async def test_pool_with_no_min_idle(self, test_connection_string):
        """Test pool with min_idle set to None."""
        pool_config = PoolConfig(max_size=5, min_idle=None)
        async with Connection(test_connection_string, pool_config) as conn:
            result = await conn.query("SELECT 1 as test")
            assert result.has_rows()

    @pytest.mark.asyncio
    async def test_pool_with_no_timeouts(self, test_connection_string):
        """Test pool with all timeout values set to None."""
        pool_config = PoolConfig(
            max_size=5,
            min_idle=1,
            max_lifetime_secs=None,
            idle_timeout_secs=None,
            connection_timeout_secs=None
        )
        async with Connection(test_connection_string, pool_config) as conn:
            result = await conn.query("SELECT 1 as test")
            assert result.has_rows()

    @pytest.mark.asyncio
    async def test_many_sequential_operations(self, test_connection_string):
        """Test that pool handles many sequential operations without degradation."""
        pool_config = PoolConfig(max_size=3, min_idle=1)
        async with Connection(test_connection_string, pool_config) as conn:
            results = []
            for i in range(20):
                result = await conn.query(f"SELECT {i} as idx")
                rows = result.rows()
                results.append(rows[0]["idx"])
            
            assert results == list(range(20))


@pytest.mark.skipif(Connection is None or PoolConfig is None, reason="fastmssql module not available")
class TestConnectionPoolLifecycle:
    """Test connection pool lifecycle and resource management."""

    @pytest.mark.asyncio
    async def test_pool_cleanup_on_context_exit(self, test_connection_string):
        """Test that pool is cleaned up when connection context exits."""
        pool_config = PoolConfig(max_size=5, min_idle=2)
        async with Connection(test_connection_string, pool_config) as conn:
            result = await conn.query("SELECT 1 as test")
            assert result.has_rows()
        
        # After context exit, another connection should work fine
        async with Connection(test_connection_string, pool_config) as conn:
            result = await conn.query("SELECT 2 as test")
            assert result.has_rows()

    @pytest.mark.asyncio
    async def test_rapid_context_enter_exit(self, test_connection_string):
        """Test creating and closing multiple connections rapidly."""
        pool_config = PoolConfig(max_size=3, min_idle=1)
        
        for i in range(5):
            async with Connection(test_connection_string, pool_config) as conn:
                result = await conn.query(f"SELECT {i} as idx")
                rows = result.rows()
                assert rows[0]["idx"] == i

    @pytest.mark.asyncio
    async def test_pool_reinitialization(self, test_connection_string):
        """Test that pool can be reinitialized after being used."""
        pool_config1 = PoolConfig(max_size=3, min_idle=1)
        async with Connection(test_connection_string, pool_config1) as conn:
            result = await conn.query("SELECT 1 as test")
            assert result.has_rows()
        
        pool_config2 = PoolConfig(max_size=5, min_idle=2)
        async with Connection(test_connection_string, pool_config2) as conn:
            result = await conn.query("SELECT 2 as test")
            assert result.has_rows()


@pytest.mark.skipif(Connection is None or PoolConfig is None, reason="fastmssql module not available")
class TestConnectionPoolPresetConfigs:
    """Test all preset pool configurations."""

    @pytest.mark.asyncio
    async def test_performance_preset(self, test_connection_string):
        """Test performance preset configuration."""
        pool_config = PoolConfig.performance()
        async with Connection(test_connection_string, pool_config) as conn:
            tasks = [conn.query("SELECT 1 as test") for _ in range(10)]
            results = await asyncio.gather(*tasks)
            assert len(results) == 10

    @pytest.mark.asyncio
    async def test_load_test_worker_preset(self, test_connection_string):
        """Test load_test_worker preset configuration."""
        pool_config = PoolConfig.load_test_worker()
        async with Connection(test_connection_string, pool_config) as conn:
            tasks = [conn.query("SELECT 1 as test") for _ in range(6)]
            results = await asyncio.gather(*tasks)
            assert len(results) == 6

    @pytest.mark.asyncio
    async def test_all_presets_work(self, test_connection_string):
        """Test that all preset configurations work correctly."""
        presets = [
            PoolConfig.one(),
            PoolConfig.low_resource(),
            PoolConfig.development(),
            PoolConfig.high_throughput(),
            PoolConfig.performance(),
            PoolConfig.load_test_worker(),
        ]
        
        for preset in presets:
            async with Connection(test_connection_string, preset) as conn:
                result = await conn.query("SELECT 1 as test")
                assert result.has_rows()
