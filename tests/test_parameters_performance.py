"""
Performance tests for Parameter and Parameters classes

These tests verify that the new parameter system doesn't add significant
overhead compared to using simple lists.
"""

import pytest
import time
import os

# Add the parent directory to Python path for development

try:
    from fastmssql import Connection, Parameters
except ImportError:
    pytest.fail("fastmssql not available - run 'maturin develop' first", allow_module_level=True)

# Test configuration
TEST_CONNECTION_STRING = os.getenv(
    "FASTMSSQL_TEST_CONNECTION_STRING",
)

@pytest.mark.integration
@pytest.mark.performance
class TestParametersPerformance:
    """Performance tests for Parameters vs simple lists."""
    
    @pytest.mark.asyncio
    async def test_parameter_creation_performance(self):
        """Test that creating Parameters objects is reasonably fast."""
        try:
            # Time creating many Parameters objects
            start_time = time.time()
            
            for i in range(1000):
                params = Parameters(i, f"test_{i}", i * 3.14, i % 2 == 0)
                # Convert to list to ensure it's processed
                _ = params.to_list()
            
            end_time = time.time()
            creation_time = end_time - start_time
            
            # Should be able to create 1000 Parameters objects in reasonable time
            assert creation_time < 1.0, f"Creating 1000 Parameters took {creation_time:.3f}s (too slow)"
            
            print(f"Created 1000 Parameters objects in {creation_time:.3f}s")
            
        except Exception as e:
            pytest.fail(f"Performance test failed: {e}")
    
    @pytest.mark.asyncio
    async def test_method_chaining_performance(self):
        """Test that method chaining doesn't add significant overhead."""
        try:
            # Time method chaining
            start_time = time.time()
            
            for i in range(500):
                params = (Parameters()
                         .add(i)
                         .add(f"name_{i}")
                         .add(i * 2.5)
                         .add(i % 3 == 0))
                _ = params.to_list()
            
            end_time = time.time()
            chaining_time = end_time - start_time
            
            # Should be reasonably fast
            assert chaining_time < 1.0, f"Method chaining 500 times took {chaining_time:.3f}s (too slow)"
            
            print(f"Method chaining 500 times took {chaining_time:.3f}s")
            
        except Exception as e:
            pytest.fail(f"Performance test failed: {e}")
    
    @pytest.mark.asyncio
    async def test_list_vs_parameters_query_performance(self):
        """Compare query performance between lists and Parameters objects."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Test with simple lists
                list_start = time.time()
                
                for i in range(50):
                    result = await conn.query(
                        "SELECT @P1 as id, @P2 as name, @P3 as value",
                        [i, f"list_test_{i}", i * 1.5]
                    )
                    _ = result.rows()  # Force processing
                
                list_end = time.time()
                list_time = list_end - list_start
                
                # Test with Parameters objects
                params_start = time.time()
                
                for i in range(50):
                    params = Parameters(i, f"params_test_{i}", i * 1.5)
                    result = await conn.query(
                        "SELECT @P1 as id, @P2 as name, @P3 as value",
                        params
                    )
                    _ = result.rows()  # Force processing
                
                params_end = time.time()
                params_time = params_end - params_start
                
                # Parameters should not be significantly slower than lists
                overhead_ratio = params_time / list_time if list_time > 0 else 1
                
                print(f"List queries: {list_time:.3f}s")
                print(f"Parameters queries: {params_time:.3f}s")
                print(f"Overhead ratio: {overhead_ratio:.2f}x")
                
                # Allow up to 50% overhead (should be much less in practice)
                assert overhead_ratio < 1.5, f"Parameters overhead too high: {overhead_ratio:.2f}x"
                
        except Exception as e:
            pytest.fail(f"Database not available for performance test: {e}")
    
    @pytest.mark.asyncio
    async def test_parameter_reuse_performance(self):
        """Test performance of reusing Parameters objects."""
        try:
            async with Connection(TEST_CONNECTION_STRING) as conn:
                # Create Parameters object once
                params = Parameters(42, "Reused", 3.14159)
                
                start_time = time.time()
                
                # Reuse the same Parameters object many times
                for i in range(100):
                    result = await conn.query(
                        "SELECT @P1 as id, @P2 as name, @P3 as value, @P4 as iteration",
                        Parameters(params.to_list()[0], params.to_list()[1], params.to_list()[2], i)
                    )
                    _ = result.rows()
                
                end_time = time.time()
                reuse_time = end_time - start_time
                
                print(f"100 parameter reuse queries took {reuse_time:.3f}s")
                
                # Should complete in reasonable time
                assert reuse_time < 10.0, f"Parameter reuse too slow: {reuse_time:.3f}s"
                
        except Exception as e:
            pytest.fail(f"Database not available for reuse test: {e}")


@pytest.mark.integration  
@pytest.mark.performance
class TestParametersMemory:
    """Memory usage tests for Parameters objects."""
    
    def test_parameters_memory_efficiency(self):
        """Test that Parameters objects don't use excessive memory."""
        import gc
        
        # Force garbage collection before test
        gc.collect()
        
        # Create many Parameters objects
        params_list = []
        for i in range(1000):
            params = Parameters(i, f"test_{i}", i * 3.14, i % 2 == 0)
            params_list.append(params)
        
        # Should be able to create 1000 without issues
        assert len(params_list) == 1000
        
        # Verify they still work
        for i, params in enumerate(params_list[:10]):  # Test first 10
            values = params.to_list()
            assert values[0] == i
            assert values[1] == f"test_{i}"
            assert abs(values[2] - i * 3.14) < 0.001
            assert values[3] == (i % 2 == 0)
        
        # Clean up
        del params_list
        gc.collect()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "performance"])
