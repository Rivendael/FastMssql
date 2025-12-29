"""
Performance and benchmark tests for SSL/TLS configuration.
"""

import pytest
import tempfile
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Optional psutil import for memory tests
try:
    HAS_PSUTIL = True
except ImportError:
    psutil = None
    HAS_PSUTIL = False

# Import the library components
try:
    from fastmssql import SslConfig, EncryptionLevel, Connection
except ImportError as e:
    pytest.fail(f"Cannot import mssql library: {e}", allow_module_level=True)


class TestSslConfigPerformance:
    """Test SSL configuration performance characteristics."""
    
    def test_ssl_config_creation_performance(self):
        """Benchmark SSL config creation time."""
        num_iterations = 1000
        
        start_time = time.perf_counter()
        
        for i in range(num_iterations):
            ssl_config = SslConfig(
                encryption_level=EncryptionLevel.Required,
                trust_server_certificate=True,
                enable_sni=True,
                server_name=f"server{i}.com"
            )
        
        end_time = time.perf_counter()
        total_time = end_time - start_time
        avg_time = total_time / num_iterations
        
        print(f"\nSSL Config Creation Performance:")
        print(f"Total time for {num_iterations} configs: {total_time:.4f}s")
        print(f"Average time per config: {avg_time:.6f}s")
        print(f"Configs per second: {num_iterations / total_time:.0f}")
        
        # Should be able to create at least 100 configs per second
        assert num_iterations / total_time > 100
    
    def test_ssl_config_factory_methods_performance(self):
        """Benchmark SSL config factory methods."""
        num_iterations = 1000
        
        factory_methods = [
            ("development", SslConfig.development),
            ("login_only", SslConfig.login_only),
            ("disabled", SslConfig.disabled),
        ]
        
        for method_name, method in factory_methods:
            start_time = time.perf_counter()
            
            for _ in range(num_iterations):
                ssl_config = method()
            
            end_time = time.perf_counter()
            total_time = end_time - start_time
            avg_time = total_time / num_iterations
            
            print(f"\n{method_name} Factory Method Performance:")
            print(f"Total time for {num_iterations} configs: {total_time:.4f}s")
            print(f"Average time per config: {avg_time:.6f}s")
            print(f"Configs per second: {num_iterations / total_time:.0f}")
            
            # Factory methods should be fast
            assert num_iterations / total_time > 500
    
    def test_ssl_config_property_access_performance(self):
        """Benchmark SSL config property access time."""
        ssl_config = SslConfig(
            encryption_level=EncryptionLevel.Required,
            trust_server_certificate=True,
            enable_sni=True,
            server_name="test.server.com"
        )
        
        num_iterations = 10000
        
        start_time = time.perf_counter()
        
        for _ in range(num_iterations):
            # Access all properties
            _ = ssl_config.encryption_level
            _ = ssl_config.trust_server_certificate
            _ = ssl_config.enable_sni
            _ = ssl_config.server_name
            _ = ssl_config.ca_certificate_path
        
        end_time = time.perf_counter()
        total_time = end_time - start_time
        avg_time = total_time / (num_iterations * 5)  # 5 properties per iteration
        
        print(f"\nSSL Config Property Access Performance:")
        print(f"Total time for {num_iterations * 5} property accesses: {total_time:.4f}s")
        print(f"Average time per property access: {avg_time:.8f}s")
        print(f"Property accesses per second: {(num_iterations * 5) / total_time:.0f}")
        
        # Property access should be very fast
        assert (num_iterations * 5) / total_time > 100000
    
    def test_ssl_config_with_certificate_file_performance(self):
        """Benchmark SSL config creation with certificate files."""
        content = "-----BEGIN CERTIFICATE-----\ntest certificate content\n-----END CERTIFICATE-----"
        
        # Create temporary certificate files
        cert_files = []
        for i in range(10):
            with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
                f.write(content)
                cert_files.append(f.name)
        
        try:
            num_iterations = 100
            
            start_time = time.perf_counter()
            
            for i in range(num_iterations):
                cert_path = cert_files[i % len(cert_files)]
                ssl_config = SslConfig(ca_certificate_path=cert_path)
            
            end_time = time.perf_counter()
            total_time = end_time - start_time
            avg_time = total_time / num_iterations
            
            print(f"\nSSL Config with Certificate Performance:")
            print(f"Total time for {num_iterations} configs: {total_time:.4f}s")
            print(f"Average time per config: {avg_time:.6f}s")
            print(f"Configs per second: {num_iterations / total_time:.0f}")
            
            # Should still be reasonably fast even with file I/O
            assert num_iterations / total_time > 10
            
        finally:
            for cert_file in cert_files:
                os.unlink(cert_file)
    
    def test_connection_creation_with_ssl_performance(self):
        """Benchmark connection creation with SSL config."""
        ssl_config = SslConfig.development()
        
        num_iterations = 100
        
        start_time = time.perf_counter()
        
        for i in range(num_iterations):
            connection = Connection(
                server="localhost",
                database=f"testdb{i}",
                ssl_config=ssl_config,
                username="testuser",
                password="testpass"
            )
        
        end_time = time.perf_counter()
        total_time = end_time - start_time
        avg_time = total_time / num_iterations
        
        print(f"\nConnection with SSL Creation Performance:")
        print(f"Total time for {num_iterations} connections: {total_time:.4f}s")
        print(f"Average time per connection: {avg_time:.6f}s")
        print(f"Connections per second: {num_iterations / total_time:.0f}")
        
        # Connection creation should be reasonably fast
        assert num_iterations / total_time > 5


class TestSslConfigConcurrentPerformance:
    """Test SSL configuration performance under concurrent load."""
    
    def test_concurrent_ssl_config_creation_performance(self):
        """Benchmark concurrent SSL config creation."""
        num_threads = 10
        configs_per_thread = 100
        total_configs = num_threads * configs_per_thread
        
        def create_configs(thread_id):
            configs = []
            for i in range(configs_per_thread):
                ssl_config = SslConfig(
                    encryption_level=EncryptionLevel.Required,
                    trust_server_certificate=True,
                    server_name=f"server{thread_id}_{i}.com"
                )
                configs.append(ssl_config)
            return configs
        
        start_time = time.perf_counter()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(create_configs, i) for i in range(num_threads)]
            
            all_configs = []
            for future in as_completed(futures):
                configs = future.result()
                all_configs.extend(configs)
        
        end_time = time.perf_counter()
        total_time = end_time - start_time
        
        print(f"\nConcurrent SSL Config Creation Performance:")
        print(f"Total configs created: {len(all_configs)}")
        print(f"Number of threads: {num_threads}")
        print(f"Total time: {total_time:.4f}s")
        print(f"Configs per second: {total_configs / total_time:.0f}")
        
        assert len(all_configs) == total_configs
        # Concurrent creation should still maintain good throughput
        assert total_configs / total_time > 200
    
    def test_concurrent_property_access_performance(self):
        """Benchmark concurrent property access."""
        ssl_config = SslConfig(
            encryption_level=EncryptionLevel.Required,
            trust_server_certificate=True,
            enable_sni=True,
            server_name="test.server.com"
        )
        
        num_threads = 5
        accesses_per_thread = 1000
        total_accesses = num_threads * accesses_per_thread * 5  # 5 properties
        
        def access_properties(thread_id):
            for _ in range(accesses_per_thread):
                _ = ssl_config.encryption_level
                _ = ssl_config.trust_server_certificate
                _ = ssl_config.enable_sni
                _ = ssl_config.server_name
                _ = ssl_config.ca_certificate_path
        
        start_time = time.perf_counter()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(access_properties, i) for i in range(num_threads)]
            
            for future in as_completed(futures):
                future.result()
        
        end_time = time.perf_counter()
        total_time = end_time - start_time
        
        print(f"\nConcurrent Property Access Performance:")
        print(f"Total property accesses: {total_accesses}")
        print(f"Number of threads: {num_threads}")
        print(f"Total time: {total_time:.4f}s")
        print(f"Accesses per second: {total_accesses / total_time:.0f}")
        
        # Concurrent property access should be very fast
        assert total_accesses / total_time > 50000
    
    def test_mixed_ssl_operations_performance(self):
        """Benchmark mixed SSL operations under concurrent load."""
        num_threads = 8
        operations_per_thread = 50
        
        def mixed_operations(thread_id):
            results = []
            
            for i in range(operations_per_thread):
                # Create different types of SSL configs
                if i % 4 == 0:
                    ssl_config = SslConfig.development()
                elif i % 4 == 1:
                    ssl_config = SslConfig.login_only()
                elif i % 4 == 2:
                    ssl_config = SslConfig.disabled()
                else:
                    ssl_config = SslConfig(
                        encryption_level=EncryptionLevel.Required,
                        trust_server_certificate=True,
                        server_name=f"server{thread_id}_{i}.com"
                    )
                
                # Access properties
                _ = ssl_config.encryption_level
                _ = ssl_config.trust_server_certificate
                
                # Create connection
                connection = Connection(
                    server="localhost",
                    database=f"db{thread_id}_{i}",
                    ssl_config=ssl_config,
                    trusted_connection=True,
                    username="testuser",
                    password="testpass"
                )
                
                results.append((ssl_config, connection))
            
            return results
        
        start_time = time.perf_counter()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(mixed_operations, i) for i in range(num_threads)]
            
            all_results = []
            for future in as_completed(futures):
                results = future.result()
                all_results.extend(results)
        
        end_time = time.perf_counter()
        total_time = end_time - start_time
        total_operations = len(all_results)
        
        print(f"\nMixed SSL Operations Performance:")
        print(f"Total operations: {total_operations}")
        print(f"Number of threads: {num_threads}")
        print(f"Total time: {total_time:.4f}s")
        print(f"Operations per second: {total_operations / total_time:.0f}")
        
        assert total_operations == num_threads * operations_per_thread
        # Mixed operations should maintain reasonable throughput
        assert total_operations / total_time > 20
    
    def test_ssl_config_string_representation_performance(self):
        """Test performance of SSL config string representations."""
        ssl_configs = []
        
        # Create various SSL configs
        for i in range(100):
            ssl_configs.append(SslConfig(
                encryption_level=EncryptionLevel.Required,
                trust_server_certificate=True,
                server_name=f"server{i}.com"
            ))
            ssl_configs.append(SslConfig.development())
            ssl_configs.append(SslConfig.login_only())
            ssl_configs.append(SslConfig.disabled())
        
        num_iterations = 1000
        
        # Test __str__ performance
        start_time = time.perf_counter()
        for _ in range(num_iterations):
            for config in ssl_configs:
                _ = str(config)
        end_time = time.perf_counter()
        
        str_time = end_time - start_time
        str_ops = num_iterations * len(ssl_configs)
        
        # Test __repr__ performance
        start_time = time.perf_counter()
        for _ in range(num_iterations):
            for config in ssl_configs:
                _ = repr(config)
        end_time = time.perf_counter()
        
        repr_time = end_time - start_time
        repr_ops = num_iterations * len(ssl_configs)
        
        print(f"\nSSL Config String Representation Performance:")
        print(f"__str__ operations: {str_ops} in {str_time:.4f}s ({str_ops / str_time:.0f} ops/sec)")
        print(f"__repr__ operations: {repr_ops} in {repr_time:.4f}s ({repr_ops / repr_time:.0f} ops/sec)")
        
        # String operations should be fast
        assert str_ops / str_time > 10000
        assert repr_ops / repr_time > 10000
    
    def test_concurrent_scaling(self):
        """Test how concurrent SSL operations scale with thread count."""
        thread_counts = [1, 2, 4, 8]
        configs_per_thread = 100
        
        for num_threads in thread_counts:
            def create_configs(thread_id):
                configs = []
                for i in range(configs_per_thread):
                    ssl_config = SslConfig(
                        encryption_level=EncryptionLevel.Required,
                        trust_server_certificate=True,
                        server_name=f"t{thread_id}_s{i}.com"
                    )
                    configs.append(ssl_config)
                return configs
            
            start_time = time.perf_counter()
            
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = [executor.submit(create_configs, i) for i in range(num_threads)]
                
                all_configs = []
                for future in as_completed(futures):
                    configs = future.result()
                    all_configs.extend(configs)
            
            end_time = time.perf_counter()
            total_time = end_time - start_time
            total_configs = len(all_configs)
            throughput = total_configs / total_time
            
            print(f"Threads {num_threads}: {total_configs} configs in {total_time:.4f}s ({throughput:.0f} configs/sec)")
            
            assert len(all_configs) == num_threads * configs_per_thread


if __name__ == "__main__":
    # Run with specific markers for performance tests
    pytest.main([__file__, "-v", "-s"])  # -s to see print output
