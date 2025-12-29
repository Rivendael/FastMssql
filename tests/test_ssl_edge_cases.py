"""
Tests for SSL/TLS edge cases, error conditions, and boundary scenarios.
"""

import pytest
import tempfile
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from fastmssql import SslConfig, EncryptionLevel
except ImportError as e:
    pytest.fail(f"Cannot import mssql library: {e}", allow_module_level=True)


class TestSslConfigThreadSafety:
    """Test SSL configuration thread safety."""
    
    def test_concurrent_ssl_config_creation(self):
        """Test creating SSL configs concurrently from multiple threads."""
        results = []
        errors = []
        
        def create_ssl_config(thread_id):
            try:
                ssl_config = SslConfig(
                    encryption_level=EncryptionLevel.Required,
                    trust_server_certificate=True,
                    server_name=f"server{thread_id}.com"
                )
                results.append((thread_id, ssl_config))
                return ssl_config
            except Exception as e:
                errors.append((thread_id, e))
                raise
        
        # Create SSL configs from 10 concurrent threads
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(create_ssl_config, i) for i in range(10)]
            
            for future in as_completed(futures):
                future.result()  # Will raise if there was an exception
        
        assert len(results) == 10
        assert len(errors) == 0
        
        # Verify all configs are independent
        for i, (thread_id, ssl_config) in enumerate(results):
            assert ssl_config.server_name == f"server{thread_id}.com"
    
    def test_concurrent_ssl_config_property_access(self):
        """Test accessing SSL config properties concurrently."""
        ssl_config = SslConfig(
            encryption_level=EncryptionLevel.Required,
            trust_server_certificate=True,
            enable_sni=True,
            server_name="test.server.com"
        )
        
        results = []
        errors = []
        
        def access_properties(thread_id):
            try:
                for _ in range(100):
                    # Access all properties
                    encryption = ssl_config.encryption_level
                    trust = ssl_config.trust_server_certificate
                    sni = ssl_config.enable_sni
                    server = ssl_config.server_name
                    ca_path = ssl_config.ca_certificate_path
                    
                    # Verify consistency
                    assert encryption == "Required"
                    assert trust is True
                    assert sni is True
                    assert server == "test.server.com"
                    assert ca_path is None
                
                results.append(thread_id)
            except Exception as e:
                errors.append((thread_id, e))
                raise
        
        # Access properties from 5 concurrent threads
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(access_properties, i) for i in range(5)]
            
            for future in as_completed(futures):
                future.result()
        
        assert len(results) == 5
        assert len(errors) == 0

class TestSslConfigMemoryManagement:
    """Test SSL configuration memory management."""
    
    def test_ssl_config_memory_leak_prevention(self):
        """Test that creating many SSL configs doesn't cause memory leaks."""
        import gc
        
        initial_objects = len(gc.get_objects())
        
        # Create and discard many SSL configs
        for i in range(1000):
            ssl_config = SslConfig(
                encryption_level=EncryptionLevel.Required,
                trust_server_certificate=True,
                server_name=f"server{i}.com"
            )
            # Config goes out of scope here
        
        # Force garbage collection
        gc.collect()
        
        final_objects = len(gc.get_objects())
        
        # Should not have created a significant number of persistent objects
        object_difference = final_objects - initial_objects
        assert object_difference < 100  # Reasonable threshold
    
    def test_ssl_config_with_large_server_names(self):
        """Test SSL config with very large server names."""
        large_server_names = [
            "a" * 1000,  # 1KB server name
            "b" * 10000,  # 10KB server name
            "ñ" * 1000,  # Unicode characters
        ]
        
        for server_name in large_server_names:
            ssl_config = SslConfig(encryption_level=EncryptionLevel.Off, server_name=server_name)
            assert ssl_config.server_name == server_name
    
    def test_ssl_config_rapid_creation_and_destruction(self):
        """Test rapid creation and destruction of SSL configs."""
        for _ in range(10000):
            ssl_config = SslConfig.development()
            # Immediately goes out of scope


class TestSslConfigExtremeInputs:
    """Test SSL configuration with extreme or unusual inputs."""
    
    def test_ssl_config_with_null_bytes_in_server_name(self):
        """Test SSL config with null bytes in server name."""
        server_name_with_nulls = "server\x00name.com"
        
        ssl_config = SslConfig(encryption_level=EncryptionLevel.Off, server_name=server_name_with_nulls)
        assert ssl_config.server_name == server_name_with_nulls
    
    def test_ssl_config_with_control_characters(self):
        """Test SSL config with control characters in server name."""
        server_name_with_controls = "server\t\n\rname.com"
        
        ssl_config = SslConfig(encryption_level=EncryptionLevel.Off, server_name=server_name_with_controls)
        assert ssl_config.server_name == server_name_with_controls
    
    def test_ssl_config_with_very_long_paths(self):
        """Test SSL config with extremely long certificate paths."""
        # Create a path that's near the filesystem limit
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Create nested directories to make a long path
            current_path = temp_dir
            path_parts = []
            
            # Create path segments until we get close to limit (but not over)
            for i in range(20):  # Should be safe on most systems
                part = f"very_long_directory_name_{i}" + "x" * 30  # Reduced size
                new_path = os.path.join(current_path, part)
                
                # Stop if path is getting too long (Windows has ~260 char limit)
                if len(new_path) > 220:  # Conservative limit
                    break
                    
                path_parts.append(part)
                current_path = new_path
                os.makedirs(current_path, exist_ok=True)
            
            cert_path = os.path.join(current_path, "cert.pem")
            
            # Ensure the path isn't too long for the final file
            if len(cert_path) > 250:
                # Use a shorter filename if needed
                cert_path = os.path.join(current_path, "c.pem")
            
            # Create certificate file
            with open(cert_path, 'w') as f:
                f.write("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----")
            
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
            
        finally:
            # Clean up
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_ssl_config_with_numeric_server_names(self):
        """Test SSL config with purely numeric server names."""
        numeric_names = [
            "123456789",
            "192.168.1.1",
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334",  # IPv6
            "12345.67890.11111.22222"
        ]
        
        for name in numeric_names:
            ssl_config = SslConfig(encryption_level=EncryptionLevel.Off, server_name=name)
            assert ssl_config.server_name == name
    
    def test_ssl_config_with_special_characters_server_names(self):
        """Test SSL config with special characters in server names."""
        special_names = [
            "server!@#$%^&*().com",
            "server[]{};:\"',./<>?com",
            "server~`+=_-|\\com",
            "тест.сервер.ком",  # Cyrillic
            "テスト.サーバー.コム",  # Japanese
            "测试.服务器.com",  # Chinese
        ]
        
        for name in special_names:
            ssl_config = SslConfig(encryption_level=EncryptionLevel.Off, server_name=name)
            assert ssl_config.server_name == name


class TestSslConfigErrorRecovery:
    """Test SSL configuration error recovery scenarios."""
    
    def test_ssl_config_after_failed_creation(self):
        """Test creating valid SSL config after failed creation attempts."""
        # First, try to create invalid configs
        for _ in range(5):
            try:
                SslConfig(encryption_level="InvalidLevel")
                assert False, "Should have raised an error"
            except ValueError:
                pass  # Expected
        
        # Now create a valid config
        ssl_config = SslConfig(encryption_level=EncryptionLevel.Required, trust_server_certificate=True)
        assert str(ssl_config.encryption_level) == "Required"

    def test_ssl_config_with_intermittent_file_access(self):
        """Test SSL config creation when certificate file access is intermittent."""
        import os
        
        # Skip if running as root (common in CI environments) - only on POSIX systems
        if hasattr(os, 'getuid') and os.getuid() == 0:
            pytest.fail("Running as root - file permissions are not enforced")
            
        content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(content)
            cert_path = f.name
        
        try:
            # Create SSL config successfully
            ssl_config1 = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config1.ca_certificate_path == cert_path
            
            # Temporarily make file inaccessible (if on POSIX)
            if os.name != 'nt':
                original_mode = os.stat(cert_path).st_mode
                os.chmod(cert_path, 0o000)
                
                try:
                    # Verify permissions are actually restricted
                    stat_info = os.stat(cert_path)
                    if stat_info.st_mode & 0o777 != 0o000:
                        pytest.fail("Unable to restrict file permissions in this environment")
                    
                    # Test that the SSL config raises an exception
                    with pytest.raises(Exception):
                        SslConfig(ca_certificate_path=cert_path)
                finally:
                    # Restore access
                    os.chmod(cert_path, original_mode)
            
            # Should work again after restoring access
            ssl_config2 = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config2.ca_certificate_path == cert_path
            
        finally:
            os.unlink(cert_path)
    
    def test_ssl_config_creation_with_transient_errors(self):
        """Test SSL config creation resilience to transient errors."""
        # Simulate transient file system issues
        temp_dir = tempfile.mkdtemp()
        cert_path = os.path.join(temp_dir, "cert.pem")
        
        try:
            # Create certificate file
            with open(cert_path, 'w') as f:
                f.write("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----")
            
            # Should succeed
            ssl_config1 = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config1.ca_certificate_path == cert_path
            
            # Remove and recreate file
            os.unlink(cert_path)
            
            # Should fail now
            with pytest.raises(Exception):
                SslConfig(ca_certificate_path=cert_path)
            
            # Recreate file
            with open(cert_path, 'w') as f:
                f.write("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----")
            
            # Should succeed again
            ssl_config2 = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config2.ca_certificate_path == cert_path
            
        finally:
            if os.path.exists(cert_path):
                os.unlink(cert_path)
            os.rmdir(temp_dir)


class TestSslConfigResourceExhaustion:
    """Test SSL configuration under resource exhaustion conditions."""
    
    def test_ssl_config_with_many_open_files(self):
        """Test SSL config creation when many files are open."""
        # Open many files to simulate resource pressure
        open_files = []
        temp_files = []
        
        try:
            # Open many temporary files
            for i in range(100):  # Reasonable number to avoid hitting system limits
                temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
                temp_file.write(f"temp file {i}")
                temp_files.append(temp_file.name)
                open_files.append(temp_file)
            
            # Now try to create SSL config with certificate
            cert_content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
            with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
                f.write(cert_content)
                cert_path = f.name
            
            try:
                ssl_config = SslConfig(ca_certificate_path=cert_path)
                assert ssl_config.ca_certificate_path == cert_path
            finally:
                os.unlink(cert_path)
                
        finally:
            # Clean up open files
            for f in open_files:
                try:
                    f.close()
                except:
                    pass
            
            for temp_file in temp_files:
                try:
                    os.unlink(temp_file)
                except:
                    pass
    
    def test_ssl_config_under_memory_pressure(self):
        """Test SSL config creation under simulated memory pressure."""
        # Create large objects to simulate memory pressure
        large_objects = []
        
        try:
            # Allocate some memory (but not enough to crash the test)
            for i in range(10):
                large_obj = bytearray(1024 * 1024)  # 1MB each
                large_objects.append(large_obj)
            
            # Should still be able to create SSL configs
            ssl_config = SslConfig.development()
            assert ssl_config.trust_server_certificate is True
            
        finally:
            # Clean up memory
            large_objects.clear()


class TestSslConfigCompatibility:
    """Test SSL configuration compatibility scenarios."""
    
    def test_ssl_config_with_different_python_versions(self):
        """Test SSL config behavior is consistent across Python versions."""
        # This test mainly ensures our implementation doesn't rely on
        # version-specific Python features
        
        ssl_config = SslConfig(
            encryption_level=EncryptionLevel.Required,
            trust_server_certificate=True,
            enable_sni=True,
            server_name="test.com"
        )
        
        # Test that all basic operations work
        assert str(ssl_config.encryption_level) == "Required"
        assert ssl_config.trust_server_certificate is True
        assert ssl_config.enable_sni is True
        assert ssl_config.server_name == "test.com"
        
        # Test string representations
        str_repr = str(ssl_config)
        repr_str = repr(ssl_config)
        
        assert len(str_repr) > 0
        assert len(repr_str) > 0
    
    def test_ssl_config_with_different_encodings(self):
        """Test SSL config with different character encodings."""
        # Test various Unicode characters in server names
        unicode_names = [
            "café.com",  # Latin-1 supplement
            "москва.рф",  # Cyrillic
            "東京.jp",  # Japanese
            "münchen.de",  # German umlauts
            "åløb.dk",  # Danish/Norwegian
            "ñoño.es",  # Spanish
        ]
        
        for name in unicode_names:
            ssl_config = SslConfig(encryption_level=EncryptionLevel.Off, server_name=name)
            assert ssl_config.server_name == name
    
    def test_ssl_config_cross_platform_paths(self):
        """Test SSL config with different path formats."""
        content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(content)
            cert_path = f.name
        
        try:
            # Test with the original path
            ssl_config1 = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config1.ca_certificate_path == cert_path
            
            # Test with normalized path
            normalized_path = os.path.normpath(cert_path)
            ssl_config2 = SslConfig(ca_certificate_path=normalized_path)
            assert ssl_config2.ca_certificate_path == normalized_path
            
            # Test with absolute path
            abs_path = os.path.abspath(cert_path)
            ssl_config3 = SslConfig(ca_certificate_path=abs_path)
            assert ssl_config3.ca_certificate_path == abs_path
            
        finally:
            os.unlink(cert_path)


class TestSslConfigBoundaryConditions:
    """Test SSL configuration boundary conditions."""
    
    def test_ssl_config_empty_string_inputs(self):
        """Test SSL config with empty string inputs."""
        # Empty server name should be allowed
        ssl_config = SslConfig(encryption_level=EncryptionLevel.Off, server_name="")
        assert ssl_config.server_name == ""
    
    def test_ssl_config_none_inputs(self):
        """Test SSL config with None inputs."""
        ssl_config = SslConfig(
            encryption_level=EncryptionLevel.Off,  # Use Off to avoid trust requirement
            server_name=None,
            ca_certificate_path=None
        )

        assert str(ssl_config.encryption_level) == "Off"  # Set to Off
        assert ssl_config.server_name is None
        assert ssl_config.ca_certificate_path is None
    
    def test_ssl_config_boolean_edge_cases(self):
        """Test SSL config with boolean edge cases."""
        # Test with explicit boolean values
        ssl_config1 = SslConfig(
            trust_server_certificate=True,
            enable_sni=True
        )
        assert ssl_config1.trust_server_certificate is True
        assert ssl_config1.enable_sni is True
        
        ssl_config2 = SslConfig(
            encryption_level=EncryptionLevel.Off,  # Use Off to avoid trust requirement
            trust_server_certificate=False,
            enable_sni=False
        )
        assert ssl_config2.trust_server_certificate is False
        assert ssl_config2.enable_sni is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
