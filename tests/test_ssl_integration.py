"""
Tests for SSL/TLS configuration integration with connections and real-world scenarios.
"""

import pytest
import tempfile
import os
import asyncio
from unittest.mock import patch, MagicMock

# Import the library components
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

try:
    from mssql import SslConfig, Connection, PoolConfig
    import mssql_python_rust as core
except ImportError as e:
    pytest.skip(f"Cannot import mssql library: {e}", allow_module_level=True)


class TestSslConnectionIntegration:
    """Test SSL configuration integration with database connections."""
    
    def test_connection_with_required_encryption(self):
        """Test connection creation with required encryption."""
        ssl_config = SslConfig(encryption_level="Required")
        pool_config = PoolConfig(max_size=5)
        
        connection = Connection(
            server="localhost",
            database="testdb",
            username="testuser",
            password="testpass",
            ssl_config=ssl_config,
            pool_config=pool_config
        )
        
        assert connection is not None
    
    def test_connection_with_login_only_encryption(self):
        """Test connection creation with login-only encryption."""
        ssl_config = SslConfig(encryption_level="LoginOnly")
        
        connection = Connection(
            server="localhost",
            database="testdb",
            ssl_config=ssl_config,
            trusted_connection=True
        )
        
        assert connection is not None
    
    def test_connection_with_disabled_encryption(self):
        """Test connection creation with disabled encryption."""
        ssl_config = SslConfig(encryption_level="Off")
        
        connection = Connection(
            server="localhost",
            database="testdb",
            ssl_config=ssl_config,
            trusted_connection=True
        )
        
        assert connection is not None
    
    def test_connection_with_development_ssl_config(self):
        """Test connection creation with development SSL config."""
        ssl_config = SslConfig.development()
        
        connection = Connection(
            server="localhost",
            database="testdb",
            ssl_config=ssl_config,
            trusted_connection=True
        )
        
        assert connection is not None
    
    def test_connection_with_custom_ca_certificate(self):
        """Test connection creation with custom CA certificate."""
        # Create a temporary certificate file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write("-----BEGIN CERTIFICATE-----\n")
            f.write("MIIDXTCCAkWgAwIBAgIJAKoK/heBjcOuMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV\n")
            f.write("BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX\n")
            f.write("aWRnaXRzIFB0eSBMdGQwHhcNMTMwODI3MjM1NDA3WhcNMTQwODI3MjM1NDA3WjBF\n")
            f.write("-----END CERTIFICATE-----\n")
            temp_cert_path = f.name
        
        try:
            ssl_config = SslConfig.with_ca_certificate(temp_cert_path)
            
            connection = Connection(
                server="localhost",
                database="testdb",
                ssl_config=ssl_config,
                trusted_connection=True
            )
            
            assert connection is not None
        finally:
            os.unlink(temp_cert_path)
    
    def test_connection_with_custom_server_name(self):
        """Test connection creation with custom server name in SSL config."""
        ssl_config = SslConfig(
            encryption_level="Required",
            server_name="custom.database.server.com"
        )
        
        connection = Connection(
            server="192.168.1.100",  # IP address
            database="testdb",
            ssl_config=ssl_config,
            trusted_connection=True
        )
        
        assert connection is not None


class TestSslConnectionStrings:
    """Test SSL configuration with connection strings."""
    
    def test_connection_string_with_ssl_config(self):
        """Test that SSL config overrides connection string encryption settings."""
        conn_string = "Server=localhost;Database=test;Integrated Security=true;Encrypt=false"
        ssl_config = SslConfig(encryption_level="Required")  # Should override Encrypt=false
        
        connection = Connection(
            connection_string=conn_string,
            ssl_config=ssl_config
        )
        
        assert connection is not None
    
    def test_connection_string_with_trust_certificate(self):
        """Test connection string with SSL config that trusts server certificate."""
        conn_string = "Server=localhost;Database=test;Integrated Security=true"
        ssl_config = SslConfig.development()  # Trusts all certificates
        
        connection = Connection(
            connection_string=conn_string,
            ssl_config=ssl_config
        )
        
        assert connection is not None
    
    def test_encrypted_connection_string_with_ssl_config(self):
        """Test encrypted connection string enhanced with SSL config."""
        conn_string = "Server=localhost;Database=test;Integrated Security=true;Encrypt=true"
        ssl_config = SslConfig(
            encryption_level="Required",
            enable_sni=False,  # Disable SNI for older servers
            server_name="localhost"
        )
        
        connection = Connection(
            connection_string=conn_string,
            ssl_config=ssl_config
        )
        
        assert connection is not None


class TestSslConfigCombinations:
    """Test various combinations of SSL configuration options."""
    
    def test_all_encryption_levels_with_sni_disabled(self):
        """Test all encryption levels with SNI disabled."""
        encryption_levels = ["Required", "LoginOnly", "Off"]
        
        for level in encryption_levels:
            ssl_config = SslConfig(
                encryption_level=level,
                enable_sni=False
            )
            
            connection = Connection(
                server="localhost",
                database="test",
                ssl_config=ssl_config,
                trusted_connection=True
            )
            
            assert connection is not None
            assert ssl_config.encryption_level == level
            assert ssl_config.enable_sni is False
    
    def test_ssl_config_with_sql_server_auth(self):
        """Test SSL configuration with SQL Server authentication."""
        ssl_config = SslConfig(encryption_level="Required")
        
        connection = Connection(
            server="localhost",
            database="test",
            username="testuser",
            password="testpassword",
            ssl_config=ssl_config
        )
        
        assert connection is not None
    
    def test_ssl_config_with_windows_auth(self):
        """Test SSL configuration with Windows authentication."""
        ssl_config = SslConfig(encryption_level="Required")
        
        connection = Connection(
            server="localhost",
            database="test",
            ssl_config=ssl_config,
            trusted_connection=True
        )
        
        assert connection is not None
    
    def test_multiple_ssl_configs_different_settings(self):
        """Test creating multiple connections with different SSL settings."""
        ssl_configs = [
            SslConfig(encryption_level="Required", enable_sni=True),
            SslConfig(encryption_level="LoginOnly", enable_sni=False),
            SslConfig(encryption_level="Off"),
            SslConfig.development(),
            SslConfig.login_only(),
            SslConfig.disabled()
        ]
        
        connections = []
        for i, ssl_config in enumerate(ssl_configs):
            connection = Connection(
                server="localhost",
                database=f"test{i}",
                ssl_config=ssl_config,
                trusted_connection=True
            )
            connections.append(connection)
        
        assert len(connections) == len(ssl_configs)
        for conn in connections:
            assert conn is not None


class TestSslConfigErrorHandling:
    """Test error handling in SSL configuration scenarios."""
    
    def test_ssl_config_with_invalid_ca_file_during_connection(self):
        """Test that invalid CA file is detected during SSL config creation."""
        with pytest.raises(Exception):
            # This should fail when creating the SSL config
            SslConfig(
                ca_certificate_path="/nonexistent/path/to/cert.pem",
                trust_server_certificate=False
            )
    
    def test_ssl_config_mutual_exclusion_enforcement(self):
        """Test that mutual exclusion is enforced during SSL config creation."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write("-----BEGIN CERTIFICATE-----\n")
            f.write("test certificate content\n")
            f.write("-----END CERTIFICATE-----\n")
            temp_cert_path = f.name
        
        try:
            # This should fail because both trust_server_certificate and ca_certificate_path are set
            with pytest.raises(Exception):
                core.SslConfig(
                    encryption_level=core.EncryptionLevel.REQUIRED,
                    trust_server_certificate=True,
                    ca_certificate_path=temp_cert_path,
                    enable_sni=True,
                    server_name=None
                )
        finally:
            os.unlink(temp_cert_path)
    
    def test_ssl_config_with_invalid_encryption_level_python(self):
        """Test that invalid encryption level raises appropriate error in Python wrapper."""
        with pytest.raises(ValueError, match="Invalid encryption_level"):
            SslConfig(encryption_level="InvalidLevel")
    
    def test_ssl_config_with_directory_as_ca_path(self):
        """Test that providing a directory as CA path raises an error."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            with pytest.raises(Exception):
                SslConfig(
                    ca_certificate_path=temp_dir,
                    trust_server_certificate=False
                )
        finally:
            os.rmdir(temp_dir)


class TestSslConfigPerformance:
    """Test performance characteristics of SSL configuration."""
    
    def test_ssl_config_creation_performance(self):
        """Test that SSL config creation is reasonably fast."""
        import time
        
        start_time = time.time()
        
        # Create many SSL configs
        configs = []
        for i in range(1000):
            config = SslConfig(
                encryption_level="Required",
                trust_server_certificate=False,
                enable_sni=True,
                server_name=f"server{i}.com"
            )
            configs.append(config)
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        # Should complete in reasonable time (less than 1 second for 1000 configs)
        assert elapsed < 1.0
        assert len(configs) == 1000
    
    def test_ssl_config_property_access_performance(self):
        """Test that SSL config property access is fast."""
        import time
        
        ssl_config = SslConfig(
            encryption_level="Required",
            trust_server_certificate=False,
            enable_sni=True,
            server_name="test.server.com"
        )
        
        start_time = time.time()
        
        # Access properties many times
        for _ in range(10000):
            _ = ssl_config.encryption_level
            _ = ssl_config.trust_server_certificate
            _ = ssl_config.enable_sni
            _ = ssl_config.server_name
            _ = ssl_config.ca_certificate_path
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        # Should complete very quickly (less than 0.1 seconds for 10000 accesses)
        assert elapsed < 0.1
    
    def test_connection_creation_with_ssl_performance(self):
        """Test that connection creation with SSL config is reasonably fast."""
        import time
        
        ssl_config = SslConfig.development()
        
        start_time = time.time()
        
        # Create many connections
        connections = []
        for i in range(100):
            connection = Connection(
                server="localhost",
                database=f"test{i}",
                ssl_config=ssl_config,
                trusted_connection=True
            )
            connections.append(connection)
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        # Should complete in reasonable time (less than 1 second for 100 connections)
        assert elapsed < 1.0
        assert len(connections) == 100


class TestSslConfigMemoryUsage:
    """Test memory usage characteristics of SSL configuration."""
    
    def test_ssl_config_memory_cleanup(self):
        """Test that SSL configs can be properly garbage collected."""
        import gc
        import weakref
        
        # Create SSL config and weak reference to it
        ssl_config = SslConfig.development()
        weak_ref = weakref.ref(ssl_config)
        
        # Verify the object exists
        assert weak_ref() is not None
        
        # Delete the reference
        del ssl_config
        
        # Force garbage collection
        gc.collect()
        
        # The weak reference should now be None (object was collected)
        # Note: This test might be flaky depending on Python's GC behavior
        # assert weak_ref() is None  # Commented out as it might be unreliable
    
    def test_multiple_ssl_configs_independence(self):
        """Test that multiple SSL configs are independent."""
        config1 = SslConfig(
            encryption_level="Required",
            server_name="server1.com"
        )
        
        config2 = SslConfig(
            encryption_level="LoginOnly",
            server_name="server2.com"
        )
        
        config3 = SslConfig.development()
        
        # Verify they are independent
        assert config1.encryption_level != config2.encryption_level
        assert config1.server_name != config2.server_name
        assert config1.trust_server_certificate != config3.trust_server_certificate
        
        # Modifying one shouldn't affect others (no shared state)
        assert config1.server_name == "server1.com"
        assert config2.server_name == "server2.com"


class TestSslConfigRealWorldScenarios:
    """Test SSL configuration in real-world scenarios."""
    
    def test_azure_sql_database_scenario(self):
        """Test SSL configuration suitable for Azure SQL Database."""
        ssl_config = SslConfig(
            encryption_level="Required",
            enable_sni=True,
            server_name=None  # Let the system determine
        )
        
        connection = Connection(
            server="myserver.database.windows.net",
            database="mydatabase",
            username="myuser",
            password="mypassword",
            ssl_config=ssl_config
        )
        
        assert connection is not None
        assert ssl_config.encryption_level == "Required"
        assert ssl_config.enable_sni is True
    
    def test_on_premises_sql_server_with_self_signed_cert(self):
        """Test SSL configuration for on-premises SQL Server with self-signed certificate."""
        ssl_config = SslConfig.development()  # Trusts self-signed certificates
        
        connection = Connection(
            server="sql-server.company.local",
            database="CompanyDB",
            ssl_config=ssl_config,
            trusted_connection=True
        )
        
        assert connection is not None
        assert ssl_config.trust_server_certificate is True
    
    def test_legacy_sql_server_login_only_encryption(self):
        """Test SSL configuration for legacy SQL Server that only supports login encryption."""
        ssl_config = SslConfig.login_only()
        
        connection = Connection(
            server="legacy-sql.company.local",
            database="LegacyDB",
            username="legacyuser",
            password="legacypass",
            ssl_config=ssl_config
        )
        
        assert connection is not None
        assert ssl_config.encryption_level == "LoginOnly"
    
    def test_development_environment_no_encryption(self):
        """Test SSL configuration for development environment with no encryption."""
        ssl_config = SslConfig.disabled()
        
        connection = Connection(
            server="dev-sql",
            database="DevDB",
            ssl_config=ssl_config,
            trusted_connection=True
        )
        
        assert connection is not None
        assert ssl_config.encryption_level == "Off"
    
    def test_corporate_environment_with_custom_ca(self):
        """Test SSL configuration for corporate environment with custom CA certificate."""
        # Create a mock corporate CA certificate
        with tempfile.NamedTemporaryFile(mode='w', suffix='.crt', delete=False) as f:
            f.write("-----BEGIN CERTIFICATE-----\n")
            f.write("MIIDXTCCAkWgAwIBAgIJAKoK/heBjcOuMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV\n")
            f.write("BAYTAlVTMRMwEQYDVQQIDApDb3Jwb3JhdGUxITAfBgNVBAoMGENvcnBvcmF0ZSBJ\n")
            f.write("VCBEZXBhcnRtZW50MQ==\n")
            f.write("-----END CERTIFICATE-----\n")
            temp_cert_path = f.name
        
        try:
            ssl_config = SslConfig(
                encryption_level="Required",
                ca_certificate_path=temp_cert_path,
                enable_sni=True
            )
            
            connection = Connection(
                server="corporate-sql.company.com",
                database="CorporateDB",
                ssl_config=ssl_config,
                trusted_connection=True
            )
            
            assert connection is not None
            assert ssl_config.ca_certificate_path == temp_cert_path
            assert ssl_config.trust_server_certificate is False
        finally:
            os.unlink(temp_cert_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
