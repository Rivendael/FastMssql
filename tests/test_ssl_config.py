"""
Test SSL configuration implementation
"""

import pytest
import tempfile
import os
from fastmssql import SslConfig


def test_ssl_config_creation():
    """Test basic SSL config creation."""
    # Default configuration
    ssl_config = SslConfig()
    assert ssl_config.encryption_level == "Required"
    assert ssl_config.trust_server_certificate == False
    assert ssl_config.ca_certificate_path is None
    assert ssl_config.enable_sni == True
    assert ssl_config.server_name is None


def test_ssl_config_development():
    """Test development SSL configuration."""
    ssl_config = SslConfig.development()
    assert ssl_config.encryption_level == "Required"
    assert ssl_config.trust_server_certificate == True
    assert ssl_config.enable_sni == False


def test_ssl_config_login_only():
    """Test login-only SSL configuration."""
    ssl_config = SslConfig.login_only()
    assert ssl_config.encryption_level == "LoginOnly"


def test_ssl_config_disabled():
    """Test disabled SSL configuration."""
    ssl_config = SslConfig.disabled()
    assert ssl_config.encryption_level == "Off"


def test_ssl_config_custom():
    """Test custom SSL configuration."""
    ssl_config = SslConfig(
        encryption_level="Required",
        trust_server_certificate=False,
        ca_certificate_path=None,
        enable_sni=True,
        server_name="custom.server.com"
    )
    
    assert ssl_config.encryption_level == "Required"
    assert ssl_config.trust_server_certificate == False
    assert ssl_config.ca_certificate_path is None
    assert ssl_config.enable_sni == True
    assert ssl_config.server_name == "custom.server.com"


def test_ssl_config_invalid_encryption_level():
    """Test SSL config with invalid encryption level."""
    with pytest.raises(ValueError):
        SslConfig(encryption_level="Invalid")


def test_ssl_config_ca_certificate():
    """Test SSL config with CA certificate file."""
    # Create a temporary certificate file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
        f.write("-----BEGIN CERTIFICATE-----\n")
        f.write("test certificate content\n")
        f.write("-----END CERTIFICATE-----\n")
        cert_path = f.name
    
    try:
        ssl_config = SslConfig.with_ca_certificate(cert_path)
        assert ssl_config.ca_certificate_path == cert_path
        assert ssl_config.trust_server_certificate == False
    finally:
        os.unlink(cert_path)


def test_ssl_config_nonexistent_ca_certificate():
    """Test SSL config with non-existent CA certificate file."""
    with pytest.raises(Exception):  # Should raise an error about file not existing
        SslConfig.with_ca_certificate("non_existent_file.pem")


def test_ssl_config_repr():
    """Test SSL config string representation."""
    ssl_config = SslConfig()
    repr_str = repr(ssl_config)
    assert "SslConfig" in repr_str
    assert "Required" in repr_str


if __name__ == "__main__":
    # Run tests manually if not using pytest
    print("Running SSL configuration tests...")
    
    try:
        test_ssl_config_creation()
        print("✅ test_ssl_config_creation passed")
        
        test_ssl_config_development()
        print("✅ test_ssl_config_development passed")
        
        test_ssl_config_login_only()
        print("✅ test_ssl_config_login_only passed")
        
        test_ssl_config_disabled()
        print("✅ test_ssl_config_disabled passed")
        
        test_ssl_config_custom()
        print("✅ test_ssl_config_custom passed")
        
        test_ssl_config_invalid_encryption_level()
        print("✅ test_ssl_config_invalid_encryption_level passed")
        
        test_ssl_config_ca_certificate()
        print("✅ test_ssl_config_ca_certificate passed")
        
        test_ssl_config_nonexistent_ca_certificate()
        print("✅ test_ssl_config_nonexistent_ca_certificate passed")
        
        test_ssl_config_repr()
        print("✅ test_ssl_config_repr passed")
        
        print("\n🎉 All tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
