"""
Test SSL configuration implementation
"""

import os
import tempfile

import pytest

from fastmssql import EncryptionLevel, SslConfig


def test_ssl_config_creation():
    """Test basic SSL config creation."""
    # When encryption is Disabled, trust settings can be None
    ssl_config = SslConfig(encryption_level=EncryptionLevel.Disabled)
    assert str(ssl_config.encryption_level) == "Disabled"
    assert not ssl_config.trust_server_certificate
    assert ssl_config.ca_certificate_path is None


def test_ssl_config_required_encryption_with_system_default():
    """Test that Required encryption can use system's default certificate store."""
    # Should succeed: Required encryption with no explicit trust settings
    # Uses system default certificate store
    ssl_config = SslConfig(
        encryption_level=EncryptionLevel.Required,
        trust_server_certificate=False,
        ca_certificate_path=None,
    )
    assert str(ssl_config.encryption_level) == "Required"
    assert not ssl_config.trust_server_certificate
    assert ssl_config.ca_certificate_path is None


def test_ssl_config_login_only_encryption_with_system_default():
    """Test that LoginOnly encryption can use system's default certificate store."""
    # Should succeed: LoginOnly encryption with no explicit trust settings
    # Uses system default certificate store
    ssl_config = SslConfig(
        encryption_level=EncryptionLevel.LoginOnly,
        trust_server_certificate=False,
        ca_certificate_path=None,
    )
    assert str(ssl_config.encryption_level) == "LoginOnly"
    assert not ssl_config.trust_server_certificate
    assert ssl_config.ca_certificate_path is None


def test_ssl_config_development():
    """Test development SSL configuration."""
    ssl_config = SslConfig.development()
    assert str(ssl_config.encryption_level) == "Required"
    assert ssl_config.trust_server_certificate


def test_ssl_config_login_only():
    """Test login-only SSL configuration."""
    ssl_config = SslConfig.login_only()
    assert str(ssl_config.encryption_level) == "LoginOnly"
    # Should NOT silently trust all server certificates
    assert not ssl_config.trust_server_certificate
    assert ssl_config.ca_certificate_path is None


def test_ssl_config_disabled():
    """Test disabled SSL configuration."""
    ssl_config = SslConfig.disabled()
    assert str(ssl_config.encryption_level) == "Disabled"


def test_ssl_config_custom():
    """Test custom SSL configuration."""
    ssl_config = SslConfig(
        encryption_level=EncryptionLevel.Required,
        trust_server_certificate=True,
        ca_certificate_path=None,
    )

    assert str(ssl_config.encryption_level) == "Required"
    assert ssl_config.trust_server_certificate
    assert ssl_config.ca_certificate_path is None


def test_ssl_config_encryption_level_case_insensitive():
    """Test that encryption level string parsing is case-insensitive."""
    # Test various case combinations
    test_cases = [
        ("required", "Required"),
        ("REQUIRED", "Required"),
        ("Required", "Required"),
        ("rEqUiReD", "Required"),
        ("loginonly", "LoginOnly"),
        ("LoginOnly", "LoginOnly"),
        ("LOGIN_ONLY", "LoginOnly"),
        ("login_only", "LoginOnly"),
        ("disabled", "Disabled"),
        ("DISABLED", "Disabled"),
        ("off", "Disabled"),
        ("OFF", "Disabled"),
    ]

    for input_val, expected in test_cases:
        ssl_config = SslConfig(encryption_level=input_val)
        assert str(ssl_config.encryption_level) == expected, f"Failed for input: {input_val}"


def test_ssl_config_encryption_level_string_vs_enum():
    """Test that encryption_level accepts both string and EncryptionLevel enum."""
    # Test with string
    ssl_config_str = SslConfig(encryption_level="Required")
    assert str(ssl_config_str.encryption_level) == "Required"

    # Test with enum
    ssl_config_enum = SslConfig(encryption_level=EncryptionLevel.Required)
    assert str(ssl_config_enum.encryption_level) == "Required"

    # Both should be equivalent
    assert str(ssl_config_str.encryption_level) == str(ssl_config_enum.encryption_level)


def test_ssl_config_all_encryption_levels_as_string():
    """Test all encryption levels can be set as strings."""
    test_cases = [
        ("required", "Required"),
        ("loginonly", "LoginOnly"),
        ("disabled", "Disabled"),
        ("off", "Disabled"),
    ]

    for input_val, expected in test_cases:
        ssl_config = SslConfig(encryption_level=input_val, trust_server_certificate=True)
        assert str(ssl_config.encryption_level) == expected


def test_ssl_config_all_encryption_levels_as_enum():
    """Test all encryption levels can be set as EncryptionLevel enum."""
    test_cases = [
        (EncryptionLevel.Required, "Required"),
        (EncryptionLevel.LoginOnly, "LoginOnly"),
        (EncryptionLevel.Disabled, "Disabled"),
    ]

    for input_val, expected in test_cases:
        ssl_config = SslConfig(encryption_level=input_val)
        assert str(ssl_config.encryption_level) == expected


def test_ssl_config_invalid_encryption_level():
    """Test SSL config with invalid encryption level."""
    with pytest.raises(ValueError, match="Invalid encryption_level"):
        SslConfig(encryption_level="Invalid")


def test_ssl_config_ca_certificate():
    """Test SSL config with CA certificate file."""
    # Create a temporary certificate file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as f:
        f.write("-----BEGIN CERTIFICATE-----\n")
        f.write("test certificate content\n")
        f.write("-----END CERTIFICATE-----\n")
        cert_path = f.name

    try:
        ssl_config = SslConfig.with_ca_certificate(cert_path)
        assert ssl_config.ca_certificate_path == cert_path
        assert not ssl_config.trust_server_certificate
    finally:
        os.unlink(cert_path)


def test_ssl_config_nonexistent_ca_certificate():
    """Test SSL config with non-existent CA certificate file."""
    with pytest.raises(Exception):  # Should raise an error about file not existing
        SslConfig.with_ca_certificate("non_existent_file.pem")


def test_ssl_config_repr():
    """Test SSL config string representation is Pythonic with key=value pairs."""
    ssl_config = SslConfig(encryption_level=EncryptionLevel.Disabled)
    repr_str = repr(ssl_config)
    assert "SslConfig" in repr_str
    assert "Disabled" in repr_str
    assert "encryption_level" in repr_str
    assert "trust_server_certificate" in repr_str
    assert "ca_certificate_path" in repr_str


def test_ssl_config_repr_with_ca_path():
    """Test that repr includes ca_certificate_path when set."""
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as f:
        f.write("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----\n")
        cert_path = f.name
    try:
        ssl_config = SslConfig.with_ca_certificate(cert_path)
        repr_str = repr(ssl_config)
        assert cert_path in repr_str
    finally:
        os.unlink(cert_path)


def test_ssl_config_equality_same_values():
    """Test that two SslConfig instances with identical values compare equal."""
    a = SslConfig(encryption_level=EncryptionLevel.Required, trust_server_certificate=False)
    b = SslConfig(encryption_level=EncryptionLevel.Required, trust_server_certificate=False)
    assert a == b


def test_ssl_config_equality_different_encryption():
    """Test that configs with different encryption levels are not equal."""
    a = SslConfig(encryption_level=EncryptionLevel.Required)
    b = SslConfig(encryption_level=EncryptionLevel.Disabled)
    assert a != b


def test_ssl_config_equality_different_trust():
    """Test that configs with different trust_server_certificate are not equal."""
    a = SslConfig(encryption_level=EncryptionLevel.Required, trust_server_certificate=True)
    b = SslConfig(encryption_level=EncryptionLevel.Required, trust_server_certificate=False)
    assert a != b


def test_ssl_config_equality_factory_methods():
    """Test equality between factory-method instances and equivalent manual configs."""
    assert SslConfig.development() == SslConfig(
        encryption_level=EncryptionLevel.Required, trust_server_certificate=True
    )
    assert SslConfig.login_only() == SslConfig(
        encryption_level=EncryptionLevel.LoginOnly, trust_server_certificate=False
    )
    assert SslConfig.disabled() == SslConfig(
        encryption_level=EncryptionLevel.Disabled, trust_server_certificate=False
    )


def test_ssl_config_inequality_with_non_ssl_object():
    """Test that SslConfig does not equal arbitrary Python objects."""
    ssl_config = SslConfig.development()
    assert ssl_config != "not an SslConfig"
    assert ssl_config != 42
    assert ssl_config != None  # noqa: E711
