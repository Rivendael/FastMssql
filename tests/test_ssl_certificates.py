"""
Comprehensive tests for SSL/TLS certificate handling and validation.
"""

import pytest
import tempfile
import os

try:
    from fastmssql import SslConfig
except ImportError as e:
    pytest.fail(f"Cannot import mssql library: {e}", allow_module_level=True)


class TestCertificateFileFormats:
    """Test different certificate file formats and validations."""
    
    def test_pem_format_certificate(self):
        """Test PEM format certificate file."""
        pem_content = """-----BEGIN CERTIFICATE-----
MIIDXTCCAkWgAwIBAgIJAKoK/heBjcOuMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTMwODI3MjM1NDA3WhcNMTQwODI3MjM1NDA3WjBF
MQswCQYDVQQGEwJBVTETMBEGA1UECAwKU29tZS1TdGF0ZTEhMB8GA1UECgwYSW50
ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
CgKCAQEAuuAlbO3xfpQlVceIcMFuelling3D4VYHBVr24zriGazHlxlX2dFBERT
-----END CERTIFICATE-----"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(pem_content)
            cert_path = f.name
        
        try:
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)
    
    def test_crt_format_certificate(self):
        """Test CRT format certificate file."""
        crt_content = """-----BEGIN CERTIFICATE-----
MIIE5zCCAs+gAwIBAgIJANmqUxc9vwi8MA0GCSqGSIb3DQEBCwUAMIGJMQswCQYD
VQQGEwJVUzELMAkGA1UECAwCQ0ExFjAUBgNVBAcMDU1vdW50YWluIFZpZXcxFDAS
BgNVBAoMC1BheVBhbCBJbmMuMRMwEQYDVQQLDApzYW5kYm94XzEwMTQwMgYDVQQD
DCtzYW5kYm94XzEwLnNhbmRib3gucGF5cGFsLmNvbTAeFw0xNDAzMDUwMDAwMDBa
Fw0zNDAzMDUyMzU5NTlaaa123bbcA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQEL
-----END CERTIFICATE-----"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.crt', delete=False) as f:
            f.write(crt_content)
            cert_path = f.name
        
        try:
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)
    
    def test_der_format_certificate(self):
        """Test DER format certificate file."""
        # Simulate DER binary data (not a real certificate, but binary format)
        der_data = bytes([
            0x30, 0x82, 0x03, 0x55, 0x30, 0x82, 0x02, 0x3d, 0xa0, 0x03, 0x02, 0x01, 0x02,
            0x02, 0x09, 0x00, 0xd9, 0xaa, 0x53, 0x17, 0x3d, 0xbf, 0x08, 0xbc, 0x30, 0x0d,
            0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0b, 0x05, 0x00
        ])
        
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.der', delete=False) as f:
            f.write(der_data)
            cert_path = f.name
        
        try:
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)
    
    def test_invalid_certificate_extension(self):
        """Test that invalid certificate file extensions are rejected."""
        content = "Some certificate content"
        
        invalid_extensions = ['.txt', '.doc', '.pdf', '.key', '.p12', '.pfx']
        
        for ext in invalid_extensions:
            with tempfile.NamedTemporaryFile(mode='w', suffix=ext, delete=False) as f:
                f.write(content)
                cert_path = f.name
            
            try:
                with pytest.raises(Exception):
                    SslConfig(ca_certificate_path=cert_path)
            finally:
                os.unlink(cert_path)
    
    def test_certificate_file_without_extension(self):
        """Test certificate file without extension."""
        content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='', delete=False) as f:
            f.write(content)
            cert_path = f.name
        
        try:
            with pytest.raises(Exception):
                SslConfig(ca_certificate_path=cert_path)
        finally:
            os.unlink(cert_path)


class TestCertificateFileValidation:
    """Test certificate file validation scenarios."""
    
    def test_empty_certificate_file(self):
        """Test empty certificate file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write("")  # Empty file
            cert_path = f.name
        
        try:
            # Should still pass validation at creation time (content validation happens later)
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)
    
    def test_malformed_pem_certificate(self):
        """Test malformed PEM certificate."""
        malformed_content = """-----BEGIN CERTIFICATE-----
This is not a valid certificate content
It's missing proper base64 encoding
-----END CERTIFICATE-----"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(malformed_content)
            cert_path = f.name
        
        try:
            # Should pass creation (validation happens during connection)
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)
    
    def test_certificate_with_extra_content(self):
        """Test certificate file with extra content."""
        content_with_extra = """Some extra content at the beginning
-----BEGIN CERTIFICATE-----
MIIDXTCCAkWgAwIBAgIJAKoK/heBjcOuMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTMwODI3MjM1NDA3WhcNMTQwODI3MjM1NDA3WjBF
-----END CERTIFICATE-----
Some extra content at the end"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(content_with_extra)
            cert_path = f.name
        
        try:
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)
    
    def test_multiple_certificates_in_file(self):
        """Test file with multiple certificates."""
        multiple_certs = """-----BEGIN CERTIFICATE-----
MIIDXTCCAkWgAwIBAgIJAKoK/heBjcOuMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTMwODI3MjM1NDA3WhcNMTQwODI3MjM1NDA3WjBF
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIE5zCCAs+gAwIBAgIJANmqUxc9vwi8MA0GCSqGSIb3DQEBCwUAMIGJMQswCQYD
VQQGEwJVUzELMAkGA1UECAwCQ0ExFjAUBgNVBAcMDU1vdW50YWluIFZpZXcxFDAS
BgNVBAoMC1BheVBhbCBJbmMuMRMwEQYDVQQLDApzYW5kYm94XzEwMTQwMgYDVQQD
-----END CERTIFICATE-----"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(multiple_certs)
            cert_path = f.name
        
        try:
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)
    
    def test_binary_garbage_in_der_file(self):
        """Test DER file with binary garbage data."""
        garbage_data = os.urandom(1024)  # Random binary data
        
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.der', delete=False) as f:
            f.write(garbage_data)
            cert_path = f.name
        
        try:
            # Should pass creation (validation happens during TLS handshake)
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)


class TestCertificateFilePermissions:
    """Test certificate file permission scenarios."""
    
    @pytest.mark.skipif(os.name == 'nt', reason="POSIX permissions not available on Windows")
    def test_certificate_file_no_read_permission(self):
        """Test certificate file with no read permission."""
        import os
        
        # Skip if running as root (common in CI environments)
        if os.getuid() == 0:
            pytest.fail("Running as root - file permissions are not enforced")
            
        content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(content)
            cert_path = f.name
        
        try:
            # Remove read permission
            os.chmod(cert_path, 0o000)
            
            # Verify permissions are actually restricted
            stat_info = os.stat(cert_path)
            if stat_info.st_mode & 0o777 != 0o000:
                pytest.fail("Unable to restrict file permissions in this environment")
            
            # Test that the SSL config raises an exception
            with pytest.raises(Exception):
                SslConfig(ca_certificate_path=cert_path)
        finally:
            # Restore permissions to delete file
            os.chmod(cert_path, 0o644)
            os.unlink(cert_path)
    
    @pytest.mark.skipif(os.name == 'nt', reason="POSIX permissions not available on Windows")
    def test_certificate_file_read_only(self):
        """Test certificate file with read-only permission."""
        content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(content)
            cert_path = f.name
        
        try:
            # Set read-only permission
            os.chmod(cert_path, 0o444)
            
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            # Restore permissions to delete file
            os.chmod(cert_path, 0o644)
            os.unlink(cert_path)


class TestCertificateFilePaths:
    """Test various certificate file path scenarios."""
    
    def test_absolute_path_certificate(self):
        """Test certificate with absolute path."""
        content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(content)
            cert_path = os.path.abspath(f.name)
        
        try:
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)
    
    def test_relative_path_certificate(self):
        """Test certificate with relative path."""
        content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        
        # Create in a temporary directory
        temp_dir = tempfile.mkdtemp()
        cert_filename = "test_cert.pem"
        cert_path = os.path.join(temp_dir, cert_filename)
        
        try:
            with open(cert_path, 'w') as f:
                f.write(content)
            
            # Change to temp directory and use relative path
            original_cwd = os.getcwd()
            os.chdir(temp_dir)
            
            try:
                ssl_config = SslConfig(ca_certificate_path=cert_filename)
                assert cert_filename in ssl_config.ca_certificate_path
            finally:
                os.chdir(original_cwd)
        finally:
            if os.path.exists(cert_path):
                os.unlink(cert_path)
            os.rmdir(temp_dir)
    
    def test_certificate_path_with_spaces(self):
        """Test certificate path with spaces."""
        content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        
        temp_dir = tempfile.mkdtemp()
        cert_filename = "test cert with spaces.pem"
        cert_path = os.path.join(temp_dir, cert_filename)
        
        try:
            with open(cert_path, 'w') as f:
                f.write(content)
            
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            if os.path.exists(cert_path):
                os.unlink(cert_path)
            os.rmdir(temp_dir)
    
    def test_certificate_path_with_unicode(self):
        """Test certificate path with unicode characters."""
        content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        
        temp_dir = tempfile.mkdtemp()
        cert_filename = "tëst_cért_ñáмё.pem"
        cert_path = os.path.join(temp_dir, cert_filename)
        
        try:
            with open(cert_path, 'w', encoding='utf-8') as f:
                f.write(content)
            
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            if os.path.exists(cert_path):
                os.unlink(cert_path)
            os.rmdir(temp_dir)
    
    def test_very_long_certificate_path(self):
        """Test very long certificate file path."""
        content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        
        temp_dir = tempfile.mkdtemp()
        # Create a deeply nested directory structure
        long_path_parts = ['a' * 50 for _ in range(5)]  # 5 levels deep, 50 chars each
        long_dir = os.path.join(temp_dir, *long_path_parts)
        os.makedirs(long_dir, exist_ok=True)
        
        cert_path = os.path.join(long_dir, "cert.pem")
        
        try:
            with open(cert_path, 'w') as f:
                f.write(content)
            
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            if os.path.exists(cert_path):
                os.unlink(cert_path)
            # Clean up the deep directory structure
            import shutil
            shutil.rmtree(temp_dir)


class TestCertificateFileSymlinks:
    """Test certificate file symbolic link scenarios."""
    
    @pytest.mark.skipif(os.name == 'nt', reason="Symlinks may not be available on Windows")
    def test_certificate_symlink(self):
        """Test certificate file accessed via symbolic link."""
        content = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"
        
        temp_dir = tempfile.mkdtemp()
        real_cert_path = os.path.join(temp_dir, "real_cert.pem")
        symlink_path = os.path.join(temp_dir, "symlink_cert.pem")
        
        try:
            # Create real certificate file
            with open(real_cert_path, 'w') as f:
                f.write(content)
            
            # Create symbolic link
            os.symlink(real_cert_path, symlink_path)
            
            ssl_config = SslConfig(ca_certificate_path=symlink_path)
            assert ssl_config.ca_certificate_path == symlink_path
        finally:
            if os.path.exists(symlink_path):
                os.unlink(symlink_path)
            if os.path.exists(real_cert_path):
                os.unlink(real_cert_path)
            os.rmdir(temp_dir)
    
    @pytest.mark.skipif(os.name == 'nt', reason="Symlinks may not be available on Windows")
    def test_broken_certificate_symlink(self):
        """Test broken symbolic link to certificate file."""
        temp_dir = tempfile.mkdtemp()
        symlink_path = os.path.join(temp_dir, "broken_symlink.pem")
        nonexistent_target = os.path.join(temp_dir, "nonexistent.pem")
        
        try:
            # Create broken symbolic link
            os.symlink(nonexistent_target, symlink_path)
            
            with pytest.raises(Exception):
                SslConfig(ca_certificate_path=symlink_path)
        finally:
            if os.path.lexists(symlink_path):
                os.unlink(symlink_path)
            os.rmdir(temp_dir)


class TestCertificateContentVariations:
    """Test various certificate content variations."""
    
    def test_minimal_valid_pem_certificate(self):
        """Test minimal valid PEM certificate structure."""
        minimal_content = """-----BEGIN CERTIFICATE-----
MIIBkTCB+wIJAMlyFqk69v+9MA0GCSqGSIb3DQEBCwUAMBQxEjAQBgNVBAMMCWxv
Y2FsaG9zdDAeFw0xNzEyMzEyMzU5NTlaFw0xODEyMzEyMzU5NTlaMBQxEjAQBgNV
BAMMCWxvY2FsaG9zdDBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQC9h+P2pgRCgMuW
LL2gVGVTX/7ZZP6yjZvddNgf9/qEQJoNELMu4KBc1iJHdT4J7x7Y1N4N4J7x7Y1N
4N4J7x7Y1AgMBAAEwDQYJKoZIhvcNAQELBQADQQBIMOzrpwivdJ6J7x7Y1N4N4J7x
7Y1N4N4J7x7Y1N4N4J7x7Y1N4N4J7x7Y1N4N4J7x7Y1N4N4J7x7Y1N4N4==
-----END CERTIFICATE-----"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(minimal_content)
            cert_path = f.name
        
        try:
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)
    
    def test_certificate_with_windows_line_endings(self):
        """Test certificate with Windows line endings (CRLF)."""
        content_with_crlf = "-----BEGIN CERTIFICATE-----\r\nMIIDXTCCAkWgAwIBAgIJAKoK/heBjcOuMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV\r\nBAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX\r\naWRnaXRzIFB0eSBMdGQwHhcNMTMwODI3MjM1NDA3WhcNMTQwODI3MjM1NDA3WjBF\r\n-----END CERTIFICATE-----\r\n"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(content_with_crlf)
            cert_path = f.name
        
        try:
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)
    
    def test_certificate_with_comments(self):
        """Test certificate file with comments."""
        content_with_comments = """# This is a test certificate
# Created for testing purposes
-----BEGIN CERTIFICATE-----
MIIDXTCCAkWgAwIBAgIJAKoK/heBjcOuMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTMwODI3MjM1NDA3WhcNMTQwODI3MjM1NDA3WjBF
-----END CERTIFICATE-----
# End of certificate"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(content_with_comments)
            cert_path = f.name
        
        try:
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)


class TestCertificateFileSize:
    """Test certificate files of various sizes."""
    
    def test_very_small_certificate_file(self):
        """Test very small certificate file."""
        tiny_content = "-----BEGIN CERTIFICATE-----\nA\n-----END CERTIFICATE-----"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(tiny_content)
            cert_path = f.name
        
        try:
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)
    
    def test_large_certificate_file(self):
        """Test large certificate file (simulating a certificate bundle)."""
        # Create a large certificate file by repeating certificate content
        base_cert = """-----BEGIN CERTIFICATE-----
MIIDXTCCAkWgAwIBAgIJAKoK/heBjcOuMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTMwODI3MjM1NDA3WhcNMTQwODI3MjM1NDA3WjBF
MQswCQYDVQQGEwJBVTETMBEGA1UECAwKU29tZS1TdGF0ZTEhMB8GA1UECgwYSW50
ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
-----END CERTIFICATE-----"""
        
        large_content = "\n".join([base_cert] * 100)  # Repeat 100 times
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
            f.write(large_content)
            cert_path = f.name
        
        try:
            ssl_config = SslConfig(ca_certificate_path=cert_path)
            assert ssl_config.ca_certificate_path == cert_path
        finally:
            os.unlink(cert_path)
    
    def test_certificate_file_size_limits(self):
        """Test certificate file at various size boundaries."""
        sizes = [1, 100, 1024, 10240, 102400]  # 1B, 100B, 1KB, 10KB, 100KB
        
        for size in sizes:
            content = "A" * size
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as f:
                f.write(content)
                cert_path = f.name
            
            try:
                ssl_config = SslConfig(ca_certificate_path=cert_path)
                assert ssl_config.ca_certificate_path == cert_path
            finally:
                os.unlink(cert_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
