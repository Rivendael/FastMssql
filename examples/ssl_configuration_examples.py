"""
SSL/TLS Configuration Examples for pymssql-rs

This file demonstrates various SSL/TLS configurations for secure connections
to Microsoft SQL Server.
"""

import asyncio
from fastmssql import Connection, PoolConfig, SslConfig


async def example_default_ssl():
    """Example: Default SSL configuration (most secure)."""
    print("=== Default SSL Configuration ===")
    
    # By default, SSL uses EncryptionLevel.Required with system trust store
    ssl_config = SslConfig()  # encryption_level="Required" by default
    
    pool_config = PoolConfig(max_size=5)
    
    conn = Connection(
        server="your-server.database.windows.net",
        database="your_database",
        username="your_username",
        password="your_password",
        pool_config=pool_config,
        ssl_config=ssl_config
    )
    
    try:
        await conn.connect()
        print("✅ Connected with default SSL configuration")
        
        result = await conn.execute("SELECT @@VERSION as version")
        for row in result.rows():
            print(f"SQL Server version: {row['version']}")
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
    finally:
        await conn.disconnect()


async def example_development_ssl():
    """Example: Development SSL configuration (trusts all certificates)."""
    print("\n=== Development SSL Configuration ===")
    print("⚠️  WARNING: This configuration is insecure and should only be used in development!")
    
    # Development configuration - trusts all certificates (insecure)
    ssl_config = SslConfig.development()
    
    conn = Connection(
        server="localhost",  # Local SQL Server instance
        database="master",
        username="sa",
        password="YourPassword123!",
        ssl_config=ssl_config
    )
    
    try:
        await conn.connect()
        print("✅ Connected with development SSL configuration")
        
        result = await conn.execute("SELECT DB_NAME() as current_db")
        for row in result.rows():
            print(f"Current database: {row['current_db']}")
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
    finally:
        await conn.disconnect()


async def example_custom_ca_certificate():
    """Example: Custom CA certificate configuration."""
    print("\n=== Custom CA Certificate Configuration ===")
    
    # Production configuration with custom CA certificate
    try:
        ssl_config = SslConfig.with_ca_certificate("path/to/your/ca_certificate.pem")
    except Exception as e:
        print(f"❌ Failed to create SSL config with CA certificate: {e}")
        print("💡 Make sure the CA certificate file exists and has proper extension (.pem, .crt, .der)")
        return
    
    conn = Connection(
        server="your-secure-server.company.com",
        database="production_db",
        username="app_user",
        password="SecurePassword123!",
        ssl_config=ssl_config
    )
    
    try:
        await conn.connect()
        print("✅ Connected with custom CA certificate")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
    finally:
        await conn.disconnect()


async def example_login_only_encryption():
    """Example: Login-only encryption (legacy mode)."""
    print("\n=== Login-Only Encryption Configuration ===")
    print("⚠️  This mode only encrypts the login process, not data transmission")
    
    # Legacy mode - only login is encrypted
    ssl_config = SslConfig.login_only()
    
    conn = Connection(
        server="legacy-server.company.com",
        database="legacy_db",
        username="legacy_user",
        password="LegacyPassword123!",
        ssl_config=ssl_config
    )
    
    try:
        await conn.connect()
        print("✅ Connected with login-only encryption")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
    finally:
        await conn.disconnect()


async def example_no_encryption():
    """Example: No encryption (not recommended)."""
    print("\n=== No Encryption Configuration ===")
    print("⚠️  WARNING: This configuration sends all data in plain text!")
    
    # No encryption - not recommended for production
    ssl_config = SslConfig.disabled()
    
    conn = Connection(
        server="internal-server",
        database="test_db",
        username="test_user",
        password="TestPassword123!",
        ssl_config=ssl_config
    )
    
    try:
        await conn.connect()
        print("✅ Connected without encryption")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
    finally:
        await conn.disconnect()


async def example_custom_ssl_config():
    """Example: Fully customized SSL configuration."""
    print("\n=== Custom SSL Configuration ===")
    
    # Fully customized SSL configuration
    ssl_config = SslConfig(
        encryption_level="Required",
        trust_server_certificate=False,  # Validate certificates (secure)
        ca_certificate_path=None,        # Use system trust store
        enable_sni=True,                 # Enable Server Name Indication
        server_name="custom.server.name" # Custom server name for validation
    )
    
    print(f"SSL Config: {ssl_config}")
    print(f"Encryption Level: {ssl_config.encryption_level}")
    print(f"Trust Server Certificate: {ssl_config.trust_server_certificate}")
    print(f"Enable SNI: {ssl_config.enable_sni}")
    print(f"Server Name: {ssl_config.server_name}")
    
    conn = Connection(
        server="your-server.database.windows.net",
        database="your_database",
        username="your_username",
        password="your_password",
        ssl_config=ssl_config
    )
    
    try:
        await conn.connect()
        print("✅ Connected with custom SSL configuration")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
    finally:
        await conn.disconnect()


async def example_connection_string_with_ssl():
    """Example: Using connection string with SSL parameters."""
    print("\n=== Connection String with SSL ===")
    
    # Connection string with SSL parameters
    # Note: SSL configuration in connection string takes precedence over ssl_config parameter
    connection_string = (
        "Server=your-server.database.windows.net;"
        "Database=your_database;"
        "User ID=your_username;"
        "Password=your_password;"
        "Encrypt=true;"  # Enable encryption
        "TrustServerCertificate=false;"  # Validate server certificate
    )
    
    conn = Connection(connection_string=connection_string)
    
    try:
        await conn.connect()
        print("✅ Connected using connection string with SSL")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
    finally:
        await conn.disconnect()


async def main():
    """Run all SSL configuration examples."""
    print("🔒 SSL/TLS Configuration Examples for pymssql-rs\n")
    
    examples = [
        example_default_ssl,
        example_development_ssl,
        example_custom_ca_certificate,
        example_login_only_encryption,
        example_no_encryption,
        example_custom_ssl_config,
        example_connection_string_with_ssl,
    ]
    
    for example in examples:
        try:
            await example()
        except Exception as e:
            print(f"❌ Example {example.__name__} failed: {e}")
        
        print()  # Add spacing between examples


if __name__ == "__main__":
    asyncio.run(main())
