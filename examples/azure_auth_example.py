# Example usage of Azure credentials with FastMSSQL

import asyncio
import os
import fastmssql

async def test_service_principal_auth():
    """Test Service Principal authentication."""
    print("Testing Service Principal authentication...")
    
    # These would typically come from environment variables or secure configuration
    azure_cred = fastmssql.AzureCredential.service_principal(
        client_id=os.getenv("AZURE_CLIENT_ID", "your-client-id"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET", "your-client-secret"),
        tenant_id=os.getenv("AZURE_TENANT_ID", "your-tenant-id")
    )

    try:
        async with fastmssql.Connection(
            server=os.getenv("AZURE_SQL_SERVER", "yourserver.database.windows.net"),
            database=os.getenv("AZURE_SQL_DATABASE", "yourdatabase"),
            azure_credential=azure_cred
        ) as conn:
            result = await conn.query("SELECT GETDATE() as current_dt, USER_NAME() as user_name")
            for row in result.rows():
                print(f"Connected successfully! Current time: {row['current_dt']}, User: {row['user_name']}")
    except Exception as e:
        print(f"Service Principal authentication failed: {e}")

async def test_managed_identity_auth():
    """Test Managed Identity authentication (only works on Azure resources)."""
    print("\nTesting Managed Identity authentication...")
    
    azure_cred = fastmssql.AzureCredential.managed_identity(client_id=None)

    try:
        async with fastmssql.Connection(
            server=os.getenv("AZURE_SQL_SERVER", "yourserver.database.windows.net"),
            database=os.getenv("AZURE_SQL_DATABASE", "yourdatabase"),
            azure_credential=azure_cred
        ) as conn:
            result = await conn.query("SELECT GETDATE() as current_dt, USER_NAME() as user_name")
            for row in result.rows():
                print(f"Managed Identity connected! Current time: {row['current_dt']}, User: {row['user_name']}")
    except Exception as e:
        print(f"Managed Identity authentication failed (expected if not on Azure resource): {e}")

async def test_user_assigned_managed_identity():
    """Test User-Assigned Managed Identity authentication."""
    print("\nTesting User-Assigned Managed Identity authentication...")
    
    azure_cred = fastmssql.AzureCredential.managed_identity(
        client_id=os.getenv("AZURE_USER_ASSIGNED_IDENTITY_CLIENT_ID")
    )

    try:
        async with fastmssql.Connection(
            server=os.getenv("AZURE_SQL_SERVER", "yourserver.database.windows.net"),
            database=os.getenv("AZURE_SQL_DATABASE", "yourdatabase"),
            azure_credential=azure_cred
        ) as conn:
            result = await conn.query("SELECT GETDATE() as current_dt, USER_NAME() as user_name")
            for row in result.rows():
                print(f"User-Assigned MI connected! Current time: {row['current_dt']}, User: {row['user_name']}")
    except Exception as e:
        print(f"User-Assigned Managed Identity authentication failed: {e}")

async def test_access_token_auth():
    """Test pre-obtained access token authentication."""
    print("\nTesting Access Token authentication...")
    
    # In a real scenario, you would obtain this token from another Azure service
    access_token = os.getenv("AZURE_ACCESS_TOKEN")
    if not access_token:
        print("AZURE_ACCESS_TOKEN environment variable not set, skipping test")
        return
        
    azure_cred = fastmssql.AzureCredential.access_token(access_token)

    try:
        async with fastmssql.Connection(
            server=os.getenv("AZURE_SQL_SERVER", "yourserver.database.windows.net"),
            database=os.getenv("AZURE_SQL_DATABASE", "yourdatabase"),
            azure_credential=azure_cred
        ) as conn:
            result = await conn.query("SELECT GETDATE() as current_dt, USER_NAME() as user_name")
            for row in result.rows():
                print(f"Access Token connected! Current time: {row['current_dt']}, User: {row['user_name']}")
    except Exception as e:
        print(f"Access Token authentication failed: {e}")

async def test_default_azure_auth():
    """Test Default Azure credential chain."""
    print("\nTesting Default Azure credential chain...")
    
    azure_cred = fastmssql.AzureCredential.default()

    try:
        async with fastmssql.Connection(
            server=os.getenv("AZURE_SQL_SERVER", "yourserver.database.windows.net"),
            database=os.getenv("AZURE_SQL_DATABASE", "yourdatabase"),
            azure_credential=azure_cred
        ) as conn:
            result = await conn.query("SELECT GETDATE() as current_dt, USER_NAME() as user_name")
            for row in result.rows():
                print(f"Default credential connected! Current time: {row['current_dt']}, User: {row['user_name']}")
    except Exception as e:
        print(f"Default Azure credential authentication failed: {e}")

async def test_database_operations():
    """Test various database operations with Azure authentication."""
    print("\nTesting database operations with Azure authentication...")
    
    azure_cred = fastmssql.AzureCredential.service_principal(
        client_id=os.getenv("AZURE_CLIENT_ID", "your-client-id"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET", "your-client-secret"),
        tenant_id=os.getenv("AZURE_TENANT_ID", "your-tenant-id")
    )

    try:
        async with fastmssql.Connection(
            server=os.getenv("AZURE_SQL_SERVER", "yourserver.database.windows.net"),
            database=os.getenv("AZURE_SQL_DATABASE", "yourdatabase"),
            azure_credential=azure_cred
        ) as conn:
            # Test SELECT query
            result = await conn.query(
                "SELECT name, database_id FROM sys.databases WHERE database_id <= @P1", 
                [5]
            )
            print("Available databases:")
            for row in result.rows():
                print(f"  - {row['name']} (ID: {row['database_id']})")
            
            # Test connection pool statistics
            stats = await conn.pool_stats()
            print(f"\nConnection Pool Stats: {stats}")
            
    except Exception as e:
        print(f"Database operations failed: {e}")

async def main():
    """Run all Azure authentication tests."""
    print("FastMSSSQL Azure Authentication Examples")
    print("=" * 50)
    
    # Check for environment variables
    required_vars = {
        'AZURE_CLIENT_ID': os.getenv('AZURE_CLIENT_ID'),
        'AZURE_CLIENT_SECRET': os.getenv('AZURE_CLIENT_SECRET'),
        'AZURE_TENANT_ID': os.getenv('AZURE_TENANT_ID'),
        'AZURE_SQL_SERVER': os.getenv('AZURE_SQL_SERVER'),
        'AZURE_SQL_DATABASE': os.getenv('AZURE_SQL_DATABASE')
    }
    
    print("Environment Variables:")
    missing_vars = []
    for var, value in required_vars.items():
        if value:
            display_value = '***' if 'SECRET' in var else value
            print(f"✅ {var}: {display_value}")
        else:
            print(f"❌ {var}: Not set")
            missing_vars.append(var)
    
    if missing_vars:
        print("\n⚠️  Missing environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\n💡 To fix this, run: source azure.env")
        print("   Then try running this script again.")
        return
    
    print("=" * 50)
    
    # Run tests
    await test_service_principal_auth()
    await test_managed_identity_auth()
    await test_user_assigned_managed_identity()
    await test_access_token_auth()
    await test_default_azure_auth()
    await test_database_operations()
    
    print("\nAzure authentication testing completed!")

if __name__ == "__main__":
    asyncio.run(main())