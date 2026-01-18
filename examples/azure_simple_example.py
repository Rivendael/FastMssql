# Simple Azure Authentication Example for FastMSSQL

import asyncio
import os
import fastmssql

async def main():
    """Simple Azure Service Principal authentication example."""
    print("🔐 FastMSSQL Azure Authentication Example")
    print("=" * 50)
    
    # Check environment variables
    required_vars = ['AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET', 'AZURE_TENANT_ID', 'AZURE_SQL_SERVER', 'AZURE_SQL_DATABASE']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\n💡 To fix this, run: source setup_files/azure.env")
        return
    
    # Create Azure Service Principal credential
    azure_cred = fastmssql.AzureCredential.service_principal(
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        tenant_id=os.getenv("AZURE_TENANT_ID")
    )
    
    try:
        async with fastmssql.Connection(
            server=os.getenv("AZURE_SQL_SERVER"),
            database=os.getenv("AZURE_SQL_DATABASE"),
            azure_credential=azure_cred
        ) as conn:
            # Test connection
            result = await conn.query("SELECT GETDATE() as current_dt, USER_NAME() as user_name")
            for row in result.rows():
                print(f"✅ Connected! Time: {row['current_dt']}, User: {row['user_name']}")
            
            # Test parameterized query
            result = await conn.query("SELECT @P1 as message", ["Hello from Azure!"])
            for row in result.rows():
                print(f"📝 Message: {row['message']}")
            
            print("\n🎉 Azure authentication example completed successfully!")
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())