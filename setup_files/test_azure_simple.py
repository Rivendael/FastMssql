#!/usr/bin/env python3
"""
Quick Azure Service Principal authentication test for FastMSSQL
"""

import asyncio
import os
import fastmssql

async def test_azure_auth():
    """Test Azure Service Principal authentication with database operations."""
    print("🔐 FastMSSQL Azure Authentication Test")
    print("=" * 50)
    
    # Create Azure credential
    azure_cred = fastmssql.AzureCredential.service_principal(
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        tenant_id=os.getenv("AZURE_TENANT_ID")
    )
    
    print(f"🌐 Server: {os.getenv('AZURE_SQL_SERVER')}")
    print(f"💾 Database: {os.getenv('AZURE_SQL_DATABASE')}")
    print(f"👤 Service Principal: {os.getenv('AZURE_CLIENT_ID')}")
    
    try:
        async with fastmssql.Connection(
            server=os.getenv("AZURE_SQL_SERVER"),
            database=os.getenv("AZURE_SQL_DATABASE"),
            azure_credential=azure_cred
        ) as conn:
            # Test basic connection
            result = await conn.query("SELECT GETDATE() as current_dt, USER_NAME() as user_name")
            for row in result.rows():
                print(f"✅ Connected! Time: {row['current_dt']}, User: {row['user_name']}")
            
            # Test database operations
            result = await conn.query("SELECT name FROM sys.databases WHERE database_id <= 5")
            print("\n📊 Available databases:")
            for row in result.rows():
                print(f"   - {row['name']}")
            
            # Test parameterized query
            result = await conn.query("SELECT @P1 as test_param", ["Hello Azure!"])
            for row in result.rows():
                print(f"\n🧪 Parameter test: {row['test_param']}")
            
            # Test connection pool stats
            stats = await conn.pool_stats()
            print(f"\n📈 Pool stats: {stats}")
            
            print("\n🎉 All Connection Azure authentication tests passed!")
            
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False
    
    return True

async def test_transaction_azure_auth():
    """Test Azure Service Principal authentication with Transaction operations."""
    print("\n🔐 FastMSSQL Transaction Azure Authentication Test")
    print("=" * 50)
    
    # Create Azure credential
    azure_cred = fastmssql.AzureCredential.service_principal(
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        tenant_id=os.getenv("AZURE_TENANT_ID")
    )
    
    print(f"🌐 Server: {os.getenv('AZURE_SQL_SERVER')}")
    print(f"💾 Database: {os.getenv('AZURE_SQL_DATABASE')}")
    print(f"👤 Service Principal: {os.getenv('AZURE_CLIENT_ID')}")
    
    try:
        # Test Transaction with Azure authentication
        transaction = fastmssql.Transaction(
            server=os.getenv("AZURE_SQL_SERVER"),
            database=os.getenv("AZURE_SQL_DATABASE"),
            azure_credential=azure_cred
        )
        
        # Test basic connection
        result = await transaction.query("SELECT GETDATE() as current_dt, USER_NAME() as user_name")
        for row in result.rows():
            print(f"✅ Transaction Connected! Time: {row['current_dt']}, User: {row['user_name']}")
        
        # Test transaction operations
        await transaction.begin()
        print("🔄 Transaction started")
        
        # Create a test table for transaction testing (if it doesn't exist)
        await transaction.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='azure_test_table' AND type='U')
            CREATE TABLE azure_test_table (
                id int IDENTITY(1,1) PRIMARY KEY,
                test_value nvarchar(100),
                created_dt datetime2 DEFAULT GETDATE()
            )
        """)
        
        # Insert test data within transaction
        await transaction.execute("INSERT INTO azure_test_table (test_value) VALUES (@P1)", ["Azure Transaction Test"])
        print("📝 Data inserted in transaction")
        
        # Query the data within the transaction
        result = await transaction.query("SELECT * FROM azure_test_table WHERE test_value = @P1", ["Azure Transaction Test"])
        for row in result.rows():
            print(f"📊 Transaction data: ID={row['id']}, Value={row['test_value']}")
        
        # Commit the transaction
        await transaction.commit()
        print("✅ Transaction committed successfully")
        
        # Test rollback functionality
        await transaction.begin()
        await transaction.execute("INSERT INTO azure_test_table (test_value) VALUES (@P1)", ["Rollback Test"])
        await transaction.rollback()
        print("🔄 Transaction rolled back successfully")
        
        # Verify rollback worked
        result = await transaction.query("SELECT COUNT(*) as count FROM azure_test_table WHERE test_value = @P1", ["Rollback Test"])
        for row in result.rows():
            if row['count'] == 0:
                print("✅ Rollback verification passed - no data found")
            else:
                print(f"❌ Rollback verification failed - found {row['count']} rows")
        
        # Clean up test data
        await transaction.execute("DELETE FROM azure_test_table WHERE test_value = @P1", ["Azure Transaction Test"])
        
        # Close the transaction connection
        await transaction.close()
        print("🔌 Transaction connection closed")
        
        print("\n🎉 All Transaction Azure authentication tests passed!")
        
    except Exception as e:
        print(f"❌ Transaction test failed: {e}")
        return False
    
    return True

async def test_all_azure_auth():
    """Run all Azure authentication tests."""
    connection_success = await test_azure_auth()
    transaction_success = await test_transaction_azure_auth()
    return connection_success and transaction_success

if __name__ == "__main__":
    success = asyncio.run(test_all_azure_auth())
    if success:
        print("\n✨ All Azure authentication tests are working perfectly!")
    else:
        print("\n💥 Some Azure authentication tests need attention")
        exit(1)