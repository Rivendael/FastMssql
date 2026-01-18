#!/bin/bash

# Script to set up database permissions using Azure CLI and sqlcmd

set -e

echo "🔐 Setting up database permissions for Azure AD Service Principal"
echo "================================================================="

# Check if Azure CLI is installed
if ! command -v az &> /dev/null; then
    echo "❌ Azure CLI is not installed. Please install it first:"
    echo "   brew install azure-cli"
    exit 1
fi

# Check if sqlcmd is installed
if ! command -v sqlcmd &> /dev/null; then
    echo "❌ sqlcmd is not installed. Please install it:"
    echo "   brew install microsoft/mssql-release/mssql-tools18"
    echo "   OR"
    echo "   brew install sqlcmd"
    exit 1
fi

# Login to Azure if needed
echo "🔑 Checking Azure CLI login status..."
if ! az account show &> /dev/null; then
    echo "Please login to Azure CLI:"
    az login
fi

# Get connection details
echo "📋 Getting connection details..."
SERVER="fastmssql-dev-srv.database.windows.net"
DATABASE="devdb"

echo "🔗 Server: $SERVER"
echo "🗂️  Database: $DATABASE"

# Get access token for SQL Database
echo "🎫 Getting access token..."
ACCESS_TOKEN=$(az account get-access-token --resource https://database.windows.net/ --query accessToken -o tsv)

if [ -z "$ACCESS_TOKEN" ]; then
    echo "❌ Failed to get access token"
    exit 1
fi

echo "✅ Got access token"

# Connect and run setup SQL
echo "🛠️  Setting up database permissions..."

sqlcmd -S "$SERVER" -d "$DATABASE" -G -P "$ACCESS_TOKEN" -Q "
-- Create user for the Service Principal
CREATE USER [fastmssql-dev-srv-app] FROM EXTERNAL PROVIDER;

-- Grant basic permissions
ALTER ROLE db_datareader ADD MEMBER [fastmssql-dev-srv-app];
ALTER ROLE db_datawriter ADD MEMBER [fastmssql-dev-srv-app];
ALTER ROLE db_ddladmin ADD MEMBER [fastmssql-dev-srv-app];

-- Verify the user was created
SELECT name, type_desc, authentication_type_desc 
FROM sys.database_principals 
WHERE name = 'fastmssql-dev-srv-app';

PRINT 'Database permissions setup completed successfully!';
"

echo "✅ Database permissions setup completed!"
echo ""
echo "🧪 You can now test the connection with:"
echo "   env \$(cat azure.env | grep -v '^#' | xargs) uv run python examples/azure_auth_example.py"