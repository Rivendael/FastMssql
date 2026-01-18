-- Database permissions setup for Azure AD Service Principal
-- Connect to your database as an Azure AD administrator and run these commands

-- Create user for the Service Principal
CREATE USER [fastmssql-dev-srv-app] FROM EXTERNAL PROVIDER;

-- Grant basic permissions (adjust as needed for your application)
ALTER ROLE db_datareader ADD MEMBER [fastmssql-dev-srv-app];
ALTER ROLE db_datawriter ADD MEMBER [fastmssql-dev-srv-app];
ALTER ROLE db_ddladmin ADD MEMBER [fastmssql-dev-srv-app];  -- Optional: for DDL operations

-- Verify the user was created
SELECT name, type_desc, authentication_type_desc 
FROM sys.database_principals 
WHERE name = 'fastmssql-dev-srv-app';

-- Grant additional permissions if needed:
-- GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dbo TO [fastmssql-dev-srv-app];
-- GRANT EXECUTE ON SCHEMA::dbo TO [fastmssql-dev-srv-app];  -- For stored procedures
