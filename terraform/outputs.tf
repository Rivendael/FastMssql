output "resource_group_name" {
  description = "The name of the resource group"
  value       = azurerm_resource_group.rg.name
}

output "sql_server_name" {
  description = "The name of the SQL Server"
  value       = azurerm_mssql_server.sql_server.name
}

output "sql_server_fqdn" {
  description = "The fully qualified domain name of the SQL Server"
  value       = azurerm_mssql_server.sql_server.fully_qualified_domain_name
}

output "database_name" {
  description = "The name of the database"
  value       = azurerm_mssql_database.database.name
}

output "sql_server_id" {
  description = "The ID of the SQL Server"
  value       = azurerm_mssql_server.sql_server.id
}

output "database_id" {
  description = "The ID of the database"
  value       = azurerm_mssql_database.database.id
}

output "sql_server_identity_principal_id" {
  description = "The principal ID of the system-assigned managed identity"
  value       = azurerm_mssql_server.sql_server.identity[0].principal_id
}

# Service Principal outputs for testing
output "service_principal_client_id" {
  description = "Client ID of the Service Principal for Azure authentication"
  value       = azuread_application.fastmssql_app.client_id
}

output "service_principal_client_secret" {
  description = "Client Secret of the Service Principal (sensitive)"
  value       = azuread_application_password.fastmssql_client_secret.value
  sensitive   = true
}

output "tenant_id" {
  description = "Azure Tenant ID"
  value       = data.azuread_client_config.current.tenant_id
}

output "subscription_id" {
  description = "Azure Subscription ID"
  value       = data.azurerm_client_config.current.subscription_id
}

# Connection string outputs
output "connection_string_sql_auth" {
  description = "Connection string using SQL authentication"
  value       = "Server=${azurerm_mssql_server.sql_server.fully_qualified_domain_name};Database=${azurerm_mssql_database.database.name};User Id=${var.sql_admin_username};Password=${var.sql_admin_password};Encrypt=true;TrustServerCertificate=false;"
  sensitive   = true
}

output "azure_sql_server_for_env" {
  description = "SQL Server FQDN for environment variables"
  value       = azurerm_mssql_server.sql_server.fully_qualified_domain_name
}

output "azure_sql_database_for_env" {
  description = "Database name for environment variables"
  value       = azurerm_mssql_database.database.name
}

# Instructions output
output "azure_auth_instructions" {
  description = "Instructions for using Azure authentication"
  value = <<-EOT
  
  🔐 Azure Authentication Setup Complete!
  
  To use Azure authentication with your FastMSSQL library:
  
  1. Set these environment variables:
     export AZURE_SQL_SERVER="${azurerm_mssql_server.sql_server.fully_qualified_domain_name}"
     export AZURE_SQL_DATABASE="${azurerm_mssql_database.database.name}"
     export AZURE_CLIENT_ID="${azuread_application.fastmssql_app.client_id}"
     export AZURE_CLIENT_SECRET="[run: terraform output -raw service_principal_client_secret]"
     export AZURE_TENANT_ID="${data.azuread_client_config.current.tenant_id}"
  
  2. Grant the Service Principal access to your database:
     - Connect to your database as Azure AD admin
     - Run: CREATE USER [${azuread_application.fastmssql_app.display_name}] FROM EXTERNAL PROVIDER;
     - Run: ALTER ROLE db_datareader ADD MEMBER [${azuread_application.fastmssql_app.display_name}];
     - Run: ALTER ROLE db_datawriter ADD MEMBER [${azuread_application.fastmssql_app.display_name}];
  
  3. Test with the examples/azure_auth_example.py script!
  
  EOT
}