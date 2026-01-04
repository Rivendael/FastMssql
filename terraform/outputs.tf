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