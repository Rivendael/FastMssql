# Create resource group
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

# Create SQL Server
resource "azurerm_mssql_server" "sql_server" {
  name                         = var.sql_server_name
  resource_group_name          = azurerm_resource_group.rg.name
  location                     = azurerm_resource_group.rg.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_username
  administrator_login_password = var.sql_admin_password

  azuread_administrator {
    login_username = var.azuread_admin_username
    object_id      = var.azuread_admin_object_id
  }

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags

  timeouts {
    create = "60m"
    delete = "60m"
  }
}

# Allow Azure services and resources to access this server
resource "azurerm_mssql_firewall_rule" "azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.sql_server.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# Allow local machine to access
resource "azurerm_mssql_firewall_rule" "local_machine" {
  name             = "AllowLocalMachine"
  server_id        = azurerm_mssql_server.sql_server.id
  start_ip_address = var.my_local_ip
  end_ip_address   = var.my_local_ip
}

# Create database
resource "azurerm_mssql_database" "database" {
  name      = var.database_name
  server_id = azurerm_mssql_server.sql_server.id

  sku_name    = var.database_sku
  collation   = "SQL_Latin1_General_CP1_CI_AS"
  max_size_gb = var.max_size_gb

  tags = var.tags
}

# Create database audit policy
resource "azurerm_mssql_database_extended_auditing_policy" "database_audit" {
  database_id                             = azurerm_mssql_database.database.id
  enabled                                 = true
  retention_in_days                       = 1
  storage_endpoint                        = null
  storage_account_access_key              = null
  storage_account_access_key_is_secondary = false
  log_monitoring_enabled                  = true
}

resource "azurerm_mssql_server_transparent_data_encryption" "tde" {
  server_id             = azurerm_mssql_server.sql_server.id
  key_vault_key_id      = null # or reference a Key Vault key for BYOK
  auto_rotation_enabled = true
}

# Create Azure AD application for Service Principal authentication
resource "azuread_application" "fastmssql_app" {
  display_name = "${var.sql_server_name}-app"
  owners       = [data.azuread_client_config.current.object_id]

  required_resource_access {
    resource_app_id = "022907d3-0f1b-48f7-badc-1ba6abab6d66" # Azure SQL Database

    resource_access {
      id   = "c39ef2d1-04ce-46dc-8b5f-e9a5c60f0fc9" # user_impersonation
      type = "Scope"
    }
  }
}

# Create Service Principal
resource "azuread_service_principal" "fastmssql_sp" {
  client_id                    = azuread_application.fastmssql_app.client_id
  app_role_assignment_required = false
  owners                       = [data.azuread_client_config.current.object_id]
}

# Create client secret for Service Principal
resource "azuread_application_password" "fastmssql_client_secret" {
  application_id = azuread_application.fastmssql_app.id
  display_name   = "FastMSSQLSecret"
  end_date_relative = "8760h" # 1 year from creation
}

# Data source to get current Azure AD configuration
data "azuread_client_config" "current" {}

# Data source to get current subscription
data "azurerm_client_config" "current" {}