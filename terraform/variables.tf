variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
  sensitive   = true
}

variable "location" {
  type        = string
  default     = "eastus"
  description = "Azure region for resources"
}

variable "resource_group_name" {
  type        = string
  default     = "fastmssql-dev-rg"
  description = "Name of the resource group"
}

variable "sql_server_name" {
  type        = string
  default     = "fastmssql-dev-srv"
  description = "Name of the SQL Server (must be globally unique)"
}

variable "sql_admin_username" {
  type        = string
  default     = "sqladmin"
  description = "SQL Server administrator username"
}

variable "sql_admin_password" {
  type        = string
  sensitive   = true
  description = "SQL Server administrator password (must be complex)"
}

variable "azuread_admin_username" {
  type        = string
  description = "Azure AD username for SQL admin"
}

variable "azuread_admin_object_id" {
  type        = string
  description = "Azure AD object ID for SQL admin"
}

variable "database_name" {
  type        = string
  default     = "devdb"
  description = "Name of the database"
}

variable "database_sku" {
  type        = string
  default     = "Basic"
  description = "Database SKU (Basic, S0, S1, S2, P1, etc.)"
}

variable "max_size_gb" {
  type        = number
  default     = 2
  description = "Maximum size of database in GB"
}

variable "tags" {
  type = object({
    Environment = string
    Project     = string
    ManagedBy   = string
  })
  default = {
    Environment = "dev"
    Project     = "fastmssql"
    ManagedBy   = "terraform"
  }
  description = "Tags to apply to all resources"
}

variable "my_local_ip" {
  description = "Your local IP address for firewall access"
  type        = string
  default     = ""
}