variable "resource_group_name" {
  description = "Name of the Azure Resource Group"
  type        = string
  default     = "fraud-detection-rg"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "East US"
}

variable "adls_account_name" {
  description = "ADLS Gen2 storage account name (globally unique, lowercase, no hyphens)"
  type        = string
  default     = "frauddetectionadls"
}

variable "eventhub_namespace_name" {
  description = "Event Hubs namespace name"
  type        = string
  default     = "fraud-detection-eh-ns"
}

variable "synapse_workspace_name" {
  description = "Synapse Analytics workspace name"
  type        = string
  default     = "fraud-detection-synapse"
}

variable "synapse_sql_admin" {
  description = "Synapse SQL admin login"
  type        = string
  default     = "sqladmin"
}

variable "synapse_sql_password" {
  description = "Synapse SQL admin password"
  type        = string
  sensitive   = true
}

variable "tags" {
  description = "Common resource tags"
  type        = map(string)
  default = {
    project     = "fraud-detection-pipeline"
    environment = "dev"
    owner       = "data-engineering"
  }
}
