output "eventhub_connection_string_producer" {
  description = "Event Hubs producer connection string"
  value       = azurerm_eventhub_authorization_rule.producer.primary_connection_string
  sensitive   = true
}

output "eventhub_connection_string_consumer" {
  description = "Event Hubs consumer connection string"
  value       = azurerm_eventhub_authorization_rule.consumer.primary_connection_string
  sensitive   = true
}

output "adls_account_name" {
  description = "ADLS Gen2 storage account name"
  value       = azurerm_storage_account.adls.name
}

output "adls_primary_key" {
  description = "ADLS primary access key"
  value       = azurerm_storage_account.adls.primary_access_key
  sensitive   = true
}

output "synapse_workspace_name" {
  description = "Synapse workspace name"
  value       = azurerm_synapse_workspace.main.name
}

output "synapse_sql_endpoint" {
  description = "Synapse dedicated SQL endpoint"
  value       = azurerm_synapse_workspace.main.connectivity_endpoints["sqlOnDemand"]
}
