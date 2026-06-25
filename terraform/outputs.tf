output "glue_database_name" {
  description = "Glue catalog database name"
  value       = module.athena.glue_database_name
}

output "athena_workgroup_name" {
  description = "Athena workgroup name"
  value       = module.athena.athena_workgroup_name
}

output "query_results_location" {
  description = "S3 location for Athena query results"
  value       = module.athena.query_results_location
}