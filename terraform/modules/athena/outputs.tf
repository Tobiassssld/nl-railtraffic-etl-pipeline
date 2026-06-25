output "glue_database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.nl_rail.name
}

output "glue_table_name" {
  description = "Glue catalog table name"
  value       = aws_glue_catalog_table.disruptions_raw.name
}

output "athena_workgroup_name" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.nl_rail.name
}

output "query_results_location" {
  description = "S3 path for Athena query results"
  value       = "s3://${var.s3_bucket}/athena-query-results/"
}