# ──────────────────────────────────────────────────────────
# Glue Data Catalog Database
# Athena uses Glue as its metadata store — the database
# is just a namespace for tables.
# ──────────────────────────────────────────────────────────
resource "aws_glue_catalog_database" "nl_rail" {
  name        = "nl_rail_etl"
  description = "NS Rail disruption data lake"
}


# ──────────────────────────────────────────────────────────
# Glue Catalog Table (External)
#
# Points at s3://bucket/athena/ — the JSONL prefix written
# by _save_jsonl_for_athena() in api_client.py.
#
# Partition projection: automatically infers year/month/day
# partitions from the S3 path structure, so we never need to
# run MSCK REPAIR TABLE after each daily load.
#
# S3 path pattern: athena/2026/06/25/disruptions_*.jsonl
# Partition template must use $${} to escape Terraform
# interpolation → produces literal ${year} for AWS.
# ──────────────────────────────────────────────────────────
resource "aws_glue_catalog_table" "disruptions_raw" {
  name          = "disruptions_raw"
  database_name = aws_glue_catalog_database.nl_rail.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"              = "json"
    "EXTERNAL"                    = "TRUE"

    # Partition projection — no manual partition management needed
    "projection.enabled"          = "true"
    "projection.year.type"        = "integer"
    "projection.year.range"       = "2025,2030"
    "projection.year.digits"      = "4"
    "projection.month.type"       = "integer"
    "projection.month.range"      = "1,12"
    "projection.month.digits"     = "2"
    "projection.day.type"         = "integer"
    "projection.day.range"        = "1,31"
    "projection.day.digits"       = "2"

    # $${year} → literal ${year} after Terraform renders the string
    "storage.location.template"   = "s3://${var.s3_bucket}/athena/$${year}/$${month}/$${day}"
  }

  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${var.s3_bucket}/athena/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = false

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters = {
        "ignore.malformed.json" = "TRUE"
        "dots.in.keys"          = "FALSE"
        "case.insensitive"      = "TRUE"
      }
    }

    # Scalar columns only — JsonSerDe silently ignores
    # undefined nested fields (timespans, section).
    # Keep start/end as strings; cast with date_parse() in queries.
    columns {
      name = "id"
      type = "string"
    }
    columns {
      name    = "type"
      type    = "string"
    }
    columns {
      name = "title"
      type = "string"
    }
    columns {
      name = "description"
      type = "string"
    }
    columns {
      name    = "start"
      type    = "string"
    }
    columns {
      name    = "end"
      type    = "string"
    }
    columns {
      name = "isactive"
      type = "boolean"
    }
  }
}


# ──────────────────────────────────────────────────────────
# Athena Workgroup
#
# Workgroup isolates query results, cost tracking, and
# per-query scan limits from the default workgroup.
# bytes_scanned_cutoff: kills queries scanning > 1 GB —
# prevents accidental full-table scans from running up costs.
# ──────────────────────────────────────────────────────────
resource "aws_athena_workgroup" "nl_rail" {
  name        = "nl-rail-workgroup"
  description = "NS Rail disruption query workgroup"
  state       = "ENABLED"

  configuration {
    result_configuration {
      output_location = "s3://${var.s3_bucket}/athena-query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 1073741824  # 1 GB safety cap
  }

  tags = {
    Environment = var.environment
  }
}