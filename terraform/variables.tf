variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "eu-north-1"
}

variable "lambda_function_name" {
  description = "Name of the Lambda function to monitor"
  type        = string
  default     = "nl-rail-etl-pipeline"
}

variable "alert_email" {
  description = "Email address to receive CloudWatch alerts"
  type        = string
}

variable "log_retention_days" {
  description = "Number of days to retain Lambda logs in CloudWatch Logs"
  type        = number
  default     = 30
}

variable "environment" {
  description = "Deployment environment tag (prod / staging / dev)"
  type        = string
  default     = "prod"
}

variable "s3_bucket" {
  description = "S3 bucket name for raw data and Athena results"
  type        = string
  default     = "nl-rail-raw-disruptions-tl"
}