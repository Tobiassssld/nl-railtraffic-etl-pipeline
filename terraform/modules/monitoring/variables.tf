variable "lambda_function_name" {
  description = "Name of the Lambda function to monitor"
  type        = string
}

variable "alert_email" {
  description = "Email address to receive CloudWatch alerts"
  type        = string
}

variable "log_retention_days" {
  description = "Number of days to retain logs"
  type        = number
  default     = 30
}

variable "environment" {
  description = "Environment tag"
  type        = string
  default     = "prod"
}