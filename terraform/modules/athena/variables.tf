variable "s3_bucket" {
  description = "S3 bucket containing raw disruption data"
  type        = string
}

variable "environment" {
  description = "Environment tag"
  type        = string
  default     = "prod"
}