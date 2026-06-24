terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.5.0"

  # Enterprise upgrade path: S3 remote backend（团队共享state）
  # backend "s3" {
  #   bucket         = "nl-rail-terraform-state"
  #   key            = "monitoring/terraform.tfstate"
  #   region         = "eu-north-1"
  #   dynamodb_table = "nl-rail-terraform-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "nl-rail-etl-pipeline"
      ManagedBy = "terraform"
    }
  }
}

module "monitoring" {
  source = "./modules/monitoring"

  lambda_function_name = var.lambda_function_name
  alert_email          = var.alert_email
  log_retention_days   = var.log_retention_days
  environment          = var.environment
}