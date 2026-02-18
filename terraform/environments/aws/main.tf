terraform {
  required_version = ">= 1.8"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "staging"
}

module "storage" {
  source      = "../../modules/storage"
  environment = var.environment
}

output "bronze_bucket" {
  value = module.storage.bronze_bucket_name
}

output "silver_bucket" {
  value = module.storage.silver_bucket_name
}

output "gold_bucket" {
  value = module.storage.gold_bucket_name
}
