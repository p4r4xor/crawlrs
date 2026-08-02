terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # bucket, region, encrypt, and use_lockfile come from
  # terraform/backend.hcl via `tofu init -backend-config`.
  backend "s3" {
    key = "aws-dev/data/terraform.tfstate"
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "crawlrs"
      Environment = var.environment
      ManagedBy   = "terraform"
      Layer       = "data"
    }
  }
}
