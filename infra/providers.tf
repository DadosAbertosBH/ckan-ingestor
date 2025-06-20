terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "2.31.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "5.60.0"
    }
  }
}

provider "aws" {
  region     = "us-east-1"
  access_key = var.access_key
  secret_key = var.secret_key
}
