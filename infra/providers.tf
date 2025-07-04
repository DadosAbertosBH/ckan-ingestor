terraform {
  required_providers {
    helm = {
      source  = "hashicorp/helm"
      version = "3.0.1"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "2.31.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "3.6.3"
    }
  }
}