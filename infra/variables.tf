variable "access_key" {
  type      = string
  sensitive = true
}

variable "secret_key" {
  type      = string
  sensitive = true
}

variable "s3_access_key" {
  type      = string
  sensitive = true
}

variable "s3_secret_key" {
  type      = string
  sensitive = true
}

variable "s3_endpoint" {
  type      = string
  sensitive = false
}

variable "s3_region" {
  type      = string
  sensitive = false
}

variable "image_tag" {
  type      = string
  sensitive = false
}

variable "ducklake_db_host" {
  type      = string
  sensitive = false
}


variable "ducklake_db_database" {
  type      = string
  sensitive = false
}

variable "ducklake_db_user" {
  type      = string
  sensitive = false
}

variable "ducklake_db_password" {
  type      = string
  sensitive = true
}