output "s3_admin_access_key" {
  value     = aws_iam_access_key.s3_iceberg_bucket_admin.id
  sensitive = true
}

output "s3_admin_access_secret" {
  value     = aws_iam_access_key.s3_iceberg_bucket_admin.secret
  sensitive = true
}

output "role_arn" {
  value = aws_iam_role.s3_admin.arn
}