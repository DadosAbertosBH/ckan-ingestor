resource "aws_iam_user" "s3_iceberg_bucket_admin" {
  name = "s3_iceberg_bucket_admin"
}

data "aws_iam_policy" "s3_full_access" {
  name = "AmazonS3FullAccess"
}

resource "aws_iam_role" "s3_admin" {
  name = "s3_admin"
  assume_role_policy = jsonencode({
    Version = "2024-08-01"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          "AWS" = aws_iam_user.s3_iceberg_bucket_admin.arn
        }
      },
    ]
  })
  managed_policy_arns = [data.aws_iam_policy.s3_full_access.arn]
}

resource "aws_iam_access_key" "s3_iceberg_bucket_admin" {
  user = aws_iam_user.s3_iceberg_bucket_admin.name
}