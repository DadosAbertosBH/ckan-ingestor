resource "aws_iam_user" "s3_iceberg_bucket_admin" {
  name = "s3_iceberg_bucket_admin"
}

resource "aws_iam_access_key" "s3_iceberg_bucket_admin" {
  user = aws_iam_user.s3_iceberg_bucket_admin.name
}

resource "aws_s3_bucket" "public" {
  bucket = "pedalin-public"
}

resource "aws_iam_policy" "public_bucket_full_access" {
  name = "public_bucket_full_access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["s3:ListBucket"]
        Effect   = "Allow"
        Resource = [aws_s3_bucket.public.arn]
      },
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject"
        ],
        Resource = ["${aws_s3_bucket.public.arn}/*"]
      }
    ]
  })
}

resource "aws_iam_user_policy" "s3_admin_manage_bucket" {
  name   = "admin-manage-bucket"
  user   = aws_iam_user.s3_iceberg_bucket_admin.name
  policy = aws_iam_policy.public_bucket_full_access.policy
}

resource "aws_iam_role" "s3_admin" {
  name = "s3_admin"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
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
  managed_policy_arns = [aws_iam_policy.public_bucket_full_access.arn]
}

