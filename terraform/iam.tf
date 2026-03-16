# AssumeRole policy for IRSA (migration-controller ServiceAccount)
data "aws_iam_policy_document" "migration_controller_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.oidc_provider, "https://", "")}:sub"
      values   = ["system:serviceaccount:migration-system:migration-controller"]
    }

    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.oidc_provider, "https://", "")}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

# IAM Role for migration controller
resource "aws_iam_role" "migration_controller" {
  name               = "${var.prefix}-CRIUMigrationControllerRole"
  assume_role_policy = data.aws_iam_policy_document.migration_controller_assume.json

  tags = merge(
    var.tags,
    {
      "Role" = "migration-controller"
    }
  )
}

# S3 and EC2 permissions for migration controller
resource "aws_iam_role_policy" "migration_controller" {
  name = "criu-migration-controller-policy"
  role = aws_iam_role.migration_controller.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3express:CreateSession"
        ]
        Resource = [
          aws_s3_directory_bucket.criu_checkpoints.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_directory_bucket.criu_checkpoints.arn,
          "${aws_s3_directory_bucket.criu_checkpoints.arn}/*"
        ]
      },
    {
      Effect = "Allow"
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams"
      ]
      Resource = "arn:aws:logs:us-east-1:786382940258:log-group:/aws/eks/*"
    },
      {
        Effect = "Allow"
        Action = [
          "ec2:DescribeInstances",
          "ec2:DescribeTags"
        ]
        Resource = "*"
      }
    ]
  })
}

# Output role ARN for Kubernetes annotation
output "migration_controller_role_arn" {
  description = "ARN of the migration controller IAM role for IRSA"
  value       = aws_iam_role.migration_controller.arn
}
