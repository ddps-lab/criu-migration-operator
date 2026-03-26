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

resource "aws_iam_role" "migration_controller" {
  name               = "${var.prefix}-CRIUMigrationControllerRole"
  assume_role_policy = data.aws_iam_policy_document.migration_controller_assume.json

  tags = var.tags
}

resource "aws_iam_role_policy" "migration_controller" {
  name = "criu-migration-controller-policy"
  role = aws_iam_role.migration_controller.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = ["*"]
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
        Resource = [
          "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/eks/*",
          "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:${var.prefix}-*"
        ]
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
