output "cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS cluster API endpoint"
  value       = module.eks.cluster_endpoint
}

output "aws_account_id" {
  description = "AWS account ID"
  value       = data.aws_caller_identity.current.account_id
}

output "migration_controller_role_arn" {
  description = "ARN of the migration controller IAM role for IRSA"
  value       = aws_iam_role.migration_controller.arn
}

output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}
