module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = "${var.prefix}-criu-migration-test"
  cluster_version = var.cluster_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access  = true
  cluster_endpoint_private_access = true

  # Enable IRSA
  enable_irsa = true

  # Spot node group only (r8gn.large, use1-az6)
  # r8gn는 ARM 기반이므로 AL2_ARM_64 AMI 사용
  eks_managed_node_groups = {
    spot = {
      name           = "${var.prefix}-spot-ng-r8gn"
      instance_types = [var.spot_instance_type]
      min_size       = var.spot_node_min_size
      max_size       = var.spot_node_max_size
      desired_size   = var.spot_node_desired_size
      # ami_type: 기본값 AL2_x86_64 사용 (m5.large 등 x86 인스턴스)
      # ARM 인스턴스(r8gn 등)를 사용하려면 ami_type = "AL2_ARM_64" 추가

      capacity_type = "SPOT"

      # Use first subnet which maps to us-east-1a, update if needed for use1-az6 specific subnet
      subnet_ids = [module.vpc.private_subnets[0]]

      labels = {
        "capacity-type"           = "spot"
        "node.kubernetes.io/spot" = "true"
      }

      taints = [
        {
          key    = "capacity-type"
          value  = "spot"
          effect = "NO_SCHEDULE"
        }
      ]

      # Enforce IMDSv2 and allow DaemonSet pods to access IMDS
      metadata_options = {
        http_endpoint               = "enabled"
        http_tokens                 = "required"  # IMDSv2 enforcement
        http_put_response_hop_limit = 2           # Allow access from pods
      }

      tags = merge(
        var.tags,
        {
          "NodeGroup" = "spot"
        }
      )
    }
  }

  tags = merge(
    var.tags,
    {
      "Cluster" = "${var.prefix}-criu-migration-test"
    }
  )
}

# Add current user to cluster access (for kubectl)
resource "aws_eks_access_entry" "current_user" {
  cluster_name       = module.eks.cluster_name
  principal_arn      = data.aws_caller_identity.current.arn
  type               = "STANDARD"

  depends_on = [module.eks]
}

resource "aws_eks_access_policy_association" "current_user_admin" {
  cluster_name       = module.eks.cluster_name
  principal_arn      = aws_eks_access_entry.current_user.principal_arn
  policy_arn         = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"

  access_scope {
    type = "cluster"
  }

  depends_on = [aws_eks_access_entry.current_user]
}
