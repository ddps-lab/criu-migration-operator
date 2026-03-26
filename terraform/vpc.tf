module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.prefix}-criu-migration-vpc"
  cidr = var.vpc_cidr

  azs            = slice(data.aws_availability_zones.available.names, 0, 2)
  public_subnets = [cidrsubnet(var.vpc_cidr, 8, 1), cidrsubnet(var.vpc_cidr, 8, 2)]

  # Public-only: no NAT gateway needed
  enable_nat_gateway = false

  # Auto-assign public IPs for nodes in public subnets
  map_public_ip_on_launch = true

  public_subnet_tags = {
    "kubernetes.io/role/elb"                                          = "1"
    "kubernetes.io/cluster/${var.prefix}-criu-migration-cluster" = "shared"
  }

  tags = var.tags
}
