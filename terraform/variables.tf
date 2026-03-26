variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "AWS CLI profile"
  type        = string
  default     = "default"
}

variable "prefix" {
  description = "Resource name prefix"
  type        = string
}

variable "cluster_version" {
  description = "EKS cluster Kubernetes version"
  type        = string
  default     = "1.31"
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "worker_instance_type" {
  description = "EC2 instance type for worker nodes"
  type        = string
  default     = "m5.large"
}

variable "worker_desired_size" {
  description = "Desired number of worker nodes"
  type        = number
  default     = 2
}

variable "worker_min_size" {
  description = "Minimum number of worker nodes"
  type        = number
  default     = 1
}

variable "worker_max_size" {
  description = "Maximum number of worker nodes"
  type        = number
  default     = 3
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
