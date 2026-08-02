locals {
  public_subnets   = [for i, az in var.azs : cidrsubnet(var.cidr, 4, i)]
  private_subnets  = [for i, az in var.azs : cidrsubnet(var.cidr, 4, i + length(var.azs))]
  database_subnets = [for i, az in var.azs : cidrsubnet(var.cidr, 4, i + length(var.azs) * 2)]
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 6.0"

  name = var.name
  cidr = var.cidr
  azs  = var.azs

  public_subnets   = local.public_subnets
  private_subnets  = local.private_subnets
  database_subnets = local.database_subnets

  create_database_subnet_group = true

  enable_nat_gateway   = true
  single_nat_gateway   = var.single_nat_gateway
  enable_dns_hostnames = true
  enable_dns_support   = true

  public_subnet_tags = {
    "kubernetes.io/role/elb" = 1
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = 1
  }

  tags = var.tags
}
