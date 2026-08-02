locals {
  name = "${var.project}-${var.environment}"
}

module "network" {
  source = "../../../modules/network"

  name = local.name
  cidr = var.vpc_cidr
  azs  = var.azs

  single_nat_gateway = var.single_nat_gateway
}

module "cluster" {
  source = "../../../modules/cluster"

  name               = local.name
  kubernetes_version = var.kubernetes_version

  vpc_id     = module.network.vpc_id
  subnet_ids = module.network.private_subnets

  endpoint_public_access       = var.cluster_endpoint_public_access
  endpoint_public_access_cidrs = var.cluster_endpoint_public_access_cidrs

  node_instance_types = var.node_instance_types
  node_min            = var.node_min
  node_max            = var.node_max
  node_desired        = var.node_desired
}
