output "region" {
  description = "AWS region this environment lives in."
  value       = var.region
}

output "name_prefix" {
  description = "The {project}-{environment} prefix every resource name derives from."
  value       = local.name
}

output "vpc_id" {
  description = "ID of the VPC."
  value       = module.network.vpc_id
}

output "private_subnets" {
  description = "IDs of the private subnets."
  value       = module.network.private_subnets
}

output "database_subnets" {
  description = "IDs of the database subnets."
  value       = module.network.database_subnets
}

output "database_subnet_group_name" {
  description = "Name of the RDS DB subnet group."
  value       = module.network.database_subnet_group_name
}

output "cluster_name" {
  description = "EKS cluster name."
  value       = module.cluster.cluster_name
}

output "cluster_endpoint" {
  description = "Kubernetes API server endpoint."
  value       = module.cluster.cluster_endpoint
}

output "node_security_group_id" {
  description = "Security group attached to the worker nodes."
  value       = module.cluster.node_security_group_id
}

output "oidc_provider_arn" {
  description = "ARN of the cluster's IAM OIDC provider, for IRSA roles."
  value       = module.cluster.oidc_provider_arn
}

output "configure_kubectl" {
  description = "Command that points kubectl at this cluster."
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.cluster.cluster_name}"
}
