output "vpc_id" {
  description = "ID of the VPC."
  value       = module.vpc.vpc_id
}

output "vpc_cidr" {
  description = "CIDR block of the VPC."
  value       = module.vpc.vpc_cidr_block
}

output "public_subnets" {
  description = "IDs of the public subnets (NAT gateways, internet-facing load balancers)."
  value       = module.vpc.public_subnets
}

output "private_subnets" {
  description = "IDs of the private subnets. EKS worker nodes live here."
  value       = module.vpc.private_subnets
}

output "database_subnets" {
  description = "IDs of the database subnets. RDS and ElastiCache live here."
  value       = module.vpc.database_subnets
}

output "database_subnet_group_name" {
  description = "Name of the RDS DB subnet group spanning the database subnets."
  value       = module.vpc.database_subnet_group_name
}
