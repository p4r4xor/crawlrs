output "endpoint" {
  description = "Instance endpoint as host:port."
  value       = module.rds.db_instance_endpoint
}

output "address" {
  description = "Instance hostname, without the port."
  value       = module.rds.db_instance_address
}

output "port" {
  description = "Port the instance listens on."
  value       = var.port
}

output "db_name" {
  description = "Name of the database on the instance."
  value       = var.db_name
}

output "username" {
  description = "Master username."
  value       = var.username
}

output "master_user_secret_arn" {
  description = "ARN of the Secrets Manager secret holding the RDS-managed master password. Wire this into External Secrets Operator, or read it once and put it in the crawler's Secret."
  value       = module.rds.db_instance_master_user_secret_arn
}

output "security_group_id" {
  description = "Security group guarding the instance."
  value       = aws_security_group.this.id
}
