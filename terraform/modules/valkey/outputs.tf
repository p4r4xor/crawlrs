output "primary_endpoint_address" {
  description = "Hostname of the primary node. Writes go here."
  value       = aws_elasticache_replication_group.this.primary_endpoint_address
}

output "reader_endpoint_address" {
  description = "Hostname of the reader endpoint. Only meaningful with more than one node."
  value       = aws_elasticache_replication_group.this.reader_endpoint_address
}

output "port" {
  description = "Port the replication group listens on."
  value       = var.port
}

output "url" {
  description = "Connection URL for the crawler's [redis].url config key. The scheme reflects transit_encryption_enabled: rediss:// when TLS is on, redis:// when it is off."
  value       = "${var.transit_encryption_enabled ? "rediss" : "redis"}://${aws_elasticache_replication_group.this.primary_endpoint_address}:${var.port}"
}

output "security_group_id" {
  description = "Security group guarding the replication group."
  value       = aws_security_group.this.id
}
