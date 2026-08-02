output "valkey_url" {
  description = "Connection URL for the chart's redis.url value. Scheme is rediss:// when transit encryption is on."
  value       = module.valkey.url
}

output "valkey_endpoint" {
  description = "Valkey primary hostname, without a scheme or port."
  value       = module.valkey.primary_endpoint_address
}

output "postgres_endpoint" {
  description = "RDS endpoint as host:port."
  value       = module.postgres.endpoint
}

output "postgres_url_template" {
  description = "Postgres URL with a PASSWORD placeholder. The password is RDS-managed and lives in Secrets Manager; substitute it from postgres_secret_arn rather than storing it here, since anything interpolated into an output is written to state in cleartext."
  value       = "postgres://${module.postgres.username}:PASSWORD@${module.postgres.endpoint}/${module.postgres.db_name}"
}

output "postgres_secret_arn" {
  description = "ARN of the Secrets Manager secret holding the RDS-managed master password."
  value       = module.postgres.master_user_secret_arn
}

output "s3_bucket" {
  description = "Output bucket name, for the chart's store.backend.s3.bucket value."
  value       = module.object_store.bucket
}

output "s3_irsa_role_arn" {
  description = "IRSA role ARN. Annotate the crawler ServiceAccount with it as eks.amazonaws.com/role-arn."
  value       = module.object_store.irsa_role_arn
}

output "region" {
  description = "AWS region, for the chart's store.backend.s3.region value."
  value       = var.region
}
