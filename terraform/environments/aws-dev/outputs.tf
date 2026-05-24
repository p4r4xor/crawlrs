# ---------------------------------------------------------------------------
# These outputs are the interface between Terraform and Helm. After
# `terraform apply`, use the values to populate `helm install --set`:
#
#   helm install crawlrs ./charts/crawlrs \
#     --set redis.url=$(terraform output -raw valkey_endpoint) \
#     --set postgres.url=$(terraform output -raw postgres_url) \
#     --set store.backend.kind=s3 \
#     --set store.backend.s3.bucket=$(terraform output -raw s3_bucket) \
#     --set store.backend.s3.region=$(terraform output -raw region)
# ---------------------------------------------------------------------------

# -- Cluster --------------------------------------------------------------

output "cluster_name" {
  description = "EKS cluster name. Use with `aws eks update-kubeconfig`."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS API server endpoint."
  value       = module.eks.cluster_endpoint
}

output "region" {
  description = "AWS region."
  value       = var.region
}

output "configure_kubectl" {
  description = "Command to configure kubectl for this cluster."
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

# -- Valkey ----------------------------------------------------------------

output "valkey_endpoint" {
  description = "Valkey primary endpoint in redis:// URL form for crawlrs config."
  value       = "redis://${aws_elasticache_replication_group.valkey.primary_endpoint_address}:6379"
}

# -- Postgres --------------------------------------------------------------

output "postgres_endpoint" {
  description = "RDS Postgres endpoint (host:port)."
  value       = module.rds.db_instance_endpoint
}

output "postgres_url" {
  description = "Postgres connection URL for crawlrs config. Password is managed by RDS; retrieve via AWS Secrets Manager."
  value       = "postgres://${var.postgres_username}@${module.rds.db_instance_endpoint}/${var.postgres_db_name}"
  sensitive   = true
}

output "postgres_secret_arn" {
  description = "ARN of the Secrets Manager secret holding the RDS master password."
  value       = module.rds.db_instance_master_user_secret_arn
}

# -- S3 -------------------------------------------------------------------

output "s3_bucket" {
  description = "S3 bucket name for Parquet + WARC output."
  value       = aws_s3_bucket.data.id
}

output "s3_irsa_role_arn" {
  description = "IAM role ARN for IRSA. Annotate the crawlrs ServiceAccount with this."
  value       = module.s3_irsa.iam_role_arn
}
