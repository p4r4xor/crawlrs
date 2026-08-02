output "bucket" {
  description = "Bucket name. Goes into the chart's store.backend.s3.bucket value."
  value       = aws_s3_bucket.this.id
}

output "bucket_arn" {
  description = "Bucket ARN."
  value       = aws_s3_bucket.this.arn
}

output "irsa_role_arn" {
  description = "ARN of the IRSA role. Annotate the crawler ServiceAccount with it as eks.amazonaws.com/role-arn."
  value       = module.irsa.arn
}
