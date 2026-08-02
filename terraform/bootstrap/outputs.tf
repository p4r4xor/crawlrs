output "state_bucket" {
  description = "Name of the S3 bucket holding Terraform state for every other root module."
  value       = aws_s3_bucket.state.id
}

output "backend_config" {
  description = "Contents of terraform/backend.hcl. Write this to that path so the environment root modules can init against the bucket: tofu output -raw backend_config > ../backend.hcl"
  value       = <<-EOT
    bucket       = "${aws_s3_bucket.state.id}"
    region       = "${var.region}"
    encrypt      = true
    use_lockfile = true
  EOT
}
