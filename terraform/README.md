# Terraform infrastructure

Provisions the cloud infrastructure that crawlrs runs on. The Helm chart (`charts/crawlrs/`) deploys the application; Terraform creates the resources the chart connects to.

## Directory layout

```
terraform/
  environments/
    aws-dev/        # AWS dev environment (VPC + EKS + Valkey + RDS + S3)
```

Each environment is a self-contained root module. Add `aws-prod/`, `gcp-dev/`, etc. as needed; they wire the same logical resources with provider-specific implementations and environment-appropriate sizing.

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5
- AWS CLI configured (`aws configure` or env vars)
- Sufficient IAM permissions (EKS, ElastiCache, RDS, S3, VPC, IAM)

## Usage

```bash
cd terraform/environments/aws-dev
terraform init
terraform plan
terraform apply
```

After apply, configure kubectl and deploy crawlrs:

```bash
# Point kubectl at the new cluster
eval "$(terraform output -raw configure_kubectl)"

# Deploy crawlrs with Terraform outputs
helm install crawlrs ../../charts/crawlrs \
  --namespace crawlrs --create-namespace \
  --set redis.url="$(terraform output -raw valkey_endpoint)" \
  --set store.backend.kind=s3 \
  --set store.backend.s3.bucket="$(terraform output -raw s3_bucket)" \
  --set store.backend.s3.region="$(terraform output -raw region)" \
  --set-file seeds.content=../../local/seeds.txt
```

The Postgres password is managed by RDS via AWS Secrets Manager. Retrieve the secret ARN from `terraform output postgres_secret_arn` and wire it into the crawlrs Secret or use External Secrets Operator.

## What gets created

| Resource | Service | Purpose |
|---|---|---|
| VPC + subnets | VPC | Network isolation; private subnets for EKS/RDS/ElastiCache |
| EKS cluster + managed node group | EKS | Kubernetes control plane + worker nodes |
| ElastiCache replication group | ElastiCache (Valkey) | Frontier queue + politeness state + Bloom dedup |
| RDS instance | RDS (Postgres 17) | Metadata ledger (`url_metadata` + `url_history` + `frontier_outbox`) |
| S3 bucket | S3 | Parquet + WARC blob output |
| IAM role (IRSA) | IAM | Pod-level S3 write access without static credentials |
| Security groups | VPC | EKS nodes -> Valkey/Postgres access |

## Adding a new environment

1. Copy `aws-dev/` to `aws-prod/` (or `gcp-dev/` for a new provider).
2. Adjust `terraform.tfvars`: instance sizes, node counts, `s3_force_destroy=false`, multi-AZ, etc.
3. For a new provider, swap the module sources (e.g. `terraform-google-modules/kubernetes-engine/google` for GKE).
4. The outputs should keep the same shape (cluster endpoint, valkey URL, postgres URL, bucket name) so the `helm install` command doesn't change.
