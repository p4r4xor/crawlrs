variable "region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod). Used in resource naming and tags."
  type        = string
  default     = "dev"
}

variable "cluster_name" {
  description = "EKS cluster name. All related resources derive names from this."
  type        = string
  default     = "crawlrs-dev"
}

# -- Networking ----------------------------------------------------------

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.0.0.0/16"
}

variable "azs" {
  description = "Availability zones. EKS requires at least two."
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

# -- EKS ----------------------------------------------------------------

variable "eks_node_instance_types" {
  description = "Instance types for the managed node group."
  type        = list(string)
  default     = ["m6i.xlarge"]
}

variable "eks_node_min" {
  description = "Minimum nodes in the managed node group."
  type        = number
  default     = 2
}

variable "eks_node_max" {
  description = "Maximum nodes in the managed node group."
  type        = number
  default     = 10
}

variable "eks_node_desired" {
  description = "Desired nodes in the managed node group."
  type        = number
  default     = 3
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version."
  type        = string
  default     = "1.32"
}

# -- Valkey (ElastiCache) ------------------------------------------------

variable "valkey_node_type" {
  description = "ElastiCache node type for the Valkey cluster."
  type        = string
  default     = "cache.r7g.large"
}

variable "valkey_num_cache_nodes" {
  description = "Number of cache nodes. 1 for dev, 2+ for prod (multi-AZ)."
  type        = number
  default     = 1
}

variable "valkey_engine_version" {
  description = "Valkey engine version on ElastiCache."
  type        = string
  default     = "8.0"
}

# -- Postgres (RDS) ------------------------------------------------------

variable "postgres_instance_class" {
  description = "RDS instance class for the metadata ledger."
  type        = string
  default     = "db.t4g.medium"
}

variable "postgres_engine_version" {
  description = "Postgres engine version."
  type        = string
  default     = "17.4"
}

variable "postgres_allocated_storage" {
  description = "Allocated storage in GB."
  type        = number
  default     = 20
}

variable "postgres_db_name" {
  description = "Database name for crawlrs metadata."
  type        = string
  default     = "crawlrs"
}

variable "postgres_username" {
  description = "Master username for the RDS instance."
  type        = string
  default     = "crawlrs"
}

# -- S3 (blob store) -----------------------------------------------------

variable "s3_bucket_prefix" {
  description = "Prefix for the S3 bucket name. Full name is {prefix}-{environment}-{region}."
  type        = string
  default     = "crawlrs-data"
}

variable "s3_force_destroy" {
  description = "Allow terraform destroy to empty and delete the bucket. Set false in prod."
  type        = bool
  default     = true
}
