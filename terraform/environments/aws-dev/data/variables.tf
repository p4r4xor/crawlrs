variable "region" {
  description = "AWS region. Must match the region in backend.hcl and the platform layer."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name, used in the default tags."
  type        = string
  default     = "dev"
}

variable "state_bucket" {
  description = "Name of the S3 bucket holding the platform layer's state. The bootstrap module outputs it. A backend block cannot take a variable, so this is passed separately from backend.hcl and must hold the same value."
  type        = string
}

variable "apply_immediately" {
  description = "Apply modifications to Valkey and Postgres at once instead of waiting for their maintenance windows."
  type        = bool
  default     = false
}

# -- Valkey --------------------------------------------------------------

variable "valkey_node_type" {
  description = "ElastiCache node type."
  type        = string
  default     = "cache.r7g.large"
}

variable "valkey_num_cache_clusters" {
  description = "Nodes in the replication group. 2 or more enables automatic failover and multi-AZ."
  type        = number
  default     = 1
}

variable "valkey_engine_version" {
  description = "Valkey engine version. 8.1 is the floor; below that ElastiCache has no Bloom filter type and the frontier cannot dedup."
  type        = string
  default     = "9.1"
}

variable "valkey_transit_encryption_enabled" {
  description = "Encrypt client connections with TLS. When true the crawler's redis URL must use the rediss:// scheme."
  type        = bool
  default     = true
}

# -- Postgres ------------------------------------------------------------

variable "postgres_instance_class" {
  description = "RDS instance class."
  type        = string
  default     = "db.t4g.medium"
}

variable "postgres_engine_version" {
  description = "Postgres engine version."
  type        = string
  default     = "17.10"
}

variable "postgres_allocated_storage" {
  description = "Initial storage allocation in GB."
  type        = number
  default     = 20
}

variable "postgres_max_allocated_storage" {
  description = "Ceiling for storage autoscaling in GB."
  type        = number
  default     = 100
}

variable "postgres_multi_az" {
  description = "Run a synchronous standby in a second AZ."
  type        = bool
  default     = false
}

variable "postgres_backup_retention_period" {
  description = "Days of automated backups to keep."
  type        = number
  default     = 7
}

variable "postgres_deletion_protection" {
  description = "Refuse to delete the instance until this is turned off."
  type        = bool
  default     = true
}

variable "postgres_skip_final_snapshot" {
  description = "Delete the instance without a final snapshot."
  type        = bool
  default     = false
}

# -- Object store --------------------------------------------------------

variable "s3_force_destroy" {
  description = "Let a destroy empty the output bucket first."
  type        = bool
  default     = false
}

variable "namespace_service_accounts" {
  description = "ServiceAccounts allowed to assume the S3 writer role, as namespace:name."
  type        = list(string)
  default     = ["crawlrs:crawlrs"]
}

variable "s3_transition_to_ia_days" {
  description = "Age in days at which output moves to STANDARD_IA. 0 disables."
  type        = number
  default     = 30
}

variable "s3_transition_to_glacier_days" {
  description = "Age in days at which output moves to GLACIER_IR. 0 disables."
  type        = number
  default     = 90
}

variable "s3_expiration_days" {
  description = "Age in days at which output is deleted. 0 keeps it forever."
  type        = number
  default     = 0
}
