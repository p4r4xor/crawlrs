variable "name" {
  description = "Identifier for the RDS instance and the prefix for its security group."
  type        = string
}

variable "vpc_id" {
  description = "ID of the VPC to place the security group in."
  type        = string
}

variable "db_subnet_group_name" {
  description = "Name of an existing DB subnet group spanning the database subnets. The network module creates one, so this module consumes it rather than provisioning a second group over the same subnets."
  type        = string
}

variable "allowed_security_group_ids" {
  description = "Security groups granted ingress on the Postgres port. Pass the EKS node security group."
  type        = list(string)
}

variable "port" {
  description = "Port the instance listens on."
  type        = number
  default     = 5432
}

variable "engine_version" {
  description = "Postgres engine version."
  type        = string
  default     = "17.10"
}

variable "major_engine_version" {
  description = "Major version for the option group. Must match the major component of engine_version."
  type        = string
  default     = "17"
}

variable "parameter_group_family" {
  description = "Parameter group family. Must match the major component of engine_version."
  type        = string
  default     = "postgres17"
}

variable "instance_class" {
  description = "RDS instance class."
  type        = string
  default     = "db.t4g.medium"
}

variable "allocated_storage" {
  description = "Initial storage allocation in GB."
  type        = number
  default     = 20
}

variable "max_allocated_storage" {
  description = "Ceiling for storage autoscaling in GB. The ledger's history table is append-only and grows for the length of a run, and the metadata commit sits on the worker's critical path, so a full disk stalls every worker at once instead of degrading. Setting this equal to allocated_storage disables autoscaling."
  type        = number
  default     = 100
}

variable "db_name" {
  description = "Name of the database created on the instance."
  type        = string
  default     = "crawlrs"
}

variable "username" {
  description = "Master username. RDS generates the password, stores it in Secrets Manager, and rotates it, so no password variable exists here and none reaches Terraform state."
  type        = string
  default     = "crawlrs"
}

variable "multi_az" {
  description = "Run a synchronous standby in a second availability zone. Doubles the instance cost and is the only thing that makes a zone failure survivable without restoring from a backup."
  type        = bool
  default     = false
}

variable "backup_retention_period" {
  description = "Days of automated backups to keep. 0 disables backups."
  type        = number
  default     = 7
}

variable "deletion_protection" {
  description = "Refuse to delete the instance until this is turned off. Losing the ledger loses cross-run dedup, so every URL already crawled gets fetched again."
  type        = bool
  default     = true
}

variable "skip_final_snapshot" {
  description = "Delete the instance without taking a final snapshot. Unrecoverable outside a throwaway environment."
  type        = bool
  default     = false
}

variable "performance_insights_enabled" {
  description = "Enable Performance Insights. Free at 7 days retention on most instance classes."
  type        = bool
  default     = true
}

variable "apply_immediately" {
  description = "Apply modifications at once instead of waiting for the maintenance window. Some changes force a reboot."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
