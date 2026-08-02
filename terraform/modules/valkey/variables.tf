variable "name" {
  description = "Name for the replication group, subnet group, parameter group, and security group."
  type        = string
}

variable "vpc_id" {
  description = "ID of the VPC to place the security group in."
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for the cache subnet group. Pass the database subnets."
  type        = list(string)
}

variable "allowed_security_group_ids" {
  description = "Security groups granted ingress on the Valkey port. Pass the EKS node security group; granting to a group rather than a CIDR keeps the rule correct when node subnets change."
  type        = list(string)
}

variable "port" {
  description = "Port the replication group listens on."
  type        = number
  default     = 6379
}

variable "node_type" {
  description = "ElastiCache node type. The frontier is memory-bound rather than CPU-bound, so the r-family gives more useful headroom per dollar than the m-family."
  type        = string
  default     = "cache.r7g.large"
}

variable "num_cache_clusters" {
  description = "Nodes in the replication group. At 1 there is no replica to fail over to, so a node replacement is a full outage of the queue. At 2 or more the module turns on automatic failover and multi-AZ."
  type        = number
  default     = 1
}

variable "engine_version" {
  description = "Valkey engine version. 8.1 is the floor: the frontier dedups at submit time with BF.RESERVE and BF.ADD, and ElastiCache exposes the Bloom filter data type from 8.1 onward. Below that the crawler connects successfully and then fails on every submit."
  type        = string
  default     = "9.1"

  validation {
    condition = (
      tonumber(split(".", var.engine_version)[0]) > 8 ||
      (tonumber(split(".", var.engine_version)[0]) == 8 &&
      tonumber(try(split(".", var.engine_version)[1], 0)) >= 1)
    )
    error_message = "Valkey 8.1 or later is required; earlier versions have no Bloom filter support on ElastiCache and the frontier cannot dedup."
  }
}

variable "parameter_group_family" {
  description = "Parameter group family. Tracks the major version in engine_version; confirm the exact string with `aws elasticache describe-cache-engine-versions --engine valkey`."
  type        = string
  default     = "valkey9"
}

variable "transit_encryption_enabled" {
  description = "Encrypt client connections with TLS. When on, the crawler's redis URL must use the rediss:// scheme; the module's url output already picks the right one. Turning it off saves roughly a millisecond per round trip and only makes sense inside a trusted VPC."
  type        = bool
  default     = true
}

variable "snapshot_retention_limit" {
  description = "Days of automatic snapshots to keep. Bloom filter state has no other source, so a node replaced without a snapshot makes the crawler re-fetch every URL it had already deduped."
  type        = number
  default     = 1

  validation {
    condition     = var.snapshot_retention_limit >= 1
    error_message = "Snapshotting must stay enabled; Bloom filter state is not reconstructible from any other source."
  }
}

variable "snapshot_window" {
  description = "Daily UTC window for the automatic snapshot, as hh:mm-hh:mm. Must not overlap maintenance_window."
  type        = string
  default     = "03:00-04:00"
}

variable "maintenance_window" {
  description = "Weekly UTC window for engine maintenance, as ddd:hh:mm-ddd:hh:mm."
  type        = string
  default     = "sun:04:00-sun:05:00"
}

variable "apply_immediately" {
  description = "Apply modifications at once instead of waiting for the maintenance window. Some changes reboot the node, so this turns a routine edit into unscheduled downtime."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
