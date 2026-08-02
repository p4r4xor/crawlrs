variable "region" {
  description = "AWS region that holds the state bucket. Every root module's backend must point at this same region."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for the state bucket name. The full name is {prefix}-tfstate-{account_id}-{region}, which stays globally unique without you picking one by hand."
  type        = string
  default     = "crawlrs"
}

variable "noncurrent_version_expiration_days" {
  description = "Days to keep superseded state file versions. Versioning is the recovery path for a corrupted or truncated state push, so this is how long you have to notice and roll back."
  type        = number
  default     = 90
}
