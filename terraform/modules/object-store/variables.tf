variable "bucket_name" {
  description = "Globally unique S3 bucket name for crawl output."
  type        = string
}

variable "force_destroy" {
  description = "Let a destroy empty the bucket first. Left off, destroying an environment fails while any output object remains, which is the behaviour you want anywhere the output matters."
  type        = bool
  default     = false
}

variable "role_name" {
  description = "Name of the IRSA role the crawler pods assume."
  type        = string
}

variable "oidc_provider_arn" {
  description = "ARN of the EKS cluster's IAM OIDC provider. The cluster module outputs it."
  type        = string
}

variable "namespace_service_accounts" {
  description = "ServiceAccounts allowed to assume the role, as namespace:name. Must match the namespace the chart installs into and the ServiceAccount it creates; a mismatch surfaces as AccessDenied on the first write, not at apply time."
  type        = list(string)
  default     = ["crawlrs:crawlrs"]
}

variable "transition_to_ia_days" {
  description = "Age in days at which output moves to STANDARD_IA. 0 disables the transition. Objects under 128 KB are billed at the 128 KB minimum in IA, so a run producing many small Parquet files costs more tiered down than left in Standard."
  type        = number
  default     = 30
}

variable "transition_to_glacier_days" {
  description = "Age in days at which output moves to GLACIER_IR. 0 disables the transition. GLACIER_IR keeps millisecond retrieval, unlike the deeper Glacier tiers."
  type        = number
  default     = 90
}

variable "expiration_days" {
  description = "Age in days at which output is deleted outright. 0 keeps it forever. Nothing in the crawler deletes output, so this and the transitions above are the only bound on what the bucket costs."
  type        = number
  default     = 0
}

variable "noncurrent_version_expiration_days" {
  description = "Days to keep superseded object versions after an overwrite."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
