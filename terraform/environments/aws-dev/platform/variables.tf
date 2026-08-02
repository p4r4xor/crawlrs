variable "region" {
  description = "AWS region. Must match the region in backend.hcl."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name. Combined with environment to name every resource."
  type        = string
  default     = "crawlrs"
}

variable "environment" {
  description = "Environment name. Combined with project to name every resource."
  type        = string
  default     = "dev"
}

# -- Networking ----------------------------------------------------------

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.0.0.0/16"
}

variable "azs" {
  description = "Availability zones to spread subnets across."
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

variable "single_nat_gateway" {
  description = "Route all private egress through one NAT gateway. Cheaper, and a single point of failure."
  type        = bool
  default     = true
}

# -- Cluster -------------------------------------------------------------

variable "kubernetes_version" {
  description = "EKS control plane version."
  type        = string
  default     = "1.34"
}

variable "cluster_endpoint_public_access" {
  description = "Expose the Kubernetes API server publicly."
  type        = bool
  default     = true
}

variable "cluster_endpoint_public_access_cidrs" {
  description = "CIDRs allowed to reach the public API endpoint. Narrow this to your egress ranges outside dev."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "node_instance_types" {
  description = "Instance types for the managed node group."
  type        = list(string)
  default     = ["m6i.xlarge"]
}

variable "node_min" {
  description = "Minimum nodes in the managed node group."
  type        = number
  default     = 2
}

variable "node_max" {
  description = "Maximum nodes in the managed node group."
  type        = number
  default     = 10
}

variable "node_desired" {
  description = "Desired nodes in the managed node group at creation."
  type        = number
  default     = 3
}
