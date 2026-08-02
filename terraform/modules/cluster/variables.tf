variable "name" {
  description = "EKS cluster name. Node groups, IAM roles, and security groups derive their names from it."
  type        = string
}

variable "kubernetes_version" {
  description = "EKS control plane version. Upgrades move one minor version at a time."
  type        = string
  default     = "1.34"
}

variable "vpc_id" {
  description = "ID of the VPC to place the cluster in."
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for the control plane network interfaces and the worker nodes. Pass the private subnets."
  type        = list(string)
}

variable "endpoint_public_access" {
  description = "Expose the Kubernetes API server publicly. Required to reach the cluster with kubectl or helm from outside the VPC."
  type        = bool
  default     = true
}

variable "endpoint_public_access_cidrs" {
  description = "CIDRs allowed to reach the public API endpoint. The default is open to the internet; EKS still enforces IAM authentication on that endpoint, but narrowing this to your egress ranges removes the unauthenticated attack surface."
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
  description = "Nodes to launch at creation. A cluster autoscaler, once installed, owns this value afterwards and Terraform stops being the source of truth for it."
  type        = number
  default     = 3
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
