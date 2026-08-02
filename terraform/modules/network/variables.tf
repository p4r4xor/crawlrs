variable "name" {
  description = "Name for the VPC and the prefix for every subnet, route table, and gateway in it."
  type        = string
}

variable "cidr" {
  description = "CIDR block for the VPC. Subnet ranges are derived from it, three per availability zone at a /20 each, so a /16 is the expected shape."
  type        = string
  default     = "10.0.0.0/16"

  validation {
    condition     = tonumber(split("/", var.cidr)[1]) <= 16
    error_message = "The VPC CIDR must be /16 or larger to fit three /20 subnet tiers across three availability zones."
  }
}

variable "azs" {
  description = "Availability zones to spread subnets across. EKS requires at least two."
  type        = list(string)

  validation {
    condition     = length(var.azs) >= 2
    error_message = "EKS requires subnets in at least two availability zones."
  }
}

variable "single_nat_gateway" {
  description = "Route all private-subnet egress through one NAT gateway instead of one per zone. Saves two hourly charges and makes outbound traffic depend on a single zone staying up; when that zone fails the crawler's fetch stage stops."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
