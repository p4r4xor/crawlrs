# aws-dev / platform

Root module for the dev network and cluster. State key `aws-dev/platform/terraform.tfstate`.

```bash
tofu init -backend-config=../../../backend.hcl
tofu plan
tofu apply
```

Budget 15 to 20 minutes on a first apply. Most of that is the EKS control plane.

## 1. What it composes

**`modules/network`:** VPC, three subnet tiers per zone, one NAT gateway.

**`modules/cluster`:** EKS control plane, one managed node group, four addons.

## 2. Outputs are a published interface

The data layer reads this module's outputs through a `terraform_remote_state` data source. Rename one and the data layer's next plan breaks, so treat `outputs.tf` as contract rather than convenience.

The dependency runs one way. This layer never reads from the data layer.

## 3. Dev shape

`terraform.tfvars` carries only what differs from the defaults in `variables.tf`. Today that is `single_nat_gateway = true`, trading cross-zone egress redundancy for two fewer NAT gateway charges.

## 4. Edgecases

### "`tofu init` cannot find the backend"

You skipped `-backend-config=../../../backend.hcl`, or `backend.hcl` does not exist yet. Run the bootstrap module first.

### "Destroy fails because resources are still attached"

Destroy the data layer first. Its security groups and subnet group live in this layer's VPC.

## 5. Limits and numbers

| Thing | Value |
|---|---|
| State key | `aws-dev/platform/terraform.tfstate` |
| First apply | 15 to 20 minutes |
| Kubernetes version | 1.34 |
| NAT gateways | 1 |
| Destroy order | after the data layer |
