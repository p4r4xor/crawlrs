# Cluster

Provisions the EKS control plane, one managed node group, and the four addons the crawler needs.

## 1. What this does not provision

Nothing inside the cluster. No `helm_release`, no `kubernetes_*` resource, and no `kubernetes` or `helm` provider block. Workloads are the Helm chart's job.

Keeping those out is what lets a single `tofu apply` create this cluster. Configure a Kubernetes provider against a cluster the same run is creating and Terraform has to authenticate to an API server that does not exist at plan time. The usual workaround, an `aws_eks_cluster_auth` token, expires 15 minutes after it is issued, so a slow rollout fails partway through the apply.

## 2. Addons

| Addon | What it gives you |
|---|---|
| `aws-ebs-csi-driver` | EBS-backed StatefulSet volumes. The node role also carries `AmazonEBSCSIDriverPolicy` for the attach and detach calls |
| `coredns` | In-cluster DNS |
| `kube-proxy` | Service routing |
| `vpc-cni` | Pod networking; assigns each pod an IP from the node's subnet |

All four track `most_recent`, so an apply picks up addon updates. Pin them if you want that to be an explicit change instead.

## 3. API endpoint access

`endpoint_public_access` is on, because kubectl and helm from outside the VPC need it. `endpoint_public_access_cidrs` defaults to the whole internet.

That default is survivable only because EKS enforces IAM authentication on the public endpoint. Narrow it to your egress ranges and you remove the unauthenticated surface entirely.

## 4. Node group sizing

`node_desired` is the count at creation. Once a cluster autoscaler is installed it owns that number, and Terraform stops being the source of truth: a later apply will try to reset it. Either leave the autoscaler off, or drop `node_desired` from your plan diffs with a `lifecycle.ignore_changes` in your own wrapper.

## 5. IRSA

The module creates the cluster's IAM OIDC provider and exposes it as `oidc_provider_arn`. The object-store module consumes that to build the role the crawler pods assume. Pod Identity is available on this cluster too; the object-store module uses IRSA.

## 6. Edgecases

### "kubectl says my user has no access after a fresh apply"

The module sets `enable_cluster_creator_admin_permissions`, so whoever ran the apply gets cluster-admin. If a different principal is running kubectl, add an access entry for it.

### "An apply keeps resetting my node count"

A cluster autoscaler is managing it. See section 4.

### "I need to jump two Kubernetes minor versions"

EKS upgrades move one minor version at a time. Change `kubernetes_version` by one, apply, then repeat.

## 7. Limits and numbers

| Thing | Value |
|---|---|
| Default Kubernetes version | 1.34 |
| Default instance type | `m6i.xlarge` |
| Node group size | 2 minimum, 3 desired, 10 maximum |
| Upgrade granularity | one minor version per apply |
| Kubernetes objects created | none |
