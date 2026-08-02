# Terraform infrastructure

Provisions the AWS resources crawlrs runs on: a VPC, an EKS cluster, and the three backing stores the crawler talks to. The Helm chart in `charts/crawlrs/` deploys the application onto them. Everything you need to stand up an environment is on this page.

Managed with [OpenTofu](https://opentofu.org). The configs are Terraform-compatible, so `tofu` and `terraform` are interchangeable; the examples use `tofu`.

## 1. Layout

```
terraform/
  bootstrap/                    Creates the S3 state bucket. Local state; run first.
  modules/                      Building blocks. No provider blocks, no state of their own.
    network/                    VPC, three subnet tiers, NAT.
    cluster/                    EKS control plane, managed node group, addons.
    valkey/                     ElastiCache Valkey, parameter group, security group.
    postgres/                   RDS Postgres, security group.
    object-store/               S3 output bucket, lifecycle rules, IRSA role.
  environments/
    aws-dev/
      platform/                 Root module: network + cluster.
      data/                     Root module: valkey + postgres + object-store.
```

Every directory under `environments/` with a `versions.tf` is a root module with its own state. You `init`, `plan`, and `apply` each one separately.

## 2. How the layers split

**Platform** holds the VPC and the cluster. **Data** holds Valkey, Postgres, and the output bucket. Each has its own state file, so a plan in one cannot propose changes to the other.

This matters when you resize Postgres or edit a lifecycle rule. Those are routine, and they land in a plan that has no reason to read the cluster at all. Put both layers in one state file and every such edit renders a diff against the EKS cluster, where a misread can propose a replacement that takes 20 minutes and drops every running pod.

The split is by how often things change, not by AWS service. Splitting further, one root module per service, is what you do when separate teams own separate services. For one application with three backing stores it adds a `terraform_remote_state` hop per boundary and buys nothing.

Modules exist so `aws-prod` is a root module that sets different variables rather than a second copy of the resources.

## 3. State

One S3 bucket, created by `bootstrap/`. Each root module's key mirrors its path under `environments/`:

| Root module | State key |
|---|---|
| `environments/aws-dev/platform` | `aws-dev/platform/terraform.tfstate` |
| `environments/aws-dev/data` | `aws-dev/data/terraform.tfstate` |

Locking is S3-native (`use_lockfile = true`), so there is no DynamoDB table to provision, pay for, or grant access to. This needs OpenTofu 1.10 or Terraform 1.10 and above.

Backend blocks are **partial**: only `key` is inline. `bucket`, `region`, `encrypt`, and `use_lockfile` come from `terraform/backend.hcl`, so the bucket name lives in one file instead of one per root module. `backend.hcl` is gitignored because it names your account; `bootstrap` generates it for you.

## 4. Standing up an environment

```bash
# Create the state bucket. Local state, one time.
cd terraform/bootstrap
tofu init
tofu apply
tofu output -raw backend_config > ../backend.hcl

# Platform layer: VPC and EKS. Budget 15 to 20 minutes.
cd ../environments/aws-dev/platform
tofu init -backend-config=../../../backend.hcl
tofu apply

# Data layer: Valkey, Postgres, S3.
cd ../data
export TF_VAR_state_bucket=$(cd ../../../bootstrap && tofu output -raw state_bucket)
tofu init -backend-config=../../../backend.hcl
tofu apply
```

Platform applies first because the data layer reads its outputs through a `terraform_remote_state` data source. That is also why the data layer needs `TF_VAR_state_bucket`: a backend block cannot take a variable, so the bucket name arrives twice, once through `backend.hcl` for the data layer's own state and once as a variable for reading the platform layer's. Both must hold the same value.

## 5. Deploying the chart

```bash
cd terraform/environments/aws-dev
eval "$(tofu -chdir=platform output -raw configure_kubectl)"

helm install crawlrs ../../../charts/crawlrs \
  --namespace crawlrs --create-namespace \
  --set redis.url="$(tofu -chdir=data output -raw valkey_url)" \
  --set store.backend.kind=s3 \
  --set store.backend.s3.bucket="$(tofu -chdir=data output -raw s3_bucket)" \
  --set store.backend.s3.region="$(tofu -chdir=data output -raw region)" \
  --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"="$(tofu -chdir=data output -raw s3_irsa_role_arn)" \
  --set-file seeds.content=../../../local/seeds.txt
```

The Postgres password never enters Terraform state. RDS generates it, stores it in Secrets Manager, and rotates it. Take `postgres_secret_arn` from the data layer and resolve it with External Secrets Operator, or read it once into the chart's Secret. `postgres_url_template` gives you the URL shape with a `PASSWORD` placeholder to substitute.

## 6. What you own

**Apply order:** platform, then data. Destroy in reverse.

**Keeping the two bucket references in step:** `backend.hcl` and `TF_VAR_state_bucket` must name the same bucket. Sourcing both from the bootstrap output is what keeps them honest.

**Narrowing API access:** `cluster_endpoint_public_access_cidrs` defaults to the whole internet. EKS still enforces IAM authentication on that endpoint, but narrowing it to your egress ranges removes the unauthenticated surface.

**Committing `.terraform.lock.hcl`:** it pins provider versions and their checksums. Without it two people running the same config can resolve different providers.

**Bounding what the output bucket costs:** nothing in the crawler deletes output. The lifecycle transitions in the data layer are the only bound on the bill.

## 7. Edgecases

### "My crawler connects to Valkey but every submit fails"

Check the engine version. The frontier dedups at submit time with `BF.RESERVE` and `BF.ADD`, and ElastiCache exposes the Bloom filter data type from Valkey 8.1 onward. Older engines accept the connection and then reject every Bloom command, which reads like an application bug.

### "`tofu init` fails with a missing bucket or an empty backend"

You are missing `backend.hcl`, or you did not pass `-backend-config` to `init`. The backend block only carries `key`; the rest comes from that file.

### "The data layer plan errors on the remote state data source"

`TF_VAR_state_bucket` is unset, names a different bucket than `backend.hcl`, or the platform layer has not been applied yet, so there is no state object at `aws-dev/platform/terraform.tfstate`.

### "`tofu destroy` on the data layer hangs on the S3 bucket"

`force_destroy` is off and the bucket still holds output. That is deliberate outside throwaway environments. Set `s3_force_destroy = true` for a dev environment, or empty the bucket first.

### "I lost `bootstrap/terraform.tfstate`"

Nothing breaks. The bucket has `prevent_destroy` set and every other root module keeps working. Re-import it when you next need to change it: `tofu import aws_s3_bucket.state <bucket-name>`.

### "I want a production environment"

Create `environments/aws-prod/{platform,data}/`, copy the four `.tf` files from `aws-dev`, and change the backend `key` and the `environment` default. The module wiring is identical. Write a `terraform.tfvars` with the production sizing: `single_nat_gateway = false`, `valkey_num_cache_clusters = 2`, `postgres_multi_az = true`. Leave the output names alone; they are what the `helm install` command reads, and keeping them stable is what makes that command work against any environment.

## 8. Limits and numbers

| Thing | Value |
|---|---|
| OpenTofu / Terraform | 1.10 or later, for S3-native locking |
| AWS provider | `~> 6.0` |
| Valkey engine | 9.1 by default, 8.1 minimum |
| Postgres engine | 17.10 |
| Kubernetes | 1.34 |
| Platform layer apply time | 15 to 20 minutes on first run |
| State locking | S3 `.tflock` object, no DynamoDB table |
| Subnet tiers per zone | 3 (public, private, database) at /20 each |

## 9. Before you commit

```bash
tofu fmt -recursive terraform/
tofu validate    # per root module, after init
```
