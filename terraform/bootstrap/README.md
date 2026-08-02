# Bootstrap

Creates the S3 bucket every other root module stores its state in. Run this once, before anything else.

```bash
tofu init
tofu apply
tofu output -raw backend_config > ../backend.hcl
```

The second command writes the backend configuration the environment root modules init against, so you do not paste the bucket name anywhere by hand.

## 1. Why this one keeps local state

It creates the state bucket, so it cannot store its state in it. `terraform.tfstate` stays in this directory and is gitignored.

Losing that file is survivable. The bucket carries `prevent_destroy`, every other root module keeps working, and you can adopt it again when you next need to change it:

```bash
tofu import aws_s3_bucket.state <bucket-name>
```

## 2. Locking

State locking is S3-native from OpenTofu 1.10 and Terraform 1.10 onward. Each root module sets `use_lockfile = true` and the lock becomes a `.tflock` object beside the state file. There is no DynamoDB table to provision, pay for, or forget to grant access to.

## 3. What you get

| Resource | What it gives you |
|---|---|
| `aws_s3_bucket.state` | Holds every root module's state, keyed by path |
| Versioning | The rollback path when a state push corrupts or truncates |
| SSE (AES256) plus bucket keys | State holds every non-sensitive resource attribute in cleartext |
| Public access block | State must never be world-readable |
| Lifecycle rule | Expires superseded state versions; sweeps abandoned multipart uploads |

## 4. Edgecases

### "`tofu apply` says the bucket already exists"

Bucket names are global across all AWS accounts. Either it is yours from a previous run, in which case import it, or someone else holds the name and you need a different `name_prefix`.

### "`tofu destroy` refuses to run"

`prevent_destroy` is set on the bucket. Deleting it orphans every resource this repo manages, so that has to be a deliberate console action, not a side effect.

## 5. Limits and numbers

| Thing | Value |
|---|---|
| Bucket name | `{name_prefix}-tfstate-{account_id}-{region}` |
| State | local, gitignored |
| Superseded version retention | 90 days |
| Multipart upload abort | 7 days |
| DynamoDB table | none |
