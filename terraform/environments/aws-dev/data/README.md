# aws-dev / data

Root module for the dev backing stores. State key `aws-dev/data/terraform.tfstate`.

Apply the platform layer first; this one reads its outputs.

```bash
export TF_VAR_state_bucket=$(cd ../../../bootstrap && tofu output -raw state_bucket)
tofu init -backend-config=../../../backend.hcl
tofu plan
tofu apply
```

## 1. What it composes

**`modules/valkey`:** frontier queue, politeness wake-times, submit-time Bloom dedup.

**`modules/postgres`:** metadata ledger and outbox table.

**`modules/object-store`:** Parquet and WARC output, plus the IRSA role the pods assume.

## 2. Why `TF_VAR_state_bucket` exists

The `terraform_remote_state` data source needs the bucket name, and a backend block cannot take a variable. So the name arrives twice: through `backend.hcl` for this module's own state, and as a variable for reading the platform layer's.

Both must hold the same value. Exporting it from the bootstrap output keeps them in step; hardcoding it in `terraform.tfvars` is what lets them drift apart without anyone noticing.

## 3. Dev shape

`terraform.tfvars` overrides only what differs from the defaults, and every override there trades a safety property for cost or speed: one Valkey node with no failover, TLS off inside the VPC, no Postgres standby, one day of backups, deletion protection off, output expiring after 30 days.

The defaults in `variables.tf` are the production shape. A production `terraform.tfvars` is mostly sizing, not safety toggles.

## 4. Consuming the outputs

`valkey_url` already carries the scheme that matches the transit-encryption setting, so pass it straight through.

The Postgres password never enters state. Take `postgres_secret_arn` and resolve it through External Secrets Operator, or read it once into the chart's Secret. `postgres_url_template` gives you the URL shape with a `PASSWORD` placeholder.

## 5. Edgecases

### "The plan fails on the remote state data source"

`TF_VAR_state_bucket` is unset, names a different bucket than `backend.hcl`, or the platform layer has not been applied so there is no state object to read.

### "Destroy hangs on the S3 bucket"

`s3_force_destroy` is off and the bucket holds output. The dev `terraform.tfvars` sets it true; check it survived your edits.

### "The crawler cannot connect to Valkey"

The dev tfvars turns transit encryption off, so the URL is `redis://`, not `rediss://`. Use the `valkey_url` output rather than assembling it.

## 6. Limits and numbers

| Thing | Value |
|---|---|
| State key | `aws-dev/data/terraform.tfstate` |
| Required env var | `TF_VAR_state_bucket` |
| Valkey nodes | 1, no failover |
| Valkey TLS | off in dev |
| Postgres backups | 1 day in dev |
| Output retention | 30 days in dev |
| Destroy order | before the platform layer |
