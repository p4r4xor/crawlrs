# Postgres

Provisions the RDS instance holding the crawler's metadata ledger, plus its security group.

## 1. What this holds

The per-URL ledger: current state per URL, an append-only history log, and the outbox table when the runtime is configured for durable link dispatch.

The history side is append-only and grows for the length of a run, which is why `max_allocated_storage` sits above `allocated_storage` by default. The metadata commit is on the worker's critical path, so a full disk does not degrade throughput; it stalls every worker at once.

## 2. The password

RDS generates it, stores it in Secrets Manager, and rotates it. Terraform never sees the value, so it cannot reach state, a plan output, or your shell history.

That is why this module has no `password` variable. Read the credential through the `master_user_secret_arn` output, either with External Secrets Operator or once by hand into the chart's Secret.

## 3. Subnet group

This module does not create one. It takes `db_subnet_group_name` and uses the group the network module already created over the database subnets.

## 4. What you get by default

| Setting | Default | What it costs you to change |
|---|---|---|
| `deletion_protection` | on | Off means an apply can delete the ledger. Losing it loses cross-run dedup, so every URL already crawled gets fetched again |
| `skip_final_snapshot` | off | On means a delete leaves nothing to restore from |
| `backup_retention_period` | 7 days | 0 disables automated backups |
| `multi_az` | off | On doubles instance cost and is the only thing that survives a zone failure without a restore |
| `storage_encrypted` | on | Not configurable |

A throwaway environment turns the first three off in its `terraform.tfvars`, which puts the tradeoff where you can see it instead of in a default.

## 5. Network access

Ingress is granted to security group IDs, never CIDRs. Pass the EKS node security group in `allowed_security_group_ids`.

## 6. Edgecases

### "`tofu plan` wants to create a second DB subnet group"

`db_subnet_group_name` is unset or names a group that does not exist, so the module falls back to creating one. Pass the network layer's `database_subnet_group_name`.

### "`tofu destroy` refuses to delete the instance"

`deletion_protection` is on. That is the point. Turn it off in a separate apply first, deliberately.

### "Workers stalled and Postgres reports no space"

Storage autoscaling hit `max_allocated_storage`, or it was set equal to `allocated_storage` and never engaged. The history table grows for the length of a run.

### "I want to move to Postgres 18"

Change `engine_version`, `major_engine_version`, and `parameter_group_family` together. Test the metadata crate's schema and queries against the new major first; a major version upgrade is not reversible in place.

## 7. Limits and numbers

| Thing | Value |
|---|---|
| Default engine version | 17.10 |
| Default instance class | `db.t4g.medium` |
| Storage | 20 GB initial, autoscaling to 100 GB |
| Port | 5432 |
| Backups | 7 days |
| Log exports | `postgresql`, `upgrade` |
| Password variable | none, RDS-managed |
