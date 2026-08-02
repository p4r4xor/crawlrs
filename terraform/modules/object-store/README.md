# Object store

Provisions the S3 bucket the crawler writes output to, its lifecycle rules, and the IRSA role the pods assume to reach it.

## 1. What lands here

A Parquet file per batch of parsed pages, and a WARC record per fetch. Output is append-only and grows for the length of a run.

Nothing in the crawler deletes any of it. The lifecycle rules below are the only bound on what the bucket costs.

## 2. Lifecycle

| Variable | Default | Effect |
|---|---|---|
| `transition_to_ia_days` | 30 | Move to STANDARD_IA |
| `transition_to_glacier_days` | 90 | Move to GLACIER_IR, which keeps millisecond retrieval |
| `expiration_days` | 0 | Delete outright. 0 keeps output forever |
| `noncurrent_version_expiration_days` | 30 | Drop superseded versions after an overwrite |

Set any of them to 0 to disable that step.

**Check your object sizes before trusting the IA transition:** objects under 128 KB are billed at the 128 KB minimum in STANDARD_IA. A run that produces many small Parquet files costs more tiered down than left in Standard.

The rule also aborts incomplete multipart uploads after 7 days. A worker killed mid-upload leaves parts that are billed as storage and do not appear in an object listing, so without this they accumulate where you will not see them.

## 3. IRSA

The pod presents the ServiceAccount token it already has, the cluster's OIDC provider vouches for it, and the pod gets short-lived credentials for this role. No access key is created, stored, or rotated.

`namespace_service_accounts` must match the namespace the chart installs into and the ServiceAccount it creates. The default is `crawlrs:crawlrs`. A mismatch does not fail the apply; it fails at runtime with `AccessDenied` on the first write.

Annotate the ServiceAccount with the `irsa_role_arn` output as `eks.amazonaws.com/role-arn`.

## 4. Policy shape

Object actions and bucket actions are separate statements. `ListBucket` applies to the bucket ARN, `PutObject` applies to the object paths beneath it. Granting both against one resource ARN gives you a policy that is either broken or wider than you meant.

## 5. Edgecases

### "The crawler gets AccessDenied on its first write"

`namespace_service_accounts` does not match where the chart actually runs, or the ServiceAccount is missing the `eks.amazonaws.com/role-arn` annotation. Both are runtime-only failures; the apply looked clean.

### "`tofu destroy` hangs on the bucket"

`force_destroy` is off and the bucket holds output. Deliberate outside throwaway environments. Set it true for dev, or empty the bucket first.

### "Storage cost is higher after enabling the IA transition"

Small objects. See the note in section 2.

### "Versioning is on and the bucket keeps growing after overwrites"

That is `noncurrent_version_expiration_days` doing its 30-day hold. Lower it if you do not need the rollback window.

## 6. Limits and numbers

| Thing | Value |
|---|---|
| Encryption | AES256 with bucket keys, not configurable |
| Public access | fully blocked, not configurable |
| Versioning | on, not configurable |
| IA transition | 30 days |
| Glacier IR transition | 90 days |
| Expiration | off by default |
| Multipart upload abort | 7 days |
| Default ServiceAccount | `crawlrs:crawlrs` |
