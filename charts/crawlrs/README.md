# crawlrs Helm chart

Deploys the crawlrs binary as a `StatefulSet` against externally-provided Valkey (or Redis) with Bloom module support, Postgres, and (for `kind=s3` store backends) an S3-compatible object store.

A one-shot `post-install` Job loads seed URLs into the Frontier on first install. Subsequent `helm upgrade` calls do not re-seed.

By default, the chart also deploys a self-contained observability stack (VictoriaMetrics + Grafana with four provisioned dashboards). If you already run your own Prometheus or Grafana, disable the bundled stack with `--set o11y.enabled=false` and point your scraper at the crawler's `/metrics` endpoint.

## TL;DR

```bash
helm install my-crawlrs ./charts/crawlrs \
  --namespace crawlrs --create-namespace \
  --set redis.url=redis://my-redis-master:6379 \
  --set postgres.url=postgres://crawlrs:secret@my-pg:5432/crawlrs \
  --set store.backend.kind=s3 \
  --set store.backend.s3.bucket=my-crawlrs-data \
  --set store.backend.s3.region=us-east-1 \
  --set secrets.existingSecret=my-crawlrs-creds \
  --set-file seeds.content=./seeds.txt

kubectl get pods -n crawlrs -l app.kubernetes.io/instance=my-crawlrs -w
helm test my-crawlrs -n crawlrs
```

## Valkey / Redis requirements

The frontier uses a Bloom filter for submit-time URL dedup (`BF.RESERVE` / `BF.ADD`). Any of these will work:

- **Valkey Bundle** (`valkey/valkey-bundle`) - bundles `valkey-bloom` alongside JSON and Search modules. Recommended.
- **Redis Stack** (`redis/redis-stack-server`) - bundles RedisBloom.
- **Stock Valkey or Redis** with the bloom module loaded manually.

Two configuration requirements:

- **`maxmemory-policy noeviction`.** The frontier writes URLs and bloom state durably. Under memory pressure you want loud OOM errors (which the runtime handles gracefully), not silent LRU eviction that drops URLs the system thinks are queued.
- **RDB snapshots, no AOF.** Bloom state survives RDB snapshots. AOF replay's per-op overhead isn't worth it for this workload. The default save thresholds (`save 60 10000 / save 300 10 / save 900 1`) give a progressively-sparser snapshot cadence that matches the frontier's write pattern.

**Managed services:** AWS ElastiCache and MemoryDB default to Valkey and support Bloom commands. Google Memorystore for Redis does not support Bloom modules at the time of writing. Verify before deploying.

## What gets deployed

### Crawler core (always)

| Resource | Name | What it does |
|---|---|---|
| StatefulSet | `<release>-crawlrs` | Worker pods with stable per-pod ordinals for shard ownership |
| Service (headless) | `<release>-crawlrs-headless` | Per-pod DNS so the metrics scraper can target individual pods |
| ServiceAccount | `<release>-crawlrs` | Created when `serviceAccount.create=true`; annotate with an IRSA role ARN for S3 access |
| ConfigMap | `<release>-crawlrs-config` | `crawl.toml` rendered from chart values |
| Secret | `<release>-crawlrs-secret` | Connection URLs for Valkey, Postgres, and (optionally) S3 credentials |
| Job | `<release>-crawlrs-seed` | Loads seed URLs into the Frontier on first install, then exits |
| PodDisruptionBudget | `<release>-crawlrs` | `maxUnavailable: 1` so rolling upgrades don't kill all workers at once |

### Observability stack (on by default)

Disabled with `--set o11y.enabled=false`. Includes VictoriaMetrics (single-node scrape + storage + PromQL) and Grafana (anonymous viewer access, four dashboards pre-provisioned).

## Required overrides

These are the values you must set for any real deployment. Everything else has sane defaults.

| Value | Why you need it |
|---|---|
| `image.tag` | Pin to your registry's tag so upgrades are explicit, not implicit |
| `redis.url` + `secrets.values.redisUrl` | The ConfigMap URL is a placeholder; the Secret is the actual source of truth at runtime |
| `postgres.url` + `secrets.values.postgresUrl` | Same pattern as Valkey |
| `store.backend.kind` | `local` for sandbox, `s3` for production |
| `replicaCount` | Defaults to 1; scale up based on how many shards each pod should own |
| `seeds.content` | URL list for the seed Job. Skip to leave the Frontier empty and bootstrap externally |

## Runtime knobs

These map to fields in the rendered `crawl.toml`. Defaults work for most deploys; tune when you have a specific reason.

| Value | What it controls |
|---|---|
| `politeness.enabled` | Master switch. `false` disables robots.txt and per-host pacing (use only against infrastructure you own). Blocklist and crawl scope stay active either way. |
| `politeness.perDomain.<host>` | Per-host overrides for `hostDelay`, `obeyRobotsTxt`, `robotsTtl`. Takes precedence over global defaults when you need to be gentler (or more aggressive) with a specific site. |
| `crawl.maxDepth` / `crawl.maxUrls` | Per-host quotas. `null` (default) means unbounded. When set, checked atomically inside the frontier's submit script so URLs over the cap are rejected without consuming bloom space. |
| `crawl.perDomain.<host>.maxUrls` / `.maxDepth` | Per-host quota overrides. Raise or lower an individual host's cap independently of the global default. |
| `access.blocklist` | Hosts the crawler refuses to visit. Checked before robots, rate limiting, or backoff. Exact host strings, no eTLD+1 rollup. |
| `runtime.linkDispatch` | How outbound URLs reach the Frontier. `direct` (default): the worker enqueues them itself after the metadata commit, accepting bounded loss on transient errors. `durable_outbox`: URLs commit into a Postgres outbox atomically with the metadata write and a publisher drains them at-least-once. |

## Reseeding

The seed Job runs on `post-install` only. To reload seeds after the initial deploy:

```bash
# Option A: reinstall (re-fires the post-install hook)
helm install --replace my-crawlrs ./charts/crawlrs ...

# Option B: re-run the existing Job spec under a new name
kubectl create job --from=job/<release>-crawlrs-seed \
  reseed-$(date +%s) -n crawlrs
```

## Sharding

With `replicaCount=N` and `sharding.numShards=S` (default 8), each pod owns shards `(ordinal, ordinal+N, ordinal+2N, ...)` mod S. The chart sets `CRAWLRS_REPLICAS` automatically so each pod knows how many peers exist and claims a disjoint subset.

| Setup | Shard ownership |
|---|---|
| 1 pod, 8 shards | Pod 0 owns all 8 |
| 4 pods, 8 shards | Pod 0: {0,4}, Pod 1: {1,5}, Pod 2: {2,6}, Pod 3: {3,7} |
| 8 pods, 8 shards | Each pod owns exactly one shard |

## Secrets

Two modes, depending on how you manage credentials:

- **Chart-rendered** (`secrets.create: true`, default): the chart writes a Secret from `secrets.values`. Good for getting started; pass sensitive fields via `--set-file` rather than committing them.
- **Externally managed** (`secrets.existingSecret: <name>`): the chart references a Secret you create yourself. It must expose keys `redisUrl`, `postgresUrl`, and (for S3 backends) `s3AccessKeyId`, `s3SecretAccessKey`.

## Probes

Three Kubernetes probes check the binary's HTTP host on port 9090:

- **Startup** (`/livez`): allows up to 150s for first-boot Postgres migrations before declaring failure.
- **Liveness** (`/livez`): restarts the pod after 30s of consecutive failures.
- **Readiness** (`/readyz`): removes the pod from service after 30s of failures. On SIGTERM, readiness flips to 503 immediately and the pod drains for 5s before shutting down workers.

Two init containers (`wait-for-redis`, `wait-for-postgres`) block the crawler container until both backends are reachable. Without them, fresh installs crashloop a few times while the dependency pods come up.

## Security context

Defaults follow the [Kubernetes restricted PodSecurity profile](https://kubernetes.io/docs/concepts/security/pod-security-standards/#restricted):

- Runs as non-root (uid/gid 65532, the `nonroot` user in distroless images)
- Read-only root filesystem (writable `/tmp` via `emptyDir`)
- All capabilities dropped, no privilege escalation

Override `podSecurityContext` / `containerSecurityContext` if your base image needs different UIDs.

## S3 lifecycle rule (production)

When using `store.backend.kind=s3`, configure an `AbortIncompleteMultipartUpload` lifecycle rule on the bucket. Without it, multipart uploads abandoned by a crashed worker (e.g. OOM mid-rotation of a Parquet file) accumulate as billable storage that the application has no way to clean up.

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket "$BUCKET" \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "abort-incomplete-multipart",
      "Status": "Enabled",
      "Filter": {"Prefix": ""},
      "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}
    }]
  }'
```

The 1-day window is generous: completed uploads are atomic, so nothing in-flight will be affected. Anything incomplete for a day is by definition orphaned. Equivalent settings exist on MinIO, GCS (XML API), and Cloudflare R2.

## Local storage (sandbox)

When `store.backend.kind=local`, blobs land at `store.backend.path` inside the crawler pod. Two modes:

- **`persistence.enabled=false`** (default): `emptyDir` volume. Blobs are lost on pod restart. Fine for validating the pipeline end-to-end.
- **`persistence.enabled=true`**: PVC via `volumeClaimTemplates`. Blobs survive restarts. Tune `persistence.size` and `persistence.storageClassName` as needed.

Production deploys typically use `kind=s3` instead.

## Observability

Four Grafana dashboards ship as JSON in `dashboards/`, loaded via ConfigMap and provisioned on startup:

| Dashboard | What it shows |
|---|---|
| **Crawler Health** | Crawl rate, success rate, active workers, pipeline latency, error attribution, per-phase percentiles. Organized into collapsible sections: Overview, Pipeline Latency, Fetch & Parse, Discovery & Scheduling, Storage & Pools. |
| **Worker Health** | Per-worker throughput, latency, restart count, skip rate. Uses the `worker` label to show all 32 workers individually. |
| **Container Resources** | Per-pod CPU, memory, network, file descriptors, allocator behavior. |
| **Redis Health** | Memory vs `maxmemory` cap, eviction rates, commands/sec, hit rate, per-key-group memory breakdown, fragmentation ratio. |

To add a dashboard, drop a JSON file in `dashboards/` and run `helm upgrade`. The ConfigMap regenerates from the glob.

To bring your own scraper instead of the bundled stack:

```bash
# Disable bundled o11y entirely
helm install ... --set o11y.enabled=false

# Or keep VictoriaMetrics but drop Grafana
helm install ... --set o11y.grafana.enabled=false
```

Then point your scraper at the headless service:

```
<release>-crawlrs-headless.<namespace>.svc.cluster.local:9090/metrics
```

## Verifying a deploy

```bash
# All pods Ready?
kubectl get pods -n crawlrs -l app.kubernetes.io/instance=my-crawlrs

# /metrics endpoint reachable?
kubectl exec -n crawlrs my-crawlrs-crawlrs-0 -- \
  wget -qO- http://localhost:9090/metrics | head -20

# Migrations applied?
kubectl logs -n crawlrs my-crawlrs-crawlrs-0 | grep -i migrat

# Seed Job ran?
kubectl logs -n crawlrs job/my-crawlrs-crawlrs-seed

# Helm test hook
helm test my-crawlrs -n crawlrs
```

## Values reference

See [`values.yaml`](./values.yaml). Every key is documented inline.
