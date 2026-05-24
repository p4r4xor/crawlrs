# crawlrs Helm chart

Deploys the crawlrs binary as a `StatefulSet` against externally-provided Valkey (or Redis) with Bloom module support, Postgres, and (for `kind=s3` store backends) an S3-compatible object store.

A one-shot `post-install` Job loads the seed URLs into the Frontier on first install; subsequent `helm upgrade` calls do not re-seed.

By default also deploys a self-contained observability stack (`vmsingle` + Grafana with three provisioned dashboards). Operators with their own Prometheus / VM / Grafana fleet can disable the bundled instances via `--set o11y.enabled=false`.

## TL;DR

```bash
# From the repo root
helm install my-crawlrs ./charts/crawlrs \
  --namespace crawlrs --create-namespace \
  --set redis.url=redis://my-redis-master:6379 \
  --set postgres.url=postgres://crawlrs:secret@my-pg:5432/crawlrs \
  --set store.backend.kind=s3 \
  --set store.backend.s3.bucket=my-crawlrs-data \
  --set store.backend.s3.region=us-east-1 \
  --set secrets.existingSecret=my-crawlrs-creds \
  --set-file seeds.content=./seeds.txt

# Watch readiness
kubectl get pods -n crawlrs -l app.kubernetes.io/instance=my-crawlrs -w

# Smoke-test readiness via the bundled helm hook
helm test my-crawlrs -n crawlrs
```

## Redis requirements

The frontier requires a **Bloom filter module** for submit-time dedup (`BF.RESERVE` / `BF.ADD` / `BF.EXISTS`). Supported deployment shapes: **Valkey Bundle** (`valkey/valkey-bundle`, bundles `valkey-bloom` alongside JSON and Search modules), **Redis Stack** (`redis/redis-stack-server`, bundles RedisBloom), or stock Redis/Valkey with the bloom module loaded manually.

Configure Redis with `maxmemory-policy noeviction`. The frontier writes URLs and bloom state durably; under memory pressure we want loud OOM errors (which the runtime warns on and continues past), not silent LRU eviction that drops URLs the system thinks are queued.

Durability: RDB snapshots only. The default save thresholds (`save 60 10000 / save 300 10 / save 900 1`) give a progressively-sparser snapshot cadence that suits the frontier's write pattern. AOF is intentionally disabled; bloom-filter state survives RDB snapshots and AOF replay's per-op overhead isn't worth it for this workload.

Managed services: AWS ElastiCache and MemoryDB now default to Valkey and support Bloom filter commands. Google Memorystore for Redis does not support Bloom modules at the time of writing. Verify before deploying.

## What's deployed

### Crawler core (always)

| Resource | Name | Purpose |
|---|---|---|
| `StatefulSet` | `<release>-crawlrs` | Worker pods with stable per-pod ordinals |
| `Service` (headless) | `<release>-crawlrs-headless` | Per-pod DNS for vmsingle scrape |
| `ServiceAccount` | `<release>-crawlrs` | (created when `serviceAccount.create=true`) |
| `ConfigMap` | `<release>-crawlrs-config` | `crawl.toml` + `seeds.txt` rendered from values |
| `Secret` | `<release>-crawlrs-secret` | Redis/Postgres URLs + (optional) S3 creds |
| `Job` | `<release>-crawlrs-seed` | One-shot seed loader; `helm.sh/hook: post-install`, runs once and exits |
| `PodDisruptionBudget` | `<release>-crawlrs` | `maxUnavailable: 1` |

### Observability stack (`o11y.enabled=true`, default)

| Resource | Name | Purpose |
|---|---|---|
| `Deployment` | `<release>-crawlrs-vmsingle` | Single-node VictoriaMetrics; scrape + storage + PromQL |
| `Service` | `<release>-crawlrs-vmsingle` | ClusterIP on 8429 |
| `PVC` | `<release>-crawlrs-vmsingle` | 10 GB default, time-series storage |
| `ConfigMap` | `<release>-crawlrs-vmsingle-scrape` | Static-targets scrape config |
| `Deployment` | `<release>-crawlrs-grafana` | Grafana with anonymous viewer access |
| `Service` | `<release>-crawlrs-grafana` | ClusterIP on 3000 |
| `ConfigMap` | `<release>-crawlrs-grafana-datasources` | vmsingle as default Prometheus datasource |
| `ConfigMap` | `<release>-crawlrs-grafana-dashboards-provider` | File-based dashboard provider config |
| `ConfigMap` | `<release>-crawlrs-grafana-dashboards` | Three dashboard JSONs from `dashboards/` |

## Required overrides for any non-trivial deploy

| Value | Purpose |
|---|---|
| `image.tag` | Pin to your registry's tag (default: `Chart.appVersion`) |
| `redis.url` + `secrets.values.redisUrl` | Both: the ConfigMap-rendered URL is a placeholder, the Secret is the source of truth |
| `postgres.url` + `secrets.values.postgresUrl` | Same pattern as Redis |
| `store.backend.kind` | `local` (sandbox) or `s3` (production) |
| `replicaCount` | Defaults to 1; scale per shard ownership math |
| `seeds.content` | URL list for the post-install seed Job. Skip to leave the Frontier empty and bootstrap externally |

## Runtime knobs worth knowing

These map straight to fields in the rendered `crawl.toml`. Defaults are sane for most deploys; tune when you have a reason.

| Value | What it does |
|---|---|
| `politeness.enabled` | Master switch. `false` swaps in no-op politeness collaborators (no robots, no per-host pacing). Only use against infrastructure you own or have explicit permission for. `[crawl]` scope and `[access]` blocklist stay active either way. |
| `politeness.perDomain.<host>` | Per-host overrides for `hostDelay`, `obeyRobotsTxt`, `robotsTtl`. Take precedence over global defaults. |
| `crawl.maxDepth` / `crawl.maxUrls` | Per-host quotas. `null` (default) is unbounded. When set, checked atomically inside the frontier's submit script; URLs over the cap are rejected without consuming bloom space. |
| `crawl.perDomain.<host>.maxUrls` / `.maxDepth` | Per-host quota overrides. Raise or lower an individual host's cap. |
| `access.blocklist` | Hosts the crawler refuses to visit. Checked first, before robots / rate / backoff. Exact host strings, no eTLD+1 rollup. |
| `runtime.linkDispatch` | `direct` (default): worker calls `Frontier::submit_batch` itself after the metadata commit; bounded loss under transient errors. `durable_outbox`: outbound URLs commit atomically into a Postgres outbox; a publisher drains them at-least-once. Trade durability for ~50x lower Postgres write rate vs `durable_outbox`. |

## Reseeding after the initial install

The seed Job has annotation `helm.sh/hook: post-install`. `helm upgrade` does not re-fire it. To reload seeds:

```bash
# Option A: reinstall the chart, which re-runs the post-install hook
helm install --replace my-crawlrs ./charts/crawlrs ...

# Option B: re-run the existing Job spec under a new name
kubectl create job --from=job/<release>-crawlrs-seed \
  reseed-$(date +%s) -n crawlrs
```

## Sharding math

With `replicaCount=N` and `sharding.numShards=S` (default 8), each pod ordinal owns shards `(ordinal, ordinal+N, ordinal+2N, ...)` mod S. The `CRAWLRS_REPLICAS` env var (set automatically by the chart) tells the binary how many peers exist so each owns a disjoint subset.

Examples:
- `replicaCount=1`, `numShards=8`: pod 0 owns all 8 shards.
- `replicaCount=4`, `numShards=8`: pod 0 owns {0,4}, pod 1 owns {1,5}, pod 2 owns {2,6}, pod 3 owns {3,7}.
- `replicaCount=8`, `numShards=8`: each pod owns exactly one shard.

## Secret modes

- **Chart-rendered** (`secrets.create: true`, default): the chart writes a `Secret` from `secrets.values`. Quick-start shape; not for production. Set values via `--set-file` for sensitive fields.
- **Externally managed** (`secrets.existingSecret: <name>`): the chart references a Secret you provide. Must expose keys `redisUrl`, `postgresUrl`, and (for `s3` backend) `s3AccessKeyId`, `s3SecretAccessKey`.

## Probes

Three Kubernetes probes wired to the binary's HTTP host on port 9090.

| Probe | Endpoint | Default thresholds |
|---|---|---|
| Startup | `/livez` | initialDelay 0s, period 5s, failureThreshold 30 (= 150s for slow Postgres migrations on first install) |
| Liveness | `/livez` | initialDelay 30s, period 10s, failureThreshold 3 (= 30s of failures restart the pod) |
| Readiness | `/readyz` | initialDelay 5s, period 10s, failureThreshold 3 (= 30s of failures take pod out of service) |

Two init containers (`wait-for-redis`, `wait-for-postgres`) gate the crawler container's startup on backend reachability. Without them, fresh `helm install` crashloops the crawler a few times while the dependency StatefulSets come up.

Shutdown protocol: SIGTERM flips `/readyz` to 503 immediately, then drains for 5s before signaling worker-pool shutdown. Readiness probe respects this; load balancers stop sending traffic during drain.

## Security context

Defaults follow the [Kubernetes restricted PodSecurity profile](https://kubernetes.io/docs/concepts/security/pod-security-standards/#restricted):

- `runAsNonRoot: true`
- `runAsUser: 65532`, `runAsGroup: 65532` (the `nonroot` user in distroless / Chainguard images)
- `readOnlyRootFilesystem: true` (writable `/tmp` provided as `emptyDir`)
- `capabilities.drop: [ALL]`
- `allowPrivilegeEscalation: false`

Override `podSecurityContext` / `containerSecurityContext` if your base image needs different UIDs.

## Required S3 bucket lifecycle rule (production)

When `store.backend.kind=s3`, the bucket MUST have an `AbortIncompleteMultipartUpload` lifecycle rule configured. Without it, multipart uploads abandoned by a crashed worker (e.g. a pod OOM mid-rotation of a 128 MB Parquet file) accumulate as billable storage parts that the application has no hook to clean up.

The rule is one-time bucket configuration, set out-of-band before the first crawl runs. AWS CLI form:

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

Equivalent settings exist on every S3-compatible store (MinIO, GCS via the XML API, Cloudflare R2, etc.); consult your provider's docs.

The 1-day window is generous: completed multipart uploads are atomic, so no in-flight upload that's still progressing will be aborted. Anything sitting incomplete for a day is by definition orphaned.

## Persistence (local store backend)

When `store.backend.kind=local`, two modes:

- `store.backend.persistence.enabled=false` (default): the chart provisions an `emptyDir` volume at `store.backend.path`. Blobs written there are lost on pod restart. Acceptable for sandboxes validating the pipeline end-to-end.
- `store.backend.persistence.enabled=true`: the chart provisions a PVC via `volumeClaimTemplates`. Blobs survive pod restarts. Use when you want to keep what you crawl across iterations. Tune `persistence.size`, `persistence.storageClassName`, and `persistence.accessMode` as needed.

Production deploys typically use `store.backend.kind=s3` with a real S3-compatible store instead.

## Observability stack

Three Grafana dashboards ship as JSON in `charts/crawlrs/dashboards/`, loaded into a ConfigMap via `Files.Glob` and provisioned by Grafana on startup.

| Dashboard | File | What it shows |
|---|---|---|
| **Crawler Health Overview** | `crawler-health.json` | Crawl rate, total URLs fetched, fetch success rate, active workers, end-to-end pipeline latency p50/p95/p99, error attribution, parse / store-write / metadata-query percentiles |
| **Container Resources** | `container-resources.json` | Per-pod CPU, memory (working set + RSS), network rx/tx, file descriptors, GC / allocator behaviour |
| **Redis Health** | `redis-health.json` | Memory used vs `maxmemory` cap, eviction + expiration rates, commands per second, hit rate, connected clients, per-key-group memory (host_queue, wake, urls, inflight, seen, host_count), per-key memory for bounded-cardinality keys, fragmentation ratio |

To **add a dashboard**, drop a JSON file in `dashboards/` and re-run `helm upgrade`. The ConfigMap regenerates from the glob.

To **disable the bundled o11y stack** (operators with their own Prometheus / VM / Grafana):

```bash
helm install ... --set o11y.enabled=false
```

Then point your existing scraper at:

```
<release>-crawlrs-headless.<namespace>.svc.cluster.local:9090/metrics
```

Per-pod static targets (for static_configs in your own scrape file):

```
<release>-crawlrs-0.<release>-crawlrs-headless.<namespace>.svc.cluster.local:9090
<release>-crawlrs-1.<release>-crawlrs-headless.<namespace>.svc.cluster.local:9090
...
```

To **only disable Grafana** (keep vmsingle for metrics-only deploys):

```bash
helm install ... --set o11y.grafana.enabled=false
```

vmsingle ships with a built-in PromQL UI at `/vmui` (port 8429) as a fallback when Grafana isn't available.

## Verifying a deploy

```bash
# All pods Ready?
kubectl get pods -n crawlrs -l app.kubernetes.io/instance=my-crawlrs

# /metrics endpoint reachable?
kubectl exec -n crawlrs my-crawlrs-crawlrs-0 -- \
  wget -qO- http://localhost:9090/metrics | head -20

# Schema migrations applied?
kubectl logs -n crawlrs my-crawlrs-crawlrs-0 | grep -i migrat

# Seed Job ran cleanly?
kubectl logs -n crawlrs job/my-crawlrs-crawlrs-seed

# helm-test hook
helm test my-crawlrs -n crawlrs
```

## Values reference

See [`values.yaml`](./values.yaml). Every key is documented inline.
