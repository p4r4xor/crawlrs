# crawlrs Helm chart (Phase 6b)

Deploys the crawlrs binary as a `StatefulSet` against externally-
provided Redis, Postgres, and (for `kind=s3` store backends) an
S3-compatible object store.

Phase 6b ships the crawler-only chart. Phase 6c bolts vmsingle +
Grafana onto this chart for the self-contained observability stack
(per [ADR-0014](../../docs/decisions/0014-observability-stack.md)).

## TL;DR

```bash
# From the repo root
helm install my-crawlrs ./charts/crawlrs \
  --namespace crawlrs --create-namespace \
  --set redis.url=redis://my-redis-master:6379 \
  --set postgres.url=postgres://crawlrs:secret@my-pg:5432/crawlrs \
  --set secrets.values.redisUrl=redis://my-redis-master:6379 \
  --set secrets.values.postgresUrl=postgres://crawlrs:secret@my-pg:5432/crawlrs

# Watch readiness
kubectl get pods -n crawlrs -l app.kubernetes.io/instance=my-crawlrs -w

# Smoke-test readiness via a helm hook job
helm test my-crawlrs -n crawlrs
```

## What's deployed

| Resource | Name | Purpose |
|---|---|---|
| `StatefulSet` | `<release>-crawlrs` | Worker pods with stable per-pod ordinals |
| `Service` (headless) | `<release>-crawlrs-headless` | Per-pod DNS for vmsingle scrape (Phase 6c) |
| `ServiceAccount` | `<release>-crawlrs` | (created when `serviceAccount.create=true`) |
| `ConfigMap` | `<release>-crawlrs-config` | `crawl.toml` rendered from values |
| `Secret` | `<release>-crawlrs-secret` | Redis/Postgres URLs + (optional) S3 creds |
| `PodDisruptionBudget` | `<release>-crawlrs` | `maxUnavailable: 1` |

## Required overrides for any non-trivial deploy

| Value | Purpose |
|---|---|
| `image.tag` | Pin to your registry's tag (default: `Chart.appVersion`) |
| `redis.url` + `secrets.values.redisUrl` | Both: the ConfigMap-rendered URL is a placeholder, the Secret is the source of truth |
| `postgres.url` + `secrets.values.postgresUrl` | Same pattern as Redis |
| `store.backend.kind` | `local` (sandbox) or `s3` (production) |
| `replicaCount` | Defaults to 1; scale per shard ownership math |

## Sharding math

With `replicaCount=N` and `sharding.numShards=S` (default 8), each
pod ordinal owns shards `(ordinal, ordinal+N, ordinal+2N, ...)` mod
S. The `CRAWLRS_REPLICAS` env var (set automatically by the chart)
tells the binary how many peers exist so each owns a disjoint subset.

Examples:
- `replicaCount=1`, `numShards=8`: pod 0 owns all 8 shards.
- `replicaCount=4`, `numShards=8`: pod 0 owns {0,4}, pod 1 owns {1,5}, pod 2 owns {2,6}, pod 3 owns {3,7}.
- `replicaCount=8`, `numShards=8`: each pod owns exactly one shard.

## Secret modes

- **Chart-rendered** (`secrets.create: true`, default):
  the chart writes a `Secret` from `secrets.values`. Quick-start
  shape; not for production. Set values via `--set-file` for sensitive
  fields.
- **Externally managed** (`secrets.existingSecret: <name>`):
  the chart references a Secret you provide. Must expose keys
  `redisUrl`, `postgresUrl`, and (for `s3` backend) `s3AccessKeyId`,
  `s3SecretAccessKey`.

## Probes

Three Kubernetes probes wired to the binary's HTTP host on port 9090:

| Probe | Endpoint | Default thresholds |
|---|---|---|
| Startup | `/livez` | initialDelay 0s, period 5s, failureThreshold 30 (= 150s for slow Postgres migrations on first install) |
| Liveness | `/livez` | initialDelay 30s, period 10s, failureThreshold 3 (= 30s of failures restart the pod) |
| Readiness | `/readyz` | initialDelay 5s, period 10s, failureThreshold 3 (= 30s of failures take pod out of service) |

Per ADR-0014: SIGTERM flips `/readyz` to 503 immediately, then drains
for 5s before signaling worker-pool shutdown. Readiness probe
respects this; load balancers stop sending traffic during drain.

## Security context

Defaults follow the [Kubernetes restricted PodSecurity
profile](https://kubernetes.io/docs/concepts/security/pod-security-standards/#restricted):

- `runAsNonRoot: true`
- `runAsUser: 65532`, `runAsGroup: 65532` (the `nonroot` user in
  distroless / Chainguard images)
- `readOnlyRootFilesystem: true` (writable `/tmp` provided as
  `emptyDir`)
- `capabilities.drop: [ALL]`
- `allowPrivilegeEscalation: false`

Override `podSecurityContext` / `containerSecurityContext` if your
base image needs different UIDs.

## Persistence (local store backend)

When `store.backend.kind=local` the chart provisions an `emptyDir`
volume at `store.backend.path`. Blobs written there are lost on pod
restart. Acceptable for sandboxes; production should use
`store.backend.kind=s3`.

A future revision (likely Phase 6c.x) will add a
`volumeClaimTemplates` mode for persistent local storage if a real
use case emerges.

## Verifying a deploy

```bash
# All pods Ready?
kubectl get pods -n crawlrs -l app.kubernetes.io/instance=my-crawlrs

# /metrics endpoint reachable?
kubectl exec -n crawlrs my-crawlrs-crawlrs-0 -- \
  wget -qO- http://localhost:9090/metrics | head -20

# Schema migrations applied?
kubectl logs -n crawlrs my-crawlrs-crawlrs-0 | grep -i migrat

# helm-test hook
helm test my-crawlrs -n crawlrs
```

## Values reference

See [`values.yaml`](./values.yaml). Every key is documented inline.
