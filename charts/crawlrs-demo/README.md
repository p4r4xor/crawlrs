# crawlrs-demo Helm chart

One-command sandbox install of the full crawlrs stack:

- `crawlrs` (the crawler StatefulSet, via the `charts/crawlrs/` subchart)
- `vmsingle` + Grafana (bundled observability, three provisioned dashboards)
- Redis (frontier + politeness backend, `redis:7.4.2-alpine`)
- Postgres (metadata ledger, `postgres:17.2-alpine`)

Backing services run as raw `StatefulSet` + `Service` manifests in
`templates/{redis,postgres}-*.yaml` using official upstream images
directly. No third-party charts in the supply chain.

Blob storage uses the **pod-local FS backend** (`store.backend.kind =
local`); blobs land at `/var/lib/crawlrs/data` inside the crawlrs pod.
By default this is an `emptyDir` volume — ephemeral, lost on pod
restart. Toggle `crawlrs.store.backend.persistence.enabled=true` to
back it with a PVC instead.

Useful for: trying crawlrs without standing up your own backing
services, dev-loop integration tests, and CI smoke tests.

**Not for production.** Sandbox shape: single replicas everywhere,
fixed credentials, sandbox-sized PVCs.

## TL;DR

```bash
helm dep build ./charts/crawlrs-demo
( cd ./charts/crawlrs-demo/charts && for f in *.tgz; do tar -xzf "$f"; done )

# Install (release name MUST be `crawlrs-demo` for the default
# Service-DNS values to resolve)
helm install crawlrs-demo ./charts/crawlrs-demo \
  --create-namespace -n crawlrs

# Wait for everything
kubectl wait --for=condition=ready pod \
  -l app.kubernetes.io/instance=crawlrs-demo \
  -n crawlrs --timeout=180s
```

## Bundled deployment topology

```
+------------------------------------------------+
|  Namespace: crawlrs                            |
|                                                |
|  crawlrs-demo-0  (StatefulSet, the crawler)    |
|         |                                      |
|         +---  redis://crawlrs-demo-redis:6379 ---+
|         +---  postgres://crawlrs-demo-postgres ---+
|         +---  file:///var/lib/crawlrs/data       |
|                                                |
|  crawlrs-demo-redis     (StatefulSet)          |
|  crawlrs-demo-postgres  (StatefulSet)          |
|                                                |
|  crawlrs-demo-crawlrs-vmsingle  (Deployment)   |
|         scrapes /metrics from crawlrs-demo-0   |
|  crawlrs-demo-crawlrs-grafana   (Deployment)   |
|         queries vmsingle, 3 provisioned dashboards |
+------------------------------------------------+
```

## Why the release name matters

Service names emitted by the demo chart's templates are of the form
`<release>-<role>` (e.g. `crawlrs-demo-redis`,
`crawlrs-demo-postgres`). The `crawlrs` subchart's URLs in
`values.yaml` reference those by literal string. If you change the
release name, those names won't resolve.

Either install with `crawlrs-demo` as the release name (the chart's
NOTES.txt will warn you if you don't), or override:

```bash
helm install my-name ./charts/crawlrs-demo \
  --set crawlrs.redis.url=redis://my-name-redis:6379 \
  --set crawlrs.postgres.url=postgres://crawlrs:crawlrs@my-name-postgres:5432/crawlrs \
  --set crawlrs.secrets.values.redisUrl=redis://my-name-redis:6379 \
  --set crawlrs.secrets.values.postgresUrl=postgres://crawlrs:crawlrs@my-name-postgres:5432/crawlrs
```

## Migrating to production

This chart is the wrong shape for production. When you outgrow it:

1. Stand up production-grade Redis (Sentinel or managed service),
   Postgres (managed RDS / Cloud SQL / your own HA cluster), and
   S3-compatible storage (real S3 / R2 / GCS) outside Kubernetes.
2. Switch to the bare `charts/crawlrs/` chart with `o11y.enabled=true`
   so you keep the bundled observability but drop the bundled deps:

   ```bash
   helm install my-crawlrs ./charts/crawlrs \
     --set redis.url=... \
     --set postgres.url=... \
     --set store.backend.kind=s3 \
     --set store.backend.s3.bucket=... \
     --set store.backend.s3.region=... \
     --set secrets.existingSecret=my-crawlrs-secrets \
     ...
   ```

The `crawlrs` subchart values you pass to `crawlrs-demo` map directly
to the `charts/crawlrs/` values; the only meaningful change between
sandbox and production is `store.backend.kind` (`local` vs `s3`) plus
the URLs of the backing services.

## Disclaimers (the short version)

| | crawlrs-demo | charts/crawlrs/ |
|---|---|---|
| Backing services | bundled, single-replica | external, your call |
| Blob storage | `kind = local` (emptyDir or PVC) | `kind = s3` against real S3 |
| Persistence | sandbox-sized PVCs (or none for blobs) | your DB + S3 retention |
| Credentials | hard-coded sandbox values | externally-managed Secret |
| HA / failover | none | yes |
| Resources | laptop-sized | configure to taste |
| Observability | bundled (vmsingle + Grafana) | bundled (vmsingle + Grafana) |

If your evaluation works, switch to `charts/crawlrs/` for anything
beyond local experimentation.
