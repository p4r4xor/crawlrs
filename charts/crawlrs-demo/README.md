# crawlrs-demo Helm chart (Phase 6d)

One-command sandbox install of the full crawlrs stack:

- `crawlrs` (the crawler StatefulSet)
- `vmsingle` + Grafana (bundled observability)
- Bitnami Redis (frontier + politeness backend)
- Bitnami Postgres (metadata ledger)
- Bitnami MinIO (S3-compatible object store with `crawlrs-data`
  bucket pre-created)

Useful for: trying crawlrs without standing up your own backing
services, dev-loop integration tests, and CI smoke tests.

**Not for production.** Sandbox shape: single replicas everywhere,
fixed credentials, no persistence. See the bottom of `NOTES.txt` for
the full list of caveats.

## TL;DR

```bash
# One-time: register the Bitnami repo, fetch + unpack subcharts.
helm repo add bitnami https://charts.bitnami.com/bitnami || true
helm dep build ./charts/crawlrs-demo
# Helm 3.16 needs the subchart tarballs unpacked alongside the .tgz.
# (Newer helm versions can drop this step.)
( cd ./charts/crawlrs-demo/charts && for f in *.tgz; do tar -xzf "$f"; done )

# Install (release name MUST be `crawlrs-demo` for the default
# service-DNS values to resolve)
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
|  crawlrs-demo-crawlrs-0  (StatefulSet)         |
|         |                                      |
|         |  redis://crawlrs-demo-redis-master   |
|         +---------------------------------+    |
|         |  postgres://crawlrs-demo-...    |    |
|         +---------------------------------+    |
|         |  s3://crawlrs-demo-minio/...    |    |
|         |                                 |    |
|  crawlrs-demo-redis-master  (Deployment) <-+   |
|  crawlrs-demo-postgresql    (StatefulSet)  +-->|
|  crawlrs-demo-minio         (Deployment) <-+   |
|                                                |
|  crawlrs-demo-crawlrs-vmsingle  (Deployment)   |
|         scrapes /metrics from crawlrs-0        |
|                                                |
|  crawlrs-demo-crawlrs-grafana  (Deployment)    |
|         queries vmsingle, ships 3 dashboards   |
+------------------------------------------------+
```

## Why the release name matters

Subcharts emit Service names of the form `<release>-<name>`. The
`crawlrs` subchart in `values.yaml` references those by literal
string (`crawlrs-demo-redis-master`, `crawlrs-demo-postgresql`,
`crawlrs-demo-minio`). If you change the release name, those names
won't resolve.

**Fix**: install with `crawlrs-demo` as the release name (the chart's
NOTES.txt will warn you if you don't), or override:

```bash
helm install my-name ./charts/crawlrs-demo \
  --set crawlrs.redis.url=redis://my-name-redis-master:6379 \
  --set crawlrs.postgres.url=postgres://crawlrs:crawlrs@my-name-postgresql:5432/crawlrs \
  --set crawlrs.store.backend.s3.endpoint=http://my-name-minio:9000 \
  --set crawlrs.secrets.values.redisUrl=redis://my-name-redis-master:6379 \
  --set crawlrs.secrets.values.postgresUrl=postgres://crawlrs:crawlrs@my-name-postgresql:5432/crawlrs
```

## Migrating to production

This chart is the wrong shape for production. When you outgrow it:

1. Stand up production-grade Redis (with Sentinel or managed
   service), Postgres (managed RDS / Cloud SQL / your own HA
   cluster), and S3-compatible storage outside Kubernetes.
2. Switch to the bare `charts/crawlrs/` chart with `o11y.enabled=true`
   so you keep the bundled observability but drop the bundled deps:

   ```bash
   helm install my-crawlrs ./charts/crawlrs \
     --set redis.url=... \
     --set postgres.url=... \
     --set store.backend.kind=s3 \
     --set store.backend.s3.bucket=... \
     --set secrets.existingSecret=my-crawlrs-secrets \
     ...
   ```

The `crawlrs` subchart values you pass to `crawlrs-demo` map directly
to the `charts/crawlrs/` values; nothing changes except where the
backing services live.

## Disclaimers (the short version)

| | crawlrs-demo | charts/crawlrs/ |
|---|---|---|
| Backing services | bundled, single-replica | external, your call |
| Persistence | none (data lost on pod restart) | S3 + your DB persistence |
| Credentials | hard-coded sandbox values | externally-managed Secret |
| HA / failover | none | yes |
| Resources | laptop-sized | configure to taste |
| Observability | bundled (vmsingle + Grafana) | bundled (vmsingle + Grafana) |

If your evaluation works, switch to `charts/crawlrs/` for anything
beyond local experimentation.
