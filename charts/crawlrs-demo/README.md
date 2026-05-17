# crawlrs-demo Helm chart

One-command sandbox install of the full crawlrs stack:

- `crawlrs` (the crawler StatefulSet, via the `charts/crawlrs/` subchart)
- `vmsingle` + Grafana (bundled observability, three provisioned dashboards)
- Redis Stack (frontier + politeness backend, `redis/redis-stack-server:7.4.0-v0`; RedisBloom required)
- Postgres (metadata ledger, `postgres:17.2-alpine`)

Backing services run as raw `StatefulSet` + `Service` manifests in `templates/{redis,postgres}-*.yaml` using official upstream images directly. No third-party charts in the supply chain.

Blob storage uses the **pod-local FS backend** (`store.backend.kind = local`); blobs land at `/var/lib/crawlrs/data` inside the crawler pod. By default this is an `emptyDir` volume (ephemeral, lost on pod restart); toggle `crawlrs.store.backend.persistence.enabled=true` to back it with a PVC instead.

Useful for: trying crawlrs without standing up your own backing services, dev-loop integration tests, and CI smoke tests.

**Not for production.** Sandbox shape: single replicas everywhere, fixed credentials, sandbox-sized PVCs.

The sandbox topology diagram and the laptop-specific deployment walkthrough live in [`local/DEPLOYMENT.md`](../../local/DEPLOYMENT.md).

## TL;DR

```bash
helm dep build ./charts/crawlrs-demo
( cd ./charts/crawlrs-demo/charts && for f in *.tgz; do tar -xzf "$f"; done )

# Install (release name MUST be `crawlrs-demo` for the default
# Service-DNS values to resolve)
helm install crawlrs-demo ./charts/crawlrs-demo \
  --set-file crawlrs.seeds.content=./seeds.txt \
  --create-namespace -n crawlrs

# Wait for everything
kubectl wait --for=condition=ready pod \
  -l app.kubernetes.io/instance=crawlrs-demo \
  -n crawlrs --timeout=180s
```

The chart's post-install Job loads `--set-file crawlrs.seeds.content` into the Frontier once and exits. Skip the `--set-file` flag to install with an empty Frontier.

## Why the release name matters

Service names emitted by the demo chart's templates are of the form `<release>-<role>` (e.g. `crawlrs-demo-redis`, `crawlrs-demo-postgres`). The `crawlrs` subchart's URLs in `values.yaml` reference those by literal string. If you change the release name, those names won't resolve.

Either install with `crawlrs-demo` as the release name (the chart's NOTES.txt will warn you if you don't), or override:

```bash
helm install my-name ./charts/crawlrs-demo \
  --set crawlrs.redis.url=redis://my-name-redis:6379 \
  --set crawlrs.postgres.url=postgres://crawlrs:crawlrs@my-name-postgres:5432/crawlrs \
  --set crawlrs.secrets.values.redisUrl=redis://my-name-redis:6379 \
  --set crawlrs.secrets.values.postgresUrl=postgres://crawlrs:crawlrs@my-name-postgres:5432/crawlrs
```

## Migrating to production

This chart is the wrong shape for production. When you outgrow it: stand up production-grade Redis (Sentinel or managed service with RedisBloom), Postgres (managed RDS / Cloud SQL / your own HA cluster), and S3-compatible storage outside Kubernetes; then switch to the bare [`charts/crawlrs/`](../crawlrs/README.md) chart with `o11y.enabled=true` so you keep the bundled observability but drop the bundled deps.

See [`charts/crawlrs/README.md`](../crawlrs/README.md#tldr) for the production helm install command and the full values reference. The `crawlrs.*` subchart values you pass to `crawlrs-demo` map directly to the `charts/crawlrs/` values; the only meaningful change between sandbox and production is `store.backend.kind` (`local` vs `s3`) plus the URLs of the backing services.

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

If your evaluation works, switch to `charts/crawlrs/` for anything beyond local experimentation.
