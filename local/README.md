# Local container deployment

Production-shape `crawlrs` running on your laptop, end to end. Same
chart as production, same backing services (Redis, Postgres), same
observability stack (vmsingle, Grafana, three provisioned dashboards).

Blob storage is **pod-local FS** for the sandbox (`store.backend.kind
= local`). For production deploys flip to `kind = s3` and bring your
own bucket / region / credentials — the prod chart at
`charts/crawlrs/` already supports both modes via the same
`[store.backend]` toggle.

## What gets deployed

Everything below runs as Pods inside one [`kind`](https://kind.sigs.k8s.io/)
cluster on your machine:

```
crawlrs-demo namespace
+-----------------------------------------------------------------+
|  crawlrs-demo-0            (StatefulSet, the crawler)           |
|       redis://crawlrs-demo-redis:6379                           |
|       postgres://crawlrs-demo-postgres:5432                     |
|       file:///var/lib/crawlrs/data  (emptyDir)                  |
|                                                                 |
|  crawlrs-demo-redis        (StatefulSet, redis:7-alpine)        |
|  crawlrs-demo-postgres     (StatefulSet, postgres:17-alpine)    |
|  crawlrs-demo-vmsingle     (Deployment, metrics storage)        |
|  crawlrs-demo-grafana      (Deployment, 3 dashboards)           |
+-----------------------------------------------------------------+
```

Backing services use **official upstream images** directly via raw
manifests in `charts/crawlrs-demo/templates/`. No third-party Helm
charts in the supply chain.

Blobs (Parquet + WARC) write to a pod-local emptyDir at
`/var/lib/crawlrs/data` by default — fast and simple, but **lost on
pod restart**. To keep them across restarts, set
`crawlrs.store.backend.persistence.enabled=true` (uses a
volumeClaimTemplates-provisioned PVC instead).

Total: ~5 pods, roughly 3-4 GB of RAM in use under steady-state crawl.

## One-time prerequisites

- **Docker** (you already have it; the kind cluster runs as Docker
  containers).
- **kubectl** (`v1.30+`).
- **helm** (`v3.16+`).
- **kind** — install with one of:
  ```bash
  # Linux / macOS via Go
  go install sigs.k8s.io/kind@v0.27.0

  # Linux x86_64 binary install (no Go)
  curl -Lo /tmp/kind https://kind.sigs.k8s.io/dl/v0.27.0/kind-linux-amd64
  install -m 0755 /tmp/kind /usr/local/bin/kind

  # macOS via brew
  brew install kind
  ```
  Confirm: `kind version` should print `v0.27.0` or newer.

## Deploy end to end

```bash
# Bring up everything: cluster, image build, load, helm install,
# wait for ready. First run takes ~10 minutes (BoringSSL compile in
# the Docker builder); subsequent runs ~30 seconds.
make local-up

# Watch crawlrs logs
make local-logs

# Open Grafana (admin / admin) + crawlrs /metrics
make local-pf
# Then in your browser:
#   http://localhost:3000   Grafana
#   http://localhost:9090/metrics
```

## What `make local-up` does

```
1. kind create cluster --config local/kind.yaml          # idempotent
2. docker build -t crawlrs:local .                       # slow first time
3. kind load docker-image crawlrs:local --name crawlrs-local
4. helm dep build charts/crawlrs-demo                    # fetches Bitnami subcharts
5. kubectl create namespace crawlrs --dry-run=client | kubectl apply -f -
6. helm upgrade --install crawlrs-demo charts/crawlrs-demo \
     --values local/values.local.yaml \
     --set-file crawlrs.seeds.content=local/seeds.txt \
     --wait --timeout 5m \
     --namespace crawlrs
```

The `--set-file crawlrs.seeds.content=local/seeds.txt` reads the seed
list from disk and renders it into the chart's ConfigMap so the
crawler picks them up at startup via `--seeds /etc/crawlrs/seeds.txt`.

## Tear down

```bash
make local-down            # uninstall the helm release; cluster stays
make local-cluster-down    # destroy the kind cluster (loses all PVC data)
```

`make local-down` is fast; `make local-cluster-down` is the full reset.

## Where each piece of config lives

| What | Where | Why |
|---|---|---|
| Image | `Dockerfile` | multi-stage cargo-chef build, non-root user |
| Build context filter | `.dockerignore` | keep image size + build context small |
| Cluster config | `local/kind.yaml` | single-node K8s 1.31, port mappings 9090 + 3000 |
| Helm overrides | `local/values.local.yaml` | image source, resource asks, runtime knobs |
| Seed URLs | `local/seeds.txt` | ~80 diverse hosts |
| Crawler config | rendered from `local/values.local.yaml` | same path as production (chart's `templates/configmap.yaml`) |
| Build & deploy targets | `Makefile` (top-level) | `make local-*` family |

## Production parity

This setup is meaningfully prod-shaped:

- **Same chart** — `charts/crawlrs-demo/` wraps the production
  `charts/crawlrs/` chart with raw manifests for Redis + Postgres
  using official upstream images. The crawler StatefulSet, ConfigMap,
  Secret, PDB, Service templates are identical between the two.
- **Same image** — built from this repo's `Dockerfile`. Push it to GHCR
  and prod will use the same artefact.
- **Same observability** — vmsingle + Grafana with the same three
  dashboards (`crawler-health`, `fetch-pipeline`, `frontier-storage`).
- **Same env-var overlay** — `CRAWLRS_REDIS_URL`, `CRAWLRS_POSTGRES_URL`
  (and `CRAWLRS_S3_*` when you flip to S3) flow through the Secret the
  same way they would in prod.

What differs:

- **Blob storage** — sandbox uses `kind = local` (pod-local FS).
  Production deploys use `kind = s3` with real S3 / R2 / GCS;
  the prod chart's `[store.backend]` toggle is the only knob that
  changes. The same `charts/crawlrs/` chart serves both; we just
  don't bundle an in-cluster S3 server (no MinIO, no Bitnami).
- **One-replica everything.** Prod runs 3+ Redis nodes, HA Postgres.
  The demo chart's defaults are single-replica.
- **Persistence sized for a laptop.** PVCs are 5 GiB Redis / 10 GiB PG;
  production sizing is 10-100x.
- **`pullPolicy: Never`** so K8s uses the kind-loaded image instead of
  trying GHCR. Production flips this to `IfNotPresent` and pulls the
  registry-hosted image.
- **Demo-grade credentials.** `crawlrs:crawlrs` for Postgres. Fine
  for the local box; never for prod.

## Iterating

When you change Rust code:

```bash
make image                         # rebuild the image (fast: deps cached)
kind load docker-image crawlrs:local --name crawlrs-local
kubectl rollout restart -n crawlrs sts/crawlrs-demo
```

When you change `local/values.local.yaml` or `local/seeds.txt`:

```bash
make local-up                      # idempotent; helm upgrade picks it up
```

`local-up` is idempotent end-to-end: re-running it on an existing
cluster just upgrades the helm release with whatever config has
changed since.
