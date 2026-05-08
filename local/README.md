# Local container deployment

Production-shape `crawlrs` running on your laptop, end to end. Same
chart as production, same backing services (Redis, Postgres, MinIO),
same observability stack (vmsingle, Grafana, three provisioned
dashboards). The only thing that changes between this and an EKS / GKE
deploy is the image source: locally-built and side-loaded into a kind
cluster instead of pulled from a registry.

## What gets deployed

Everything below runs as Pods inside one [`kind`](https://kind.sigs.k8s.io/)
cluster on your machine:

```
crawlrs-demo namespace
+----------------------------------------------------+
|  crawlrs-demo-crawlrs-0      (StatefulSet)         |
|       redis://crawlrs-demo-redis:6379              |
|       postgres://crawlrs-demo-postgres:5432        |
|       s3://crawlrs-demo-minio:9000/crawlrs-data    |
|                                                    |
|  crawlrs-demo-redis        (StatefulSet, redis:7-alpine)        |
|  crawlrs-demo-postgres     (StatefulSet, postgres:17-alpine)    |
|  crawlrs-demo-minio        (StatefulSet, minio/minio:RELEASE)   |
|  crawlrs-demo-minio-bucket-init  (Job, mc mb crawlrs-data)      |
|  crawlrs-demo-crawlrs-vmsingle   (metrics storage)              |
|  crawlrs-demo-crawlrs-grafana    (3 dashboards)                 |
+----------------------------------------------------+
```

Backing services use **official upstream images** directly via raw
manifests in `charts/crawlrs-demo/templates/`. No third-party Helm
charts in the supply chain.

Total: ~6 pods, roughly 4-6 GB of RAM in use under steady-state crawl.

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

- **Same chart** — `charts/crawlrs-demo/` is the production `charts/crawlrs/`
  chart wrapped with Bitnami Redis + Postgres + MinIO subcharts. The
  crawler StatefulSet, ConfigMap, Secret, PDB, Service are identical.
- **Same image** — built from this repo's `Dockerfile`. Push it to GHCR
  and prod will use the same artefact.
- **Same observability** — vmsingle + Grafana with the same three
  dashboards (`crawler-health`, `fetch-pipeline`, `frontier-storage`).
- **Same env-var overlay** — `CRAWLRS_REDIS_URL`, `CRAWLRS_POSTGRES_URL`,
  `CRAWLRS_S3_*` flow through the Secret the same way they would in prod.

What differs:

- **One-replica everything.** Prod runs 3+ Redis nodes, HA Postgres,
  multi-replica MinIO. The demo chart's defaults are single-replica.
- **Persistence sized for a laptop.** PVCs are 5 GiB Redis / 10 GiB PG /
  50 GiB MinIO; production sizing is 10-100x.
- **`pullPolicy: Never`** so K8s uses the kind-loaded image instead of
  trying GHCR. Production flips this to `IfNotPresent` and pulls the
  registry-hosted image.
- **Demo-grade credentials.** `crawlrs:crawlrs`, `minioadmin`, etc.
  Fine for the local box; never for prod.

## Iterating

When you change Rust code:

```bash
make image                         # rebuild the image (fast: deps cached)
kind load docker-image crawlrs:local --name crawlrs-local
kubectl rollout restart -n crawlrs sts/crawlrs-demo-crawlrs
```

When you change `local/values.local.yaml` or `local/seeds.txt`:

```bash
make local-up                      # idempotent; helm upgrade picks it up
```

`local-up` is idempotent end-to-end: re-running it on an existing
cluster just upgrades the helm release with whatever config has
changed since.
