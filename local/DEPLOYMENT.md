# Local container deployment

Production-shape `crawlrs` running on your laptop, end to end. Same chart as production, same backing services (Valkey, Postgres), same observability stack (vmsingle, Grafana, four provisioned dashboards).

Blob storage is **pod-local FS** for the sandbox (`store.backend.kind = local`). For production deploys flip to `kind = s3` and bring your own bucket / region / credentials; the prod chart at `charts/crawlrs/` already supports both modes via the same `[store.backend]` toggle.

## What gets deployed

Everything below runs as Pods inside one [`kind`](https://kind.sigs.k8s.io/) cluster on your machine.

```
crawlrs namespace
+-----------------------------------------------------------------+
|  crawlrs-demo-crawlrs-0    (StatefulSet, the crawler)           |
|       redis://crawlrs-demo-redis:6379                           |
|       postgres://crawlrs-demo-postgres:5432                     |
|       file:///var/lib/crawlrs/data  (emptyDir)                  |
|                                                                 |
|  crawlrs-demo-crawlrs-seed (Job, one-shot; runs at install)     |
|                                                                 |
|  crawlrs-demo-redis        (StatefulSet, valkey-bundle)         |
|  crawlrs-demo-postgres     (StatefulSet, postgres:17.2-alpine)  |
|  crawlrs-demo-vmsingle     (Deployment, metrics storage)        |
|  crawlrs-demo-grafana      (Deployment, 3 dashboards)           |
+-----------------------------------------------------------------+
```

Backing services use **official upstream images** directly via raw manifests in `charts/crawlrs-demo/templates/`. No third-party Helm charts in the supply chain. Valkey is `valkey/valkey-bundle` (the frontier's Bloom filter commands require the `valkey-bloom` module, which the bundle image ships by default).

Blobs (Parquet + WARC) write to a pod-local emptyDir at `/var/lib/crawlrs/data` by default; fast and simple, but **lost on pod restart**. To keep them across restarts, set `crawlrs.store.backend.persistence.enabled=true` (uses a `volumeClaimTemplates`-provisioned PVC instead).

Total: ~5 pods + one transient seed Job, roughly 3-4 GB of RAM in use under steady-state crawl.

## One-time prerequisites

The four tools needed (Docker, kubectl, helm, kind) are listed in the [root README's Quick start](../README.md#quick-start). Run `make local-deps-check` to confirm they're all on PATH.

Deeper notes on installing `kind` specifically:

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
make local-up         # cluster + image build + load + helm install + seed Job
make local-logs       # tail crawlrs logs
make local-pf         # open Grafana :3000 + crawlrs /metrics :9090 (admin / admin)
```

First run takes ~3 minutes (BoringSSL compile in the Docker builder); subsequent runs are seconds.

## What `make local-up` does

```
1. kind create cluster --config local/kind.yaml          # idempotent
2. docker build -t crawlrs:local .                       # slow first time
3. kind load docker-image crawlrs:local --name crawlrs-local
4. helm dep build charts/crawlrs-demo                    # resolves file:// crawlrs subchart
5. kubectl create namespace crawlrs --dry-run=client | kubectl apply -f -
6. helm upgrade --install crawlrs-demo charts/crawlrs-demo \
     --values local/values.local.yaml \
     --set-file crawlrs.seeds.content=local/seeds.txt \
     --wait --timeout 5m \
     --namespace crawlrs
7. The chart's post-install crawlrs-seed Job loads local/seeds.txt
   into the Frontier and exits.
```

The StatefulSet itself never touches seeds, so pod restarts do NOT re-trigger seed-loading. To reseed after the initial install, see [`charts/crawlrs/README.md`](../charts/crawlrs/README.md#reseeding-after-the-initial-install).

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
| Seed URLs | `local/seeds.txt` | sandbox seed list |
| Crawler config | rendered from `local/values.local.yaml` | same path as production (chart's `templates/configmap.yaml`) |
| Build & deploy targets | `Makefile` (top-level) | `make local-*` family |

## Production parity

This setup is meaningfully prod-shaped: same chart, same image, same observability, same env-var overlay. The full sandbox-vs-production comparison lives in [`charts/crawlrs-demo/README.md`](../charts/crawlrs-demo/README.md#disclaimers-the-short-version).

The TL;DR is: only `store.backend.kind` (`local` vs `s3`), credentials, persistence sizing, and `pullPolicy` differ. The crawler StatefulSet, ConfigMap, Secret, PDB, Service templates are identical.

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

`local-up` is idempotent end-to-end: re-running it on an existing cluster just upgrades the helm release with whatever config has changed since.
