# crawlrs

A distributed web crawler. Written in Rust. Deployed as a Kubernetes
StatefulSet with Redis for queue coordination, Postgres for the
metadata ledger, and S3-compatible object storage for crawled blobs.

## Components

**Frontier.** Per-shard per-host URL queue on Redis Stack. URLs flow
through atomic Lua scripts: submit dedups via RedisBloom and pushes
to the host queue (or per-shard overflow if the backlog cap is hit);
claim pops the next ready host, then its next queued URL, and stamps
a lease ZSET entry. A background promoter loop drains the wake ZSET
into the ready list at a configurable tick (50 ms default), so claim
is O(1) under any host count. Stranded URLs whose worker crashed are
re-pushed onto their host queue once the lease expires; the operator
sees this via `crawlrs_frontier_lease_reclaim_total`. Sharding via
`HostHashShardPolicy` (default 8 shards); swappable for
`SingleShardPolicy` in tests or a custom policy in production.

**Politeness.** Policy layer that gates fetches without owning
scheduling state. `check` returns Allow / Disallow on the basis of
robots.txt, the blocklist, and a per-host circuit breaker;
`record_fetch` / `record_failure` return a `NextWake` plan that the
runtime applies via `Frontier::advance_wake`. Three-tier robots.txt
cache (in-process LRU -> Redis hash -> network fetch). Per-domain
overrides, exponential backoff on 429 / 503 / transport errors with
`Retry-After` honored as a floor, circuit breaker after N
consecutive failures.

**Storage.** Two output paths shipped in parallel:

- **ParquetStore**: analytical primary. Arrow + zstd column
  compression. LanceDB ingests it directly; DuckDB / Polars / Spark
  read it natively.
- **WarcStore**: archival mirror. ISO 28500 records, per-record gzip,
  byte-exact HTTP wire framing preserved. Native `WARC-Type: revisit`
  for body deduplication on recrawl.

A `MultiStore` composite fans out writes to both; the metadata ledger
records the Parquet path as the canonical pointer. Both stores share
the same path layout
(`<bucket>/crawlrs/run=<id>/shard=<n>/worker=<id>/{parquet,warc}/...`)
and rotation policy (128 MB / 100k rows / 30 minutes).

**Metadata ledger.** Postgres-backed (`crawlrs-metadata`). Two-table
shape: `url_metadata` (current state per URL) and `url_history`
(append-only event log). Drives cross-run dedup, retry budgeting, and
the dead-letter queue.

**Observability.** 33-metric Prometheus contract. The Helm chart
bundles a single-node VictoriaMetrics for storage and Grafana with
three provisioned dashboards (crawler health overview, fetch
pipeline, frontier + storage). Operators with their own Prometheus or
VM fleet disable the bundled stack with one flag.

**Operability.** `crawlrs-bin` exposes `/metrics` + `/healthz` +
`/livez` + `/readyz` on a single port. SIGTERM-driven graceful
shutdown: mark `/readyz` unhealthy -> 5s drain -> frontier shutdown ->
worker drain -> store flush -> exit. Per-pod ordinal extraction from
the Kubernetes Downward API for stable shard ownership.

## Prerequisites

Pick the path you want first; install only what that path needs.

### For the sandbox path (full K8s-shape stack on your laptop)

| Tool | Version | Purpose | Install |
|---|---|---|---|
| **Docker** | Engine 24+ | Container runtime; kind nodes are Docker containers | [docs.docker.com/engine/install](https://docs.docker.com/engine/install/) |
| **kubectl** | v1.30+ | Talks to the cluster | [kubernetes.io/docs/tasks/tools/](https://kubernetes.io/docs/tasks/tools/) |
| **helm** | v3.16+ | Renders + installs the chart | [helm.sh/docs/intro/install](https://helm.sh/docs/intro/install/) |
| **kind** | v0.27+ | Local single-node Kubernetes | `go install sigs.k8s.io/kind@v0.27.0` or [release binaries](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) |

Verify everything's on PATH:

```bash
make local-deps-check
# expected: ok: docker, kind, helm, kubectl all present
```

### For the bare-metal cargo path (no Kubernetes)

| Tool | Purpose | Install |
|---|---|---|
| **Rust toolchain** | rustup auto-installs the 1.94.1 pin from `rust-toolchain.toml` | [rustup.rs](https://rustup.rs/) |
| **Redis Stack** | Frontier + politeness backend (RedisBloom is required) | `docker run --rm -d -p 6379:6379 redis/redis-stack-server:7.4.0-v0` |
| **Postgres** | Metadata ledger | `docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=crawlrs -e POSTGRES_DB=crawlrs postgres:17-alpine` |

System libs the Rust build needs (Debian/Ubuntu names; equivalents on
macOS via Homebrew, Fedora via dnf):

```bash
sudo apt-get install -y pkg-config libssl-dev cmake clang build-essential
```

## Quickstart

### Sandbox: full K8s stack on your laptop, one command

```bash
make local-up
```

That target idempotently:

1. Creates a `kind` cluster (`crawlrs-local`)
2. Builds the `crawlrs:local` container image (multi-stage `cargo-chef`; first build ~10 min, subsequent rebuilds ~30 s)
3. Loads the image into the kind cluster (no external registry needed)
4. Resolves the demo chart's deps + `helm install` with sandbox values
5. Waits for all 5 pods (`crawlrs`, `redis`, `postgres`, `vmsingle`, `grafana`) to reach Ready

Once it returns, useful follow-up commands:

```bash
make local-status     # pods + helm release state
make local-logs       # tail crawler logs (Ctrl-C to stop)
make local-pf         # port-forward Grafana :3000 and crawler /metrics :9090
make local-down       # helm uninstall (keeps cluster + PVCs)
make local-cluster-down   # destroy the kind cluster (full reset)
```

Browse the Grafana dashboards (`crawler-health`, `fetch-pipeline`,
`frontier-storage`) at <http://localhost:3000> after `make local-pf`
(`admin` / `admin`).

The sandbox bundles Redis + Postgres + the observability stack via
raw manifests with official upstream images. Blob storage is
pod-local FS (`store.backend.kind = local`); production deploys
flip to `kind = s3` against real S3 / R2 / GCS. Single replicas
everywhere, fixed credentials, sandbox-sized PVCs; for evaluation,
dev loops, and CI smoke tests only.

See [`local/README.md`](local/README.md) for the laptop-deployment
walkthrough and [`charts/crawlrs-demo/README.md`](charts/crawlrs-demo/README.md)
for what the chart deploys + the migration path to production.

### Production

Deploy the bare crawlrs chart against your own Redis, Postgres, and
S3-compatible object store:

```bash
helm install my-crawlrs ./charts/crawlrs \
  --namespace crawlrs --create-namespace \
  --set redis.url=redis://redis-master.shared:6379 \
  --set postgres.url=postgres://crawlrs:secret@pg.shared:5432/crawlrs \
  --set store.backend.kind=s3 \
  --set store.backend.s3.bucket=my-crawlrs-data \
  --set store.backend.s3.region=us-east-1 \
  --set secrets.existingSecret=my-crawlrs-creds
```

See [`charts/crawlrs/README.md`](charts/crawlrs/README.md) for the
full values reference, sharding math, and security context defaults.

### Local: no Kubernetes, just `cargo run`

```bash
# 1. Start Redis + Postgres locally (docker is the easiest way; skip
#    if you already have these running)
docker run --rm -d --name crawlrs-redis -p 6379:6379 redis:7-alpine
docker run --rm -d --name crawlrs-postgres -p 5432:5432 \
  -e POSTGRES_USER=crawlrs -e POSTGRES_PASSWORD=crawlrs \
  -e POSTGRES_DB=crawlrs postgres:17-alpine

# 2. Copy the sample config and edit endpoints / credentials
cp crates/crawlrs-bin/examples/crawl.toml ./crawl.toml
$EDITOR crawl.toml

# 3. (Optional) pre-flight: parse + validate the config
make validate
# Or directly: cargo run -p crawlrs-bin -- validate --config ./crawl.toml

# 4. Drop a few seed URLs and start the crawler
echo "https://example.com" > seeds.txt
make run
# Or directly: cargo run -p crawlrs-bin -- crawl --config ./crawl.toml --seeds ./seeds.txt
```

The binary serves `/metrics`, `/healthz`, `/readyz`, `/livez` on
`0.0.0.0:9090` (configurable in `crawl.toml`).

```bash
curl -s localhost:9090/metrics | grep crawlrs_urls_fetched_total
```

Tear down:

```bash
docker stop crawlrs-redis crawlrs-postgres
```

## Architecture

The system is a hexagonal / ports-and-adapters Rust workspace. The
domain crate (`crawlrs-core`) defines traits for `Frontier`,
`Politeness`, `Fetcher`, `Parser`, `Store`, `MetadataStore`,
`SiteAdapter`, and `ShardingPolicy`; concrete impls live in sibling
adapter crates. The runtime crate composes adapters into a tokio
worker pool. A binary crate wires CLI + config + HTTP host. No domain
type touches a backend type at any boundary.

The diagram below shows a canonical 3-pod deployment. Each box
carries a short list of what it does and a `does not:` line marking
the boundary of its responsibility.

```mermaid
flowchart TB
    subgraph statefulset ["crawlrs StatefulSet (3 replicas shown; scales horizontally)"]
        direction LR
        Pod0["crawlrs-0\n- 4 tokio worker tasks\n- owns shards 0, 3, 6 (of 8)\n- per-task Redis consumer name\n  (UUID-suffixed)\ndoes not: coordinate directly\nwith peer pods"]
        Pod1["crawlrs-1\n- 4 tokio worker tasks\n- owns shards 1, 4, 7\nsame shape as Pod-0"]
        Pod2["crawlrs-2\n- 4 tokio worker tasks\n- owns shards 2, 5\nsame shape as Pod-0"]
    end

    Redis["Redis Stack (RedisBloom module required; Sentinel-backed; Redis Cluster is a v2 promotion trigger)\n- 8 logical shards via hash-tagged key prefixes\n- per-shard host_queue LISTs + wake/ready/inflight ZSETs\n- per-shard URL HASH (content-addressed UrlId -> payload)\n- per-shard RedisBloom 'seen' (deployment-wide; cross-run dedup)\n- RDB-only durability (save 60 10000 / 300 10 / 900 1)\ndoes not: execute application logic; just\ncoordinates queues + scheduling state"]

    Postgres["Postgres (single primary; HA is your own replicas)\n- url_metadata: 1 row/URL, mutable; drives cross-run dedup\n- url_history: append-only event log; audit trail\n- schema migrations applied automatically by the binary\ndoes not: shard. Single source of truth\nacross every pod in the StatefulSet."]

    S3["S3-compatible object store\n- path: bucket/crawlrs/run=R/shard=S/worker=W/{parquet,warc}/...\n- per-pod prefixes; no write contention across workers\n- Parquet (analytical, LanceDB-friendly) + WARC (archival, byte-exact wire)\n- rotation: 128 MB / 100k rows / 30 min, whichever fires first\ndoes not: index. URL-to-blob reverse lookup\ngoes through Postgres url_metadata.blob_path"]

    subgraph o11y ["Bundled observability (chart-included; toggle off to BYO Prometheus)"]
        direction TB
        Vm["vmsingle (1 replica, 10 GB PVC)\n- Prometheus-format scrape every 15s\n- per-pod targets via headless-service DNS:\n  crawlrs-N.crawlrs-headless:9090/metrics\n- 1-month retention default\ndoes not: cluster. Promote to vmagent +\nvmstorage at over 1M data points/sec."]
        Gf["Grafana (1 replica)\n- queries vmsingle via Prometheus protocol\n- 3 provisioned dashboards (file-based provider):\n  crawler-health, fetch-pipeline, frontier-storage\ndoes not: persist user state; sandbox-shape"]
        Vm --> Gf
    end

    statefulset -- "URL queue +\npoliteness state" --> Redis
    statefulset -- "metadata reads/writes" --> Postgres
    statefulset -- "Parquet + WARC blobs" --> S3
    Vm -. "per-pod scrape" .-> statefulset

    classDef pod fill:#f8f8ff,stroke:#446,stroke-width:1px
    classDef backend fill:#f4f4f4,stroke:#666,stroke-width:1px
    classDef o11ystyle fill:#fafaf2,stroke:#888,stroke-dasharray:4 2
    class Pod0,Pod1,Pod2 pod
    class Redis,Postgres,S3 backend
    class o11y,Vm,Gf o11ystyle
```

The cardinal architectural property the diagram makes visible: every
adapter lives behind a trait in `crawlrs-core`, so swapping Redis
for an in-memory frontier (in tests) or Postgres for DynamoDB (in
v2) is bounded to the matching adapter crate. Pods don't talk to
each other directly. Coordination is entirely through Redis (queue
distribution + per-host backoff) and Postgres (per-URL state).
Failure recovery is built into the queue protocol via the lease
ZSET and the reclaim pass, not into a separate control plane.

## Workspace layout

| Crate | Purpose |
|---|---|
| `crawlrs-core` | Domain types + traits + errors. Zero I/O. |
| `crawlrs-fetch` | `Fetcher` impl backed by `wreq`. |
| `crawlrs-frontier` | `Frontier` impl: per-host queues + atomic-Lua claim + lease ZSET + RedisBloom dedup. |
| `crawlrs-parse` | `Parser` impl backed by `lol_html`. |
| `crawlrs-politeness` | `Politeness` impl: per-host scheduling, robots cache, backoff. |
| `crawlrs-metadata` | `MetadataStore` impl: Postgres + sqlx. |
| `crawlrs-store` | `Store` impls: `ParquetStore`, `WarcStore`, `MultiStore`. |
| `crawlrs-runtime` | Composition: tokio worker pool + maintenance task. |
| `crawlrs-bin` | The `crawlrs` CLI binary. |
| `crawlrs-fakes` | Test doubles (in-memory impls of the traits). |

## Building & testing

Workspace toolchain pinned in `rust-toolchain.toml` (Rust 1.94.1).
Test suite uses `cargo nextest` with slow-test thresholds configured
in `.config/nextest.toml`.

```bash
make build         # cargo build --workspace
make test          # cargo test --workspace (fast tests + testcontainer integration)
make nextest       # cargo nextest run --workspace (faster CI runner; same suite)
make lint          # fmt-check + clippy with -D warnings
make fmt           # cargo fmt --all
make clippy        # cargo clippy --workspace --all-targets -- -D warnings
make help          # full target list
```

Helm chart targets:

```bash
make chart-lint        # helm lint both charts
make chart-template    # render both charts to /tmp; useful for diffing
make chart-deps        # helm dep build (resolves the file:// crawlrs subchart)
```

Local container deployment targets (require kind + kubectl + helm; see
prerequisites above):

```bash
make image                # docker build -t crawlrs:local .
make local-cluster-up     # kind create cluster (idempotent)
make local-cluster-down   # kind delete cluster
make local-up             # full pipeline: cluster + image + load + helm install
make local-down           # helm uninstall (keeps cluster + PVCs)
make local-logs           # kubectl logs -f sts/crawlrs-demo
make local-pf             # port-forward Grafana :3000 + metrics :9090
make local-status         # pods + helm release status
```

Pre-commit hooks (`.pre-commit-config.yaml`) enforce fmt, clippy, an
ASCII-only-punctuation rule, and typo detection. One-time setup:

```bash
pre-commit install
pre-commit install --hook-type pre-push
cargo install --locked cargo-deny cargo-machete cargo-nextest typos-cli
```

## Contributing

Issues and PRs welcome. Run `make lint` before submitting.

## License

GNU General Public License v3.0 only. See [`LICENSE`](LICENSE).
