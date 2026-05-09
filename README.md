# crawlrs

A distributed web crawler. Written in Rust. Deployed as a Kubernetes
StatefulSet with Redis for queue coordination, Postgres for the
metadata ledger, and S3-compatible object storage for crawled blobs.

## Components

**Frontier.** Per-shard Redis Streams + consumer groups for
at-least-once URL delivery. Per-worker consumer names so multiple
tokio tasks in one process don't collide. `XAUTOCLAIM` reclaims
stranded entries from crashed peers without a separate maintenance
process. Sharding via `HostHashShardPolicy` (default 8 shards);
swappable for `SingleShardPolicy` in tests or a custom policy in
production.

**Politeness.** Per-host wake-time scheduling via Redis ZSETs
(sub-millisecond decisions). Three-tier robots.txt cache:
in-process LRU -> Redis hash -> network fetch, TTL-aligned. Per-domain
delay overrides, manual exclude list, exponential backoff on
429 / 503 / transport errors with `Retry-After` honored as a floor,
circuit breaker after N consecutive failures.

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

**Observability.** 29-metric Prometheus contract. The Helm chart
bundles a single-node VictoriaMetrics for storage and Grafana with
three provisioned dashboards (crawler health overview, fetch
pipeline, frontier + storage). Operators with their own Prometheus or
VM fleet disable the bundled stack with one flag.

**Operability.** `crawlrs-bin` exposes `/metrics` + `/healthz` +
`/livez` + `/readyz` on a single port. SIGTERM-driven graceful
shutdown: mark `/readyz` unhealthy -> 5s drain -> frontier shutdown ->
worker drain -> store flush -> exit. Per-pod ordinal extraction from
the Kubernetes Downward API for stable shard ownership.

## Quickstart

### Sandbox (one command)

```bash
make chart-deps
helm install crawlrs-demo ./charts/crawlrs-demo \
  --create-namespace -n crawlrs

kubectl wait --for=condition=ready pod \
  -l app.kubernetes.io/instance=crawlrs-demo \
  -n crawlrs --timeout=180s

# Grafana
kubectl port-forward -n crawlrs \
  svc/crawlrs-demo-crawlrs-grafana 3000:3000
# http://localhost:3000  (admin / admin)
```

The sandbox bundles Redis + Postgres + the observability stack via
raw manifests with official upstream images. Blob storage is
pod-local FS (`store.backend.kind = local`); production deploys
flip to `kind = s3` against real S3 / R2 / GCS. Single replicas
everywhere, fixed credentials, no persistence; for evaluation, dev
loops, and CI smoke tests only.

See [`charts/crawlrs-demo/README.md`](charts/crawlrs-demo/README.md)
for what's deployed and the migration path to production.

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

### Local (no Kubernetes)

```bash
# Bring up Redis + Postgres via docker run (or your own
# orchestration), then:
cp crates/crawlrs-bin/examples/crawl.toml ./crawl.toml
$EDITOR crawl.toml                    # point at your local services
echo "https://example.com" > seeds.txt
make run                              # cargo run -p crawlrs-bin ...
```

`crawlrs validate --config ./crawl.toml` parses the file and prints a
one-line summary; useful as a pre-flight check.

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

    Redis["Redis (Sentinel-backed; Redis Cluster is a v2 promotion trigger)\n- 8 logical shards via key prefixes\n- per-shard stream + consumer group ('fetchers')\n- per-shard ZSETs (host wake times) and hashes\n  (backoff state, robots cache)\n- XAUTOCLAIM reclaims stranded entries (idle over 5min)\n  from crashed peers; no separate maintenance pod\ndoes not: execute application logic; just\ncoordinates queues + politeness state"]

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
Failure recovery is built into the queue protocol via XAUTOCLAIM,
not into a separate control plane.

## Workspace layout

| Crate | Purpose |
|---|---|
| `crawlrs-core` | Domain types + traits + errors. Zero I/O. |
| `crawlrs-fetch` | `Fetcher` impl backed by `wreq`. |
| `crawlrs-frontier-redis` | `Frontier` impl: Redis Streams + consumer groups. |
| `crawlrs-parse` | `Parser` impl backed by `lol_html`. |
| `crawlrs-politeness` | `Politeness` impl: per-host scheduling, robots cache, backoff. |
| `crawlrs-metadata` | `MetadataStore` impl: Postgres + sqlx. |
| `crawlrs-store` | `Store` impls: `ParquetStore`, `WarcStore`, `MultiStore`. |
| `crawlrs-runtime` | Composition: tokio worker pool + maintenance task. |
| `crawlrs-bin` | The `crawlrs` CLI binary. |
| `crawlrs-fakes` | Test doubles (in-memory impls of the traits). |

## Building

Workspace toolchain pinned in `rust-toolchain.toml` (Rust 1.94.1).
Test suite uses `cargo nextest` with slow-test thresholds configured
in `.config/nextest.toml`.

```bash
make build         # cargo build --workspace
make test          # cargo test --workspace
make lint          # fmt-check + clippy with -D warnings
make help          # full target list
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
