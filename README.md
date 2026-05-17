<pre align="center">
                                           $$\                     
                                           $$ |                    
 $$$$$$$\  $$$$$$\  $$$$$$\  $$\  $$\  $$\ $$ | $$$$$$\   $$$$$$$\ 
$$  _____|$$  __$$\ \____$$\ $$ | $$ | $$ |$$ |$$  __$$\ $$  _____|
$$ /      $$ |  \__|$$$$$$$ |$$ | $$ | $$ |$$ |$$ |  \__|\$$$$$$\  
$$ |      $$ |     $$  __$$ |$$ | $$ | $$ |$$ |$$ |       \____$$\ 
\$$$$$$$\ $$ |     \$$$$$$$ |\$$$$$\$$$$  |$$ |$$ |      $$$$$$$  |
 \_______|\__|      \_______| \_____\____/ \__|\__|      \_______/ 
 
</pre>

<p align="center">
  <a href="./ARCHITECTURE.md">Architecture</a> |
  <a href="./charts/crawlrs/">Helm chart</a> |
  <a href="./local/DEPLOYMENT.md">Local sandbox</a> |
  <a href="./crates/crawlrs-bin/examples/crawl.toml">Example config</a>
</p>

<p align="center">
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-GPL--3.0-informational" alt="License"></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/built%20with-Rust-orange" alt="Built with Rust"></a>
  <a href="https://kubernetes.io/"><img src="https://img.shields.io/badge/runs%20on-Kubernetes-blue" alt="Runs on Kubernetes"></a>
</p>

<p align="center">An extremely fast distributed web crawler. Deploy anywhere. Monitor like production.</p>

- **Extremely fast.** Hundreds of async workers per pod, one round trip per URL batch, 50 ms scheduling tick. Scales horizontally; throughput grows with cluster size.
- **Built to survive.** Fault tolerance is built in at every layer of the pipeline. Failed fetches retry; crashed workers recover; the queue and the metadata ledger both survive backend restarts; exhausted retries land in a dead-letter queue. Nothing is silently dropped.
- **Built for stealth.** Randomised TLS / HTTP-2 fingerprints on every HTTP request, plus a patched Chrome binary with source-level Audio / Canvas / WebGL / WebRTC spoofing for JS-heavy targets. No flaky JS injection; the patches live in the binary.
- **Deploy anywhere.** Standalone binary, container image, or a one-command Helm install with bundled Redis + Postgres + Grafana. The same chart runs on a laptop kind cluster and in production.
- **Monitor like production.** Prometheus contract across every stage. Three provisioned Grafana dashboards out of the box. Liveness / readiness / startup probes wired to the actual pipeline.
- **Plug in your own crawl logic.** Per-domain `SiteAdapter` hooks for sites that need bespoke extraction, login flows, or custom pagination. URL-pattern routing picks the right adapter; everything else falls through to the generic pipeline.

---

## Quick start

Get the whole stack running on your laptop with one command. You need four tools on PATH:

| Tool | Version | Install |
|---|---|---|
| **Docker** | Engine 24+ | [docs.docker.com/engine/install](https://docs.docker.com/engine/install/) |
| **kubectl** | v1.30+ | [kubernetes.io/docs/tasks/tools/](https://kubernetes.io/docs/tasks/tools/) |
| **helm** | v3.16+ | [helm.sh/docs/intro/install](https://helm.sh/docs/intro/install/) |
| **kind** | v0.27+ | `go install sigs.k8s.io/kind@v0.27.0` or [release binaries](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) |

```bash
git clone https://github.com/p4r4xor/crawlrs && cd crawlrs
make local-deps-check     # sanity-check the four tools above
make local-up             # bring up the stack
```

That target idempotently:

1. Creates a `kind` cluster (`crawlrs-local`).
2. Builds the `crawlrs:local` container image (multi-stage `cargo-chef`; first build ~3 minutes, subsequent rebuilds ~30 s).
3. Loads the image into the kind cluster (no external registry needed).
4. Resolves the demo chart's deps and runs `helm install` with sandbox values.
5. Waits for all five pods (`crawlrs`, `redis`, `postgres`, `vmsingle`, `grafana`) to reach Ready.
6. Runs a post-install `crawlrs-seed` Job that loads `local/seeds.txt` into the Frontier and exits.

```bash
make local-status         # pods + helm release state
make local-pf             # open Grafana at :3000, /metrics at :9090
make local-logs           # tail crawler logs
make local-down           # helm uninstall (keeps cluster + PVCs)
make local-cluster-down   # destroy the kind cluster (full reset)
```

Iterate by editing `local/seeds.txt` or `local/values.local.yaml` and re-running `make local-up`; it's idempotent.

---

## What you can build

- **A continuously-fresh search corpus.** Re-crawl a known domain set on a cadence; downstream readers see Parquet they can query immediately.
- **An ML training dataset that's actually yours.** Your bucket, your schema, no third-party pipeline in between.
- **A vector-store ingest pipeline.** Crawl → Parquet → LanceDB / pgvector / Qdrant. Same blob layout works for all three.
- **A competitive-intelligence feed.** Cross-run dedup means each run only emits URLs that are new or changed.
- **An archival mirror.** WARC output is byte-exact wire format; the same crawl feeds both analytical and archival paths.

---

## What crawlrs does well

**Atomic, reasonable frontier.** Per-host queues with atomic admission, dedup, and per-host quota checks (`max_urls`, `max_depth`). Each host's queue is independent so hot domains don't starve cold ones.

**Fault-tolerant by construction.** Every layer in the pipeline (worker, queue, metadata, outbound dispatch) has its own recovery story. The runtime never silently drops a URL: it gets retried, recovered after a backend restart, or routed to the dead-letter queue.

**Adaptive politeness across millions of domains.** Per-host scheduling, robots.txt cache, backoff on failure, per-domain overrides. Comfortable rates are learned per-host from observed responses instead of a static delay. Hosts that ask for slowdowns get them; hosts that don't get full throughput. State is sharded so politeness scales with the cluster.

**Anti-bot by default.** Randomised TLS / HTTP-2 fingerprints on every HTTP request slip past common detection. For JS-heavy targets, a patched Chrome binary (closed source today) handles rendering with source-level Audio / Canvas / WebGL / WebRTC spoofing.

**Smart scheduling.** Wake-time ordering with atomic per-host quotas. Workers never race for the same URL. Priority weighting (depth, freshness, host-importance) is on the roadmap.

**Output your warehouse already understands.** Parquet (analytical; LanceDB-ready) and WARC (archival; byte-exact wire) write in parallel under the same path layout. Plug into LanceDB, DuckDB, Polars, or Spark with no transform step.

**Trait-boundary separation.** `crawlrs-core` defines `Frontier`, `Politeness`, `Fetcher`, `Parser`, `Store`, `MetadataStore`, `SiteAdapter`, `ShardingPolicy`. Every concrete impl is one crate. Swapping the metadata store from Postgres to something else is a one-crate change, not a refactor.

---

## Custom crawl logic

Two ways to attach per-domain extraction. Rust impls of `SiteAdapter` for compile-time-checked logic; hot-loadable Lua / WASM scripts for adapters you want to change without rebuilding. Both live behind the same trait and dispatch via first-match URL routing.

```rust
use async_trait::async_trait;
use crawlrs_core::{CanonicalUrl, FetchResponse, ParsedDocument, Result, SiteAdapter};

struct GitHubAdapter;

#[async_trait]
impl SiteAdapter for GitHubAdapter {
    fn matches(&self, url: &CanonicalUrl) -> bool {
        url.host() == Some("github.com")
    }

    async fn extract(&self, resp: &FetchResponse) -> Result<Option<ParsedDocument>> {
        // Custom shape: README, repo metadata, file listings, etc.
        // Return Ok(Some(doc)) to handle, Ok(None) to fall through.
        todo!()
    }
}
```

Register in priority order via `SiteAdapterRegistry::register`; first match wins.

---

## Deploy

Helm install against your own Redis, Postgres, and S3-compatible object store:

```bash
helm install my-crawlrs ./charts/crawlrs \
  --namespace crawlrs --create-namespace \
  --set redis.url=redis://redis-master.shared:6379 \
  --set postgres.url=postgres://crawlrs:secret@pg.shared:5432/crawlrs \
  --set store.backend.kind=s3 \
  --set store.backend.s3.bucket=my-crawlrs-data \
  --set store.backend.s3.region=us-east-1 \
  --set secrets.existingSecret=my-crawlrs-creds \
  --set-file seeds.content=./seeds.txt
```

The chart runs a one-shot `crawlrs seed` Job on `helm install` to load the seeds. `helm upgrade` does not re-seed; to reload seeds after the initial install, run `helm install --replace` or `kubectl create job --from=job/<release>-crawlrs-seed reseed-$(date +%s)`. 

See [`charts/crawlrs/README.md`](./charts/crawlrs/README.md) for the full values reference and security context.

### Other ways to run

For when you want crawlrs without Kubernetes:

| Mode | Command |
|---|---|
| Standalone binary | `cargo build --release && ./target/release/crawlrs --help` |
| Container image | `docker build -t crawlrs:local .` |

Both still need Redis Stack and Postgres reachable. See [`local/DEPLOYMENT.md`](./local/DEPLOYMENT.md) for one-line Docker commands to bring them up.

---

## Roadmap

- **Coverage-driven scheduling.** Spread the crawl budget across the target so a finite number of requests reaches as much of it as possible.
- **Priority scheduling.** Pick the next URL by expected value (depth bias, freshness, host importance) instead of pure queue order.
- **Published benchmarks.** Repeatable throughput numbers in `BENCHMARKS.md`.
- **Open-source patched Chrome binary.** Possible future release of the rendering extension.

---

## Documentation

- **Architecture**: [`ARCHITECTURE.md`](./ARCHITECTURE.md)
- **Local sandbox walkthrough**: [`local/DEPLOYMENT.md`](./local/DEPLOYMENT.md)
- **Helm chart values reference**: [`charts/crawlrs/README.md`](./charts/crawlrs/README.md)
- **Example crawl.toml**: [`crates/crawlrs-bin/examples/crawl.toml`](./crates/crawlrs-bin/examples/crawl.toml)
- **CLI reference**: `crawlrs --help`

---

## Contributing

Issues and PRs welcome.

```bash
make help        # full target list
make test        # cargo test --workspace (incl. testcontainer integration)
make lint        # fmt-check + clippy -D warnings
```

Pre-commit hooks enforce fmt, clippy, ASCII-only punctuation, and typo detection. One-time setup installs the hooks plus the CLI tools they call:

```bash
pre-commit install
pre-commit install --hook-type pre-push
cargo install --locked cargo-deny cargo-machete cargo-nextest typos-cli
```

---

## License

[GPL-3.0-only](./LICENSE).
