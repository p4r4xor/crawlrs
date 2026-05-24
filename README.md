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

- **Extremely fast.** Thousands of async workers per pod, batched at the queue boundary so each URL costs about one Redis round trip. Throughput scales linearly when you add pods.
- **Built to survive.** Every stage has its own recovery path: retries on transient failures, lease-based recovery for crashed workers, a persistent queue and ledger that survive Redis or Postgres restarts. URLs that exhaust their retry budget land in a dead-letter queue instead of disappearing.
- **Built for stealth.** Every HTTP request ships with a randomised TLS and HTTP-2 fingerprint, so common bot-detection heuristics don't see a single repeating signature. For JS-heavy targets, a patched Chrome handles rendering; Audio, Canvas, WebGL, and WebRTC spoofing live in the binary, not in injected JS that breaks every Chrome release.
- **Deploy anywhere.** Ship a standalone binary, run the container image, or `helm install` the whole stack: Redis, Postgres, and Grafana included. The same chart runs on a laptop kind cluster and in production; no separate config tree per environment.
- **Monitor like production.** Every pipeline stage exports Prometheus metrics on the same contract, so dashboards built for one deployment work for the next. Three Grafana dashboards ship pre-provisioned, and the liveness / readiness / startup probes check actual pipeline health, not just whether the binary is running.
- **Plug in your own crawl logic.** Some sites need bespoke handling: custom extraction shapes, login flows, weird pagination. Register a `SiteAdapter` keyed on a URL pattern; matching URLs run your code, everything else falls through to the generic pipeline.

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

## What you can build!

- **A continuously-fresh search corpus.** Point the crawler at a domain set, schedule re-crawls on a cadence, and let downstream readers query the Parquet output directly. Each run deduplicates against previous runs, so only new or changed pages land in the next batch.
- **An ML training dataset you actually own.** The data lands in your bucket, in your schema, with no third-party pipeline sitting between the crawl and the training job.
- **A vector-store ingest pipeline.** Crawl into Parquet, then load into LanceDB, pgvector, or Qdrant. The blob layout is the same for all three, so switching stores doesn't mean re-crawling.
- **An archival mirror.** WARC output is byte-exact wire format. The same crawl can feed both an analytical path (Parquet) and an archival path (WARC) in parallel; you don't have to choose one.

---

## What crawlrs does well

**Atomic, reasonable frontier.** Every URL goes through one atomic admission path: bloom dedup, per-host quota check (`max_urls`, `max_depth`), then enqueue. Each host has its own queue, so a hot domain can't starve everything else.

**Fault-tolerant by construction.** Every layer in the pipeline has its own recovery path. Workers restart under a supervisor with a bounded restart budget. The queue and ledger survive Redis or Postgres restarts. URLs that fail transiently get retried; URLs that exhaust their budget land in a dead-letter queue. Nothing is silently dropped.

**Adaptive politeness across millions of domains.** The crawler learns a comfortable rate per host from observed responses instead of applying a static delay everywhere. Hosts that return 429s or 503s get exponential backoff; hosts that respond normally get full throughput. On top of that: per-host robots.txt caching, a circuit breaker for persistently failing hosts, and per-domain overrides when you need manual control. The politeness state is sharded, so it scales with the cluster.

**Anti-bot by default.** Every HTTP request ships with a randomised TLS and HTTP-2 fingerprint. For JS-heavy targets, a patched Chrome binary (closed source today) handles rendering with source-level Audio, Canvas, WebGL, and WebRTC spoofing baked into the binary itself.

**Smart scheduling.** Hosts are ordered by wake-time and claimed atomically, so workers never race for the same URL. Per-host quotas cap how many URLs any single domain can consume from the crawl budget. Priority weighting (depth, freshness, host-importance) is on the roadmap.

**Output your warehouse already understands.** Parquet and WARC write in parallel under the same path layout. Parquet is query-ready for LanceDB, DuckDB, Polars, or Spark with no transform step. WARC gives you byte-exact wire format for archival or compliance.

**Trait-boundary separation.** The core crate defines the trait surfaces: `Frontier`, `Politeness`, `Fetcher`, `Parser`, `Store`, `MetadataStore`, `SiteAdapter`, `ShardingPolicy`. Every concrete implementation lives in its own crate. If you want to swap the metadata store from Postgres to something else, that's a one-crate change, not a cross-repo refactor.

---

## Custom crawl logic

There are two ways to attach per-domain extraction. Pick based on how often the logic changes:

- **Rust adapters** - write a `SiteAdapter` impl compiled into the binary. Type-checked, fast, best when the rules are stable.
- **Lua / WASM scripts** - drop a script into the config path. Reloads without a rebuild or restart, best when you're still iterating.

The runtime tries each registered adapter against the URL in order and uses the first match. Everything else falls through to the generic pipeline.

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

| Feature | Status | What it does |
|---|---|---|
| **Priority scheduling** | planned | Pick the next URL by expected value (depth, freshness, host importance) instead of pure queue order. |
| **Coverage-driven scheduling** | planned | Spread the crawl budget across the target so a finite number of requests reaches as much of it as possible. |
| **Published benchmarks** | planned | Repeatable throughput numbers in `BENCHMARKS.md` so you can size a cluster before deploying. |
| **Open-source Chrome binary** | exploring | Possible future release of the patched rendering extension. |

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
