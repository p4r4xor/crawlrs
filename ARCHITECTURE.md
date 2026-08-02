# Architecture

The domain crate (`crawlrs-core`) defines traits for every external dependency; concrete impls live in sibling adapter crates; a runtime crate composes them into a tokio worker pool; a binary wires CLI + config + HTTP host. No domain type touches a backend type at any boundary.

The cardinal property the layout protects: swapping the Frontier backend, metadata store, or fetcher is bounded to the matching adapter crate. Pods never coordinate directly. State lives in Valkey (queues + scheduling) and Postgres (per-URL ledger). Failure recovery is built into the queue protocol via the lease ZSET and reclaim pass, not into a separate control plane.

## Components

Sections follow the lifecycle of one URL: frontier picks it, politeness gates it, fetch retrieves it, parse extracts from it, store writes it, metadata records the outcome.

### Frontier

Per-shard per-host URL queue on Valkey. URLs flow through atomic Lua scripts: `submit` checks per-host quota (`[crawl] max_urls`), dedups through the Bloom filter module, then pushes to the host queue. `claim` pops the next ready host, then its next queued URL, and stamps a lease ZSET entry. A background promoter loop drains the wake ZSET into the ready list at a configurable tick (50 ms default), so claim stays O(1) under any host count. Stranded URLs whose worker crashed are re-pushed once the lease expires; the operator sees this via `crawlrs_frontier_lease_reclaim_total`.

Sharding via `HostHashShardPolicy` (default 8 shards); swappable for `SingleShardPolicy` in tests or a custom policy in production.

### Politeness

Policy layer that gates fetches without owning the queue. `CompositePoliteness` wires three named collaborators behind their respective traits.

- **`WakePlanner`:** Produces the `NextWake` plan after every `record_fetch` / `record_failure`. The default impl is `AdaptiveWakePlanner`: per-host comfortable rate is learned from observed response codes using AIMD-style adjustment. Successes additively increase the rate; 429 and 503 responses cut it multiplicatively; `Retry-After` from a throttled response is honoured as a hard floor. Per-host state (current rate, recent outcome counts, last throttle timestamp) lives in a Valkey hash on the host's owning shard, alongside the existing wake ZSET. `StaticWakePlanner` stays as the fallback for permissioned crawls where adaptive learning is not appropriate.
- **`RobotsChecker`:** Consults the robots.txt cache and parses with `texting_robots`. The cache is two-tier: in-process moka LRU on the worker, then a Valkey hash shared across pods.
- **`BackoffTracker`:** Opens a per-host circuit breaker after N consecutive failures, preventing the adaptive planner from chasing a host that is genuinely down rather than throttling.

Per-domain overrides act as ceilings on top of the adaptive output: an operator-set max rate cannot be exceeded by the learned rate. The master switch (`[politeness] enabled = false`) swaps in no-op collaborators for stealth crawls against infrastructure you own; the `[crawl]` scope and `[access]` blocklist remain active when politeness is off.

### Fetch

Two `Fetcher` impls behind one trait. The runtime calls `Fetcher::fetch(req)` and never knows which underlying transport served the response.

- **`WreqFetcher`** (in `crawlrs-fetch`) is the default for HTTP traffic. Built on `wreq`, which randomises TLS extension ordering, HTTP/2 SETTINGS frames, and other JA3 / JA4-shaped signals per request. Handles redirects up to a configurable cap, enforces `max_body_bytes`, returns the canonical redirect chain. Decompression covers gzip, brotli, zstd, and deflate.
- **`ChromeFetcher`** (in `crawlrs-fetch-chrome`, closed source today) drives a patched Chromium build for JS-heavy targets. The patches are at the C++ source level: Audio, Canvas, WebGL, and WebRTC spoofing live in the rendering stack rather than as runtime JS injection. Inline reCAPTCHA v3 solver handles score-based challenges in-browser without a round trip to an external provider.

A `RouterFetcher` composes the two and dispatches per URL via a per-domain selection table; `WreqFetcher` is the unconfigured default. The factory wires either a single concrete impl (one transport compiled in) or the router (both compiled in). The parser's `RenderHint` (described below) lets the runtime auto-upgrade a page that looked like HTML to wreq but parses as a client-side-rendered app, by re-submitting through Chrome on the next pass.

Proxy selection sits behind the orthogonal `ProxyResolver` trait. Three impls today: `NoProxyResolver`, `StaticProxyResolver`, `EnvProxyResolver` (reads `HTTP_PROXY` / `HTTPS_PROXY`). Each fetcher consults the resolver per request, so a rotating-proxy impl is a one-crate change.

### Parse

`crawlrs-parse` ships `LolHtmlParser`, a streaming HTML parser built on `lol_html`. Element handlers extract `<a[href]>`, `<base>`, `<link rel="canonical">`, and page metadata (title, description, OG tags) as the parser walks the document. No full DOM is buffered.

Discovered links are canonicalised via `crawlrs-core::CanonicalUrl`: lowercase host, default-port strip, fragment strip, query parameter sort, scheme filter (only `http` and `https` pass). The output `ParsedDocument` carries the canonical URL, content hash for body dedup, page text, link set, and a `RenderHint` (`Html`, `MaybeSpa`, `Pdf`, `Other`). The hint is what feeds back into the fetch layer's auto-upgrade decision; a `MaybeSpa` page on the wreq path is a candidate for re-fetching through `ChromeFetcher`.

The parser pre-sizes its output `Vec` and `String` allocations from the body size, keeping per-page allocator churn bounded under steady load.

### Storage

Two output paths shipped in parallel.

- **`ParquetStore`:** Analytical primary. Arrow + zstd column compression. LanceDB ingests directly; DuckDB / Polars / Spark read natively.
- **`WarcStore`:** Archival mirror. ISO 28500 records, per-record gzip, byte-exact HTTP wire framing preserved. Native `WARC-Type: revisit` for body deduplication on recrawl.

A `MultiStore` composite fans out writes to both; the metadata ledger records the Parquet path as the canonical pointer. Shared path layout (`<bucket>/crawlrs/run=<id>/shard=<n>/worker=<id>/{parquet,warc}/...`) and rotation policy (size / row count / wall-clock, whichever fires first; defaults tuned in `values.yaml`).

### Metadata ledger

Postgres-backed (`crawlrs-metadata`). Two-table shape: `url_metadata` (current state per URL) and `url_history` (append-only event log). Drives cross-run dedup, retry budgeting, and the dead-letter queue.

Workers have two strategies for outbound URL dispatch.

- **`linkDispatch: direct`:** Worker calls `Frontier::submit_batch` itself after the metadata commit. Bounded loss under transient Frontier errors.
- **`linkDispatch: durable_outbox`:** Outbound URLs commit atomically into a `frontier_outbox` table in the same metadata transaction. A horizontally-leased publisher drains the table into the Frontier with at-least-once delivery.

### Seed bootstrap

A separate `crawlrs seed` subcommand loads the initial URL list into the Frontier and exits. The Helm chart runs it as a one-shot `post-install` Job, so the StatefulSet itself never reads seeds and pod restarts are pure-runtime. Per-batch failures (Valkey OOM, transient errors) get logged and the loader continues; the Job exits non-zero only if every batch fails.

### Observability

Prometheus metrics contract spanning fetch, parse, store, frontier, politeness, and metadata. The Helm chart bundles a single-node VictoriaMetrics for storage and Grafana with four provisioned dashboards.

- **Crawler Health Overview:** Rates, success, latency, error attribution.
- **Worker Health:** Per-worker throughput, latency, restart count, skip rate.
- **Container Resources:** CPU, memory, network, file descriptors.
- **Valkey Health:** Memory against the `maxmemory` cap, eviction, per-key-group usage.

Operators with their own Prometheus or VM fleet disable the bundled stack with one flag.

### Operability

`crawlrs-bin` exposes `/metrics` + `/healthz` + `/livez` + `/readyz` on a single port. SIGTERM-driven graceful shutdown: mark `/readyz` unhealthy, 5 s drain, frontier shutdown, worker drain, store flush, exit. Per-pod ordinal extraction from the Kubernetes Downward API for stable shard ownership.

## Workspace layout

| Crate | Purpose |
|---|---|
| `crawlrs-core` | Domain types + traits + errors. Zero I/O. |
| `crawlrs-fetch` | `WreqFetcher` (HTTP) and the `RouterFetcher` composer. |
| `crawlrs-fetch-chrome` | `ChromeFetcher`: patched Chromium build + Audio / Canvas / WebGL / WebRTC source-level spoofing + reCAPTCHA v3 solver. Closed source today. |
| `crawlrs-frontier` | `Frontier` impl: per-host queues, atomic-Lua claim, lease ZSET, Bloom filter dedup, per-host quota. |
| `crawlrs-parse` | `LolHtmlParser`, streaming HTML extraction. |
| `crawlrs-politeness` | `CompositePoliteness` wiring `AdaptiveWakePlanner` (+ `StaticWakePlanner` fallback) + `RobotsChecker` + `BackoffTracker`. Two-tier robots cache. |
| `crawlrs-metadata` | `MetadataStore` + `Outbox` impl: Postgres + sqlx. |
| `crawlrs-store` | `Store` impls: `ParquetStore`, `WarcStore`, `MultiStore`. |
| `crawlrs-runtime` | Composition: tokio worker pool, maintenance task, outbox publisher. |
| `crawlrs-bin` | The `crawlrs` CLI binary (`crawl`, `seed`, `validate`, `version`). |
| `crawlrs-fakes` | Test doubles (in-memory impls of every trait). |

## Testing

Testcontainer-backed integration suites exercise the real Valkey + Postgres adapters; unit tests use the `crawlrs-fakes` in-memory impls for everything else.

```bash
make test           # cargo test --workspace (incl. testcontainer integration)
make nextest        # cargo nextest run --workspace (faster CI runner; same suite)
```

## Helm charts

| Chart | Purpose |
|---|---|
| `charts/crawlrs/` | The production chart. StatefulSet + ConfigMap + Secret + Service + seed Job + bundled observability. BYO Valkey / Postgres / S3. |
| `charts/crawlrs-demo/` | Umbrella that wraps `crawlrs/` with raw manifests for Valkey + Postgres using official upstream images. Sandbox-shape; `make local-up` uses this. |

```bash
make chart-lint        # helm lint both charts
make chart-template    # render to /tmp; useful for diffing
make chart-deps        # helm dep build (resolves the file:// crawlrs subchart)
```
