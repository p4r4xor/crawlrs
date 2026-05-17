# crawlrs-bin

The `crawlrs` CLI binary. Wires every workspace crate together into
a runnable process.

## Quick start (sandbox)

Spin up Redis + Postgres (via `docker run` or your preferred
orchestration), point the crawler at a local-FS store path, then:

```bash
# Copy the sample config
cp crates/crawlrs-bin/examples/crawl.toml ./crawl.toml
# Edit endpoints / credentials to match your local services
$EDITOR crawl.toml

# Validate the config without starting workers
cargo run -p crawlrs-bin -- validate --config crawl.toml

# Bootstrap the Frontier with seeds (one-shot; idempotent via bloom)
echo "https://example.com" > seeds.txt
cargo run -p crawlrs-bin -- seed --config crawl.toml --path seeds.txt

# Start the crawler (pure runtime; never touches seeds again)
cargo run -p crawlrs-bin -- crawl --config crawl.toml
```

The binary mounts an HTTP server at `0.0.0.0:9090` (configurable in
`crawl.toml`) exposing four endpoints:

- `/metrics` - Prometheus exposition format spanning fetch, parse, store, frontier, politeness, and metadata.
- `/healthz` - process is up. Always 200 if the binary is running.
- `/livez` - internal liveness. 200 unless the worker pool has
  deadlocked.
- `/readyz` - ready to serve. 200 once adapters connected; flips
  503 during shutdown so scrapers stop hitting the pod before
  workers drain.

## CLI

```text
crawlrs <COMMAND>

Commands:
  crawl     Start the worker pool. Loads the config, builds the
            runtime, mounts the HTTP host, and runs until SIGTERM.
            Does not touch seeds.
  seed      One-shot: load URLs from a file into the Frontier and
            exit. Tolerant of per-batch failures. Intended to run
            once via a Helm post-install Job.
  validate  Parse the config file and print a one-line summary.
            Exits non-zero on parse / structural errors.
  version   Print the binary version.
```

## Environment variables

A small set of high-impact knobs accept env overrides for ConfigMap
ergonomics in Kubernetes:

| Var                  | Overrides              |
|----------------------|------------------------|
| `CRAWLRS_CONFIG`     | `--config` path |
| `CRAWLRS_SEEDS`      | `crawlrs seed --path` |
| `CRAWLRS_RUN_ID`     | top-level `run_id` |
| `CRAWLRS_REDIS_URL`  | `[redis].url` |
| `CRAWLRS_POSTGRES_URL` | `[postgres].url` |
| `CRAWLRS_LISTEN`     | `[server].listen` |
| `CRAWLRS_WORKERS`    | `[runtime].workers` |
| `CRAWLRS_LOG_FORMAT` | `text` (default) or `json` |
| `RUST_LOG`           | per-crate verbosity (e.g. `crawlrs_runtime=debug,info`) |
| `POD_NAME`           | derives shard ownership in StatefulSet deploys |
| `CRAWLRS_REPLICAS`   | replica count for shard-ownership math |

## Shutdown

SIGTERM (or Ctrl-C) triggers graceful shutdown:

1. `/readyz` flips to 503 (load balancers stop sending traffic).
2. 5-second drain delay (in-flight scrapes / probes land).
3. Worker pool stops accepting new claims.
4. In-flight URLs finish processing (frontier ack/nack).
5. Store flush.
6. Process exits.

## Configuration

See `examples/crawl.toml` for the fully-commented template. Every
section has sensible defaults; the only required keys are `run_id`,
`[redis].url`, and `[postgres].url`.

## Architecture

This crate is the composition root. It owns no business logic; it:

- Parses CLI args via `cli::Cli` (clap-derive). Four subcommands dispatch to four entry points.
- Loads + validates `crawl.toml` via `config::CrawlrsConfig`.
- Builds adapters via `factory::build` (full adapter graph for `crawl`) or `factory::build_frontier` (Redis-only for `seed`, no Postgres or store).
- Hosts the HTTP server via `http::serve` (axum at the configured port). Only `crawl` mounts the server; `seed` / `validate` / `version` exit without it.
- Runs the maintenance loop via `maintenance::run` (periodic gauge refresh + heartbeat).
- Composes the outbox publisher in the runtime layer when `runtime.linkDispatch = "durable_outbox"`.
- Listens for SIGTERM via `shutdown::wait_for_signal`.
- Orchestrates the long-running lifecycle via `run::crawl` and the one-shot bootstrap via `seed::seed`.

Anything that's a domain rule lives elsewhere.
