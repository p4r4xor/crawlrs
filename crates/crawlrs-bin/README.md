# crawlrs-bin

The `crawlrs` CLI binary. Wires every workspace crate together into
a runnable process.

## Quick start (sandbox)

Spin up Redis + Postgres + MinIO via docker-compose, then:

```bash
# Copy the sample config
cp crates/crawlrs-bin/examples/crawl.toml ./crawl.toml
# Edit endpoints / credentials to match your local services
$EDITOR crawl.toml

# Validate the config without starting workers
cargo run -p crawlrs-bin -- validate --config crawl.toml

# Start the crawler with a seeds file
echo "https://example.com" > seeds.txt
cargo run -p crawlrs-bin -- crawl --config crawl.toml --seeds seeds.txt
```

The binary mounts an HTTP server at `0.0.0.0:9090` (configurable in
`crawl.toml`) exposing four endpoints:

- `/metrics` - Prometheus exposition format (29 metrics).
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
  crawl     Start the crawler. Loads the config, builds the runtime,
            mounts the HTTP host, and runs until SIGTERM.
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
| `CRAWLRS_SEEDS`      | `--seeds` path |
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

- Parses CLI args via `cli::Cli` (clap-derive).
- Loads + validates `crawl.toml` via `config::CrawlrsConfig`.
- Builds every adapter via `factory::build` (one place that knows
  about every adapter crate).
- Hosts the HTTP server via `http::serve` (axum at the configured
  port).
- Runs the maintenance loop via `maintenance::run` (15s cadence).
- Listens for SIGTERM via `shutdown::wait_for_signal`.
- Orchestrates the lifecycle via `run::crawl`.

Anything that's a domain rule lives elsewhere.
