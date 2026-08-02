# crawlrs-bin

The `crawlrs` CLI binary. Wires every workspace crate together into a runnable process.

## Quick start (sandbox)

Spin up Valkey + Postgres (via `docker run` or your preferred orchestration), point the crawler at a local-FS store path, then:

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

## HTTP endpoints

The binary starts an HTTP server at `0.0.0.0:9090` (configurable in `crawl.toml`) with four endpoints:

- `/metrics` - Prometheus exposition format covering fetch, parse, store, frontier, politeness, and metadata. This is what vmsingle scrapes.
- `/healthz` - always 200 if the binary is running. Useful as a basic reachability check.
- `/livez` - 200 unless the worker pool has deadlocked. Wired to the Kubernetes liveness probe.
- `/readyz` - 200 once all adapters are connected; flips to 503 during shutdown so load balancers stop sending traffic before workers drain. Wired to the readiness probe.

## CLI

```text
crawlrs <COMMAND>

Commands:
  crawl     Start the worker pool. Loads the config, builds the
            runtime, mounts the HTTP host, and runs until SIGTERM.
  seed      Load URLs from a file into the Frontier and exit.
            Tolerant of per-batch failures. Runs once via a Helm
            post-install Job.
  validate  Parse the config file and print a one-line summary.
            Exits non-zero on structural errors.
  version   Print the binary version.
```

## Environment variables

These let you override config values via env vars, which is how the Helm chart's ConfigMap and Secret inject connection strings at deploy time.

| Var | What it overrides |
|---|---|
| `CRAWLRS_CONFIG` | `--config` path |
| `CRAWLRS_SEEDS` | `crawlrs seed --path` |
| `CRAWLRS_RUN_ID` | top-level `run_id` in crawl.toml |
| `CRAWLRS_REDIS_URL` | `[redis].url` |
| `CRAWLRS_POSTGRES_URL` | `[postgres].url` |
| `CRAWLRS_LISTEN` | `[server].listen` (HTTP bind address) |
| `CRAWLRS_WORKERS` | `[runtime].workers` (async workers per pod) |
| `CRAWLRS_LOG_FORMAT` | `text` (default, human-readable) or `json` (structured, for log aggregators) |
| `RUST_LOG` | per-crate verbosity (e.g. `crawlrs_runtime=debug,info`) |
| `POD_NAME` | derives the pod ordinal for shard ownership in StatefulSet deploys |
| `CRAWLRS_REPLICAS` | replica count for shard-ownership math |

## Shutdown

SIGTERM (or Ctrl-C) triggers graceful shutdown:

1. `/readyz` flips to 503 so load balancers stop routing traffic.
2. 5-second drain delay for in-flight scrapes and probes to land.
3. Worker pool stops accepting new claims.
4. In-flight URLs finish processing (frontier ack/nack).
5. Store buffers flush.
6. Process exits.

## Configuration

See `examples/crawl.toml` for the fully-commented template. Every section has sensible defaults; the only required keys are `run_id`, `[redis].url`, and `[postgres].url`.
