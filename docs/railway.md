# Deploying Watchtower on Railway

Railway is a cloud platform that makes deploying containers simple. Watchtower
supports Railway natively — no config file needed. Just set environment variables
in the Railway dashboard and deploy.

## Quick Deploy

### 1. Create a new project

1. Go to [railway.app](https://railway.app) and create a new project
2. Choose **Deploy from GitHub repo**
3. Select the Watchtower repository

Railway will auto-detect the `Dockerfile` and `railway.toml`.

### 2. Set environment variables

In the Railway dashboard, go to your service's **Variables** tab and add:

**Required** — at least one sink:

```
WATCHTOWER_SINK_TYPE=elasticsearch
WATCHTOWER_SINK_ADDRESSES=https://your-elasticsearch:9200
WATCHTOWER_SINK_USERNAME=elastic
WATCHTOWER_SINK_PASSWORD=your-password
WATCHTOWER_SINK_TLS=true
```

Or forward to an upstream Watchtower aggregator:

```
WATCHTOWER_SINK_TYPE=watchtower
WATCHTOWER_SINK_TARGET=your-aggregator.railway.internal:9090
```

**Optional** tuning:

```
WATCHTOWER_WORKERS=2
WATCHTOWER_BATCH_SIZE=1024
WATCHTOWER_BUFFER_SIZE=8192
WATCHTOWER_FLUSH_INTERVAL=2s
WATCHTOWER_HEALTH_PORT=9091
RUST_LOG=info
```

### 3. Deploy

Click **Deploy**. Railway will:

1. Build the Rust binary via the multi-stage Dockerfile
2. Start the container
3. Inject the `PORT` environment variable (Watchtower listens on it automatically)
4. TCP health check confirms the gRPC port is accepting connections

Your Watchtower instance is now receiving logs on the Railway-assigned port.

### 4. Connect your application

Use the Railway-provided internal URL to connect your app:

```rust
use watchtower::client::ClientBuilder;

let mut client = ClientBuilder::new("http://watchtower.railway.internal:PORT")
    .resource("my-service", "railway", "instance-1")
    .connect()
    .await?;

client.info("app started", vec![]).await?;
```

Or from any gRPC client using the public URL that Railway provides.

---

## Environment Variable Reference

### Server

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `9090` | gRPC listen port (set automatically by Railway) |
| `WATCHTOWER_HEALTH_PORT` | `9091` | Health/metrics HTTP port |
| `WATCHTOWER_HEALTH_ENABLED` | `true` | Enable health/metrics server |
| `RUST_LOG` | `info` | Log level (`debug`, `info`, `warn`, `error`) |

### Pipeline

| Variable | Default | Description |
|----------|---------|-------------|
| `WATCHTOWER_WORKERS` | `2` | Pipeline worker count |
| `WATCHTOWER_BATCH_SIZE` | `1024` | Records per batch to sinks |
| `WATCHTOWER_BUFFER_SIZE` | `8192` | Pipeline channel capacity |
| `WATCHTOWER_FLUSH_INTERVAL` | `2s` | Max flush delay (`5s`, `100ms`, `2m`) |

### Spillover

| Variable | Default | Description |
|----------|---------|-------------|
| `WATCHTOWER_SPILLOVER_ENABLED` | `false` | Enable disk spillover |
| `WATCHTOWER_SPILLOVER_PATH` | `/var/lib/watchtower/spillover.bin` | Spillover file path |

### Built-in viewer

Enable the embedded SQLite log viewer (web UI + JSON API) so you can browse logs
in a browser without Elasticsearch. When enabled, it runs as an additional sink
alongside any others. Expose `WATCHTOWER_VIEWER_PORT` via a separate Railway
domain to reach the UI, and set basic auth before exposing it publicly.

| Variable | Default | Description |
|----------|---------|-------------|
| `WATCHTOWER_VIEWER_ENABLED` | `false` | Enable the built-in viewer |
| `WATCHTOWER_VIEWER_PORT` | `9092` | Viewer HTTP port; binds `[::]:PORT` (reachable behind the Railway proxy) |
| `WATCHTOWER_VIEWER_LISTEN_ADDR` | _(unset)_ | Full bind address; overrides `WATCHTOWER_VIEWER_PORT` |
| `WATCHTOWER_VIEWER_DB_PATH` | `:memory:` | `:memory:` (live logs since last restart) or a file path (durable — put it on a Railway volume) |
| `WATCHTOWER_VIEWER_MAX_RECORDS` | `1000000` | Retention: trim oldest rows beyond this |
| `WATCHTOWER_VIEWER_MAX_AGE` | `7d` | Retention: delete rows older than this (`30m`, `12h`, `7d`) |
| `WATCHTOWER_VIEWER_AUTH_USERNAME` | _(unset)_ | HTTP basic auth username (set with password to require auth) |
| `WATCHTOWER_VIEWER_AUTH_PASSWORD` | _(unset)_ | HTTP basic auth password |

> The viewer is **off** by default. With `:memory:`, logs are lost on every
> redeploy/restart — attach a Railway volume and point `WATCHTOWER_VIEWER_DB_PATH`
> at it for durability.

### Sink (single)

Use the `WATCHTOWER_SINK_*` prefix for a single sink:

| Variable | Description |
|----------|-------------|
| `WATCHTOWER_SINK_TYPE` | `elasticsearch`, `opensearch`, or `watchtower` |
| `WATCHTOWER_SINK_ADDRESSES` | Comma-separated URLs (ES/OS only) |
| `WATCHTOWER_SINK_INDEX` | Index name (default: `watchtower-logs`) |
| `WATCHTOWER_SINK_USERNAME` | Basic auth username (ES/OS only) |
| `WATCHTOWER_SINK_PASSWORD` | Basic auth password (ES/OS only) |
| `WATCHTOWER_SINK_TLS` | `true`/`false` — validate TLS certs (ES/OS only) |
| `WATCHTOWER_SINK_TARGET` | Upstream address (watchtower forward only) |
| `WATCHTOWER_SINK_COMPRESSION` | `true`/`false` — gzip (watchtower forward only) |
| `WATCHTOWER_SINK_TIMEOUT` | RPC timeout (watchtower forward only) |
| `WATCHTOWER_SINK_BATCH_SIZE` | Records per bulk request (ES/OS only) |
| `WATCHTOWER_SINK_FLUSH_INTERVAL` | Bulk flush interval (ES/OS only) |
| `WATCHTOWER_SINK_RETRY_ATTEMPTS` | Retry count on failure |

### Sinks (multiple)

Use indexed prefixes for multiple sinks: `WATCHTOWER_SINK_0_*`, `WATCHTOWER_SINK_1_*`, etc.

```
WATCHTOWER_SINK_0_TYPE=watchtower
WATCHTOWER_SINK_0_TARGET=aggregator.railway.internal:9090

WATCHTOWER_SINK_1_TYPE=elasticsearch
WATCHTOWER_SINK_1_ADDRESSES=https://es.example.com:9200
WATCHTOWER_SINK_1_USERNAME=elastic
WATCHTOWER_SINK_1_PASSWORD=secret
WATCHTOWER_SINK_1_TLS=true
```

---

## Example Configurations

### Sidecar forwarding to aggregator

```
PORT=9090
WATCHTOWER_SINK_TYPE=watchtower
WATCHTOWER_SINK_TARGET=watchtower-aggregator.railway.internal:9090
```

### Direct to Elasticsearch (Elastic Cloud)

```
PORT=9090
WATCHTOWER_SINK_TYPE=elasticsearch
WATCHTOWER_SINK_ADDRESSES=https://my-deployment.es.us-east-1.aws.elastic.cloud:9243
WATCHTOWER_SINK_USERNAME=elastic
WATCHTOWER_SINK_PASSWORD=my-password
WATCHTOWER_SINK_TLS=true
WATCHTOWER_SINK_INDEX=app-logs
```

### Direct to OpenSearch (AWS)

```
PORT=9090
WATCHTOWER_SINK_TYPE=opensearch
WATCHTOWER_SINK_ADDRESSES=https://search-my-domain.us-east-1.es.amazonaws.com
WATCHTOWER_SINK_USERNAME=admin
WATCHTOWER_SINK_PASSWORD=Admin123!
WATCHTOWER_SINK_TLS=true
```

### Fan-out to aggregator + Elasticsearch

```
PORT=9090
WATCHTOWER_SINK_0_TYPE=watchtower
WATCHTOWER_SINK_0_TARGET=aggregator.railway.internal:9090
WATCHTOWER_SINK_1_TYPE=elasticsearch
WATCHTOWER_SINK_1_ADDRESSES=https://es.example.com:9200
WATCHTOWER_SINK_1_USERNAME=elastic
WATCHTOWER_SINK_1_PASSWORD=secret
WATCHTOWER_SINK_1_TLS=true
```

---

## Railway-Specific Notes

### Port handling

Railway injects the `PORT` environment variable. Watchtower automatically uses
it for the gRPC server. You do **not** need to set `PORT` manually — Railway
handles it. The health server runs on a separate port (`WATCHTOWER_HEALTH_PORT`,
default 9091).

### Health checks

Railway uses a TCP health check against the gRPC port (the `PORT` it injects).
This works because tonic accepts TCP connections immediately on startup.

The HTTP health endpoints (`/healthz`, `/readyz`, `/metrics`) run on a separate
port (default 9091, configurable via `WATCHTOWER_HEALTH_PORT`). These are
designed for Kubernetes probes and Prometheus scraping, not Railway's built-in
health check.

### Private networking

For service-to-service communication within Railway, use the internal DNS:

```
watchtower.railway.internal:<PORT>
```

This avoids public internet roundtrips and is faster.

### Volumes (persistent spillover)

If you need crash-resilient spillover, attach a Railway volume:

1. Go to your service settings in Railway
2. Add a volume mounted at `/var/lib/watchtower`
3. Set `WATCHTOWER_SPILLOVER_ENABLED=true`

### Build caching

Railway caches Docker layers. The first build compiles all Rust dependencies
and takes several minutes. Subsequent builds only recompile your code changes
(much faster).

### Resource recommendations

| Scenario | Railway plan | Notes |
|----------|-------------|-------|
| Sidecar (< 1K logs/sec) | Hobby | 512 MB RAM is plenty |
| Medium (1K–10K logs/sec) | Pro | 1 GB RAM, 1 vCPU |
| Aggregator | Pro | 2+ GB RAM, 2+ vCPU |
