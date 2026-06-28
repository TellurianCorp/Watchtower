# Configuration Reference

Watchtower is configured via a YAML file (default: `watchtower.yaml`). All
fields have sensible defaults for running as a lightweight sidecar.

```bash
watchtower --config /etc/watchtower/watchtower.yaml
```

## Full Example

```yaml
server:
  listen_addr: "[::]:9090"
  max_recv_msg_size: 4194304
  max_concurrent_streams: 100
  keepalive_interval: "30s"
  keepalive_timeout: "10s"
  enable_compression: true
  tls_cert: "/etc/watchtower/server.crt"
  tls_key: "/etc/watchtower/server.key"
  tls_ca: "/etc/watchtower/ca.crt"

pipeline:
  batch_size: 1024
  flush_interval: "2s"
  buffer_size: 8192
  workers: 2

health:
  enabled: true
  listen_addr: "[::]:9091"

spillover:
  enabled: true
  path: "/var/lib/watchtower/spillover.bin"

sinks:
  - type: watchtower
    target: "aggregator.internal:9090"
    enable_compression: true
    timeout: "10s"
    retry_attempts: 3
    retry_backoff: "1s"

  - type: elasticsearch
    addresses:
      - "https://es-node-1:9200"
      - "https://es-node-2:9200"
    index: "watchtower-logs"
    username: "elastic"
    password: "changeme"
    tls: true
    batch_size: 512
    flush_interval: "5s"
    retry_attempts: 3
    retry_backoff: "1s"
```

---

## `server`

Controls the gRPC listener.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen_addr` | string | `[::]:9090` | Address and port to bind the gRPC server |
| `max_recv_msg_size` | integer | `4194304` (4 MB) | Maximum inbound message size in bytes |
| `max_concurrent_streams` | integer | `100` | Maximum concurrent gRPC streams per connection |
| `keepalive_interval` | duration | `30s` | How often to send keepalive pings to clients |
| `keepalive_timeout` | duration | `10s` | How long to wait for a keepalive ack before closing |
| `enable_compression` | bool | `true` | Accept and send gzip-compressed gRPC messages |
| `tls_cert` | string | _(none)_ | Path to PEM-encoded server certificate. Enables TLS when set |
| `tls_key` | string | _(none)_ | Path to PEM-encoded private key. Required if `tls_cert` is set |
| `tls_ca` | string | _(none)_ | Path to PEM-encoded CA certificate. Enables mTLS (client cert verification) |

### Duration format

All duration fields accept human-readable strings:

- `"100ms"` — milliseconds
- `"5s"` — seconds
- `"2m"` — minutes
- `"30"` — interpreted as seconds

### TLS modes

| tls_cert | tls_key | tls_ca | Mode |
|----------|---------|--------|------|
| unset | unset | unset | Plaintext (no TLS) |
| set | set | unset | Server-side TLS (clients don't need certificates) |
| set | set | set | Mutual TLS (clients must present a certificate signed by the CA) |

---

## `pipeline`

Controls batching and buffering between ingestion and sink delivery.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size` | integer | `1024` | Maximum records per batch sent to sinks. When the internal buffer accumulates this many records, a flush is triggered |
| `flush_interval` | duration | `2s` | Maximum time a partial batch can sit before being flushed. Ensures low-volume logs aren't delayed indefinitely |
| `buffer_size` | integer | `8192` | Bounded channel capacity (number of batches). This is the backpressure threshold — when full, new batches are either spilled to disk or dropped |
| `workers` | integer | `2` | Number of concurrent goroutines pulling from the buffer and delivering to sinks. Increase for high-throughput deployments |

### Tuning guidelines

| Scenario | Recommendation |
|----------|---------------|
| Low-volume sidecar (< 1K logs/sec) | `batch_size: 256`, `workers: 1`, `buffer_size: 1024` |
| Medium-volume (1K–10K logs/sec) | `batch_size: 1024`, `workers: 2`, `buffer_size: 8192` (defaults) |
| High-volume (> 10K logs/sec) | `batch_size: 4096`, `workers: 4`, `buffer_size: 16384` |
| Aggregator instance | `batch_size: 8192`, `workers: 8`, `buffer_size: 65536` |

---

## `health`

HTTP server for Kubernetes probes and Prometheus metrics.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable the health/metrics HTTP server |
| `listen_addr` | string | `[::]:9091` | Address and port for the HTTP server |

### Endpoints

| Path | Method | Description |
|------|--------|-------------|
| `/healthz` | GET | Liveness probe. Always returns `200 OK` if the process is running |
| `/readyz` | GET | Readiness probe. Returns `200` after the gRPC server starts accepting connections, `503` during startup or shutdown |
| `/metrics` | GET | Prometheus metrics in text exposition format |

### Available metrics

| Metric | Type | Description |
|--------|------|-------------|
| `watchtower_records_received_total` | counter | Total log records received via gRPC |
| `watchtower_batches_received_total` | counter | Total batches received via gRPC |
| `watchtower_records_dropped_total` | counter | Records dropped due to backpressure (when spillover is disabled or fails) |
| `watchtower_pipeline_buffer_used` | gauge | Current pipeline buffer occupancy |
| `watchtower_pipeline_flushes_total` | counter | Total pipeline flushes to sinks |
| `watchtower_sink_records_sent_total` | counter | Records successfully sent, labeled by `sink` |
| `watchtower_sink_errors_total` | counter | Delivery errors, labeled by `sink` |
| `watchtower_sink_retries_total` | counter | Retry attempts, labeled by `sink` |
| `watchtower_active_streams` | gauge | Currently active gRPC streaming connections |
| `watchtower_grpc_errors_total` | counter | gRPC-level errors (stream receive failures, etc.) |

---

## `spillover`

Disk-backed buffer for crash resilience. When the in-memory pipeline channel is
full, batches are written to this file instead of being dropped.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable disk spillover |
| `path` | string | `/var/lib/watchtower/spillover.bin` | Path to the spillover file. Parent directories are created automatically |

### How it works

1. The pipeline channel is full and a new batch arrives
2. Instead of dropping, the batch is serialized (protobuf), CRC32-checksummed,
   and appended to the spillover file
3. On next startup, any pending spillover records are replayed back into the
   pipeline before the gRPC server starts accepting
4. After a full successful replay, the file is truncated

### Considerations

- The spillover file grows unbounded while backpressure persists. Monitor
  disk usage in production
- Each record is individually CRC32-checked; corrupt records are skipped during
  replay
- I/O is synchronous (append + flush) to guarantee durability. This adds ~1ms
  per spilled batch on SSD

---

## `sinks`

Array of downstream delivery targets. Watchtower fans out every batch to **all**
configured sinks. You can mix sink types.

### Sink type: `elasticsearch` / `opensearch`

Both use the same HTTP Bulk API (`/_bulk`). The configuration is identical.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | — | `"elasticsearch"` or `"opensearch"` |
| `addresses` | string[] | — | **Required.** List of node URLs (e.g., `https://es:9200`). Requests are round-robin load balanced |
| `index` | string | `watchtower-logs` | Target index name |
| `username` | string | _(none)_ | Basic auth username |
| `password` | string | _(none)_ | Basic auth password |
| `tls` | bool | `false` | When `true`, validates server TLS certificates. When `false`, accepts any certificate (for self-signed dev environments) |
| `batch_size` | integer | `512` | Records per HTTP bulk request |
| `flush_interval` | duration | `5s` | Max time before flushing a partial bulk request |
| `retry_attempts` | integer | `3` | Number of retries on failure (exponential backoff) |
| `retry_backoff` | duration | `1s` | Base delay for exponential backoff (1s, 2s, 4s, ...) |

#### Index mapping

Records are indexed as JSON documents with this structure:

```json
{
  "@timestamp": "2024-11-15T10:30:00.123456789Z",
  "severity": 9,
  "body": "user logged in",
  "resource": {
    "service_name": "auth-service",
    "host_name": "host-1",
    "instance_id": "abc-123"
  },
  "attributes": {
    "user_id": "usr_12345",
    "ip": "10.0.0.1"
  },
  "trace_id": "0af7651916cd43dd8448eb211c80319c",
  "span_id": "b7ad6b7169203331",
  "schema_url": ""
}
```

### Sink type: `watchtower`

Forwards batches to a larger upstream Watchtower instance via the same gRPC
`WatchtowerService.Ingest` RPC. Used for aggregation topologies.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | — | `"watchtower"` |
| `target` | string | — | **Required.** Address of the upstream Watchtower (e.g., `aggregator:9090`) |
| `enable_compression` | bool | `true` | Send gzip-compressed gRPC requests |
| `timeout` | duration | `10s` | Per-RPC timeout |
| `tls_cert` | string | _(none)_ | Path to client TLS certificate (for mTLS to upstream) |
| `tls_ca` | string | _(none)_ | Path to CA certificate for verifying the upstream server |
| `retry_attempts` | integer | `3` | Number of retries on failure |
| `retry_backoff` | duration | `1s` | Base delay for exponential backoff |

---

## `viewer`

The built-in log viewer stores incoming logs in an embedded SQLite database and
serves a browser UI plus a JSON API for searching them. When the viewer is
enabled, Watchtower can run with **no external sinks at all** — useful for
development, small deployments, and environments where Elasticsearch or
OpenSearch is unavailable.

> **Validation rule:** at least one sink **or** `viewer.enabled: true` must be
> set. Watchtower refuses to start if both are absent, because logs would be
> silently discarded.

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Start the viewer HTTP server and SQLite store |
| `listen_addr` | string | `127.0.0.1:9092` | TCP address to bind the viewer. Defaults to localhost — expose deliberately if network access is needed |
| `db_path` | string | `:memory:` | SQLite database path. `:memory:` is ephemeral (lost on restart); a file path (e.g. `/var/lib/watchtower/logs.db`) persists across restarts with WAL mode enabled |
| `retention.max_records` | integer | `1000000` | Maximum rows to keep. When exceeded, the oldest rows are trimmed |
| `retention.max_age` | duration | `7d` | Rows older than this are deleted. Accepts `ms`/`s`/`m`/`h`/`d` suffixes or a bare integer (seconds) — e.g. `"48h"`, `"30d"`, `"90m"` |
| `auth.username` | string | _(none)_ | HTTP Basic auth username. Auth is disabled when `auth` is omitted |
| `auth.password` | string | _(none)_ | HTTP Basic auth password |

### Endpoints

All viewer endpoints are served on `listen_addr` (default port 9092).

| Path | Method | Description |
|------|--------|-------------|
| `/` | GET | Browser web UI — live-tail view with severity filters, service filter, and full-text search |
| `/api/logs` | GET | JSON log query API. Returns `{ records, oldest_id, newest_id }` |
| `/api/services` | GET | JSON list of distinct service names seen in the store. Returns `{ services: [...] }` |

#### `/api/logs` query parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `q` | string | Full-text substring match on the log body |
| `min_severity` | string or int | Minimum severity. Accepts names (`TRACE`, `DEBUG`, `INFO`, `WARN`, `WARNING`, `ERROR`, `FATAL`) or the raw integer severity value |
| `service` | string | Exact match on `resource.service_name` |
| `since` | duration | Return only logs from the last N duration (e.g. `"1h"`, `"30m"`, `"2d"`). Anchored to the request time |
| `after_id` | integer | Return only records with `id > after_id`, ascending order (live-tail polling) |
| `before_id` | integer | Return only records with `id < before_id`, descending order (paginate backwards) |
| `limit` | integer | Maximum records to return. Default `100`, maximum `1000` |

When `after_id` is set, results are ordered ascending by `id` (oldest of the new batch first). Otherwise results are ordered descending (newest first). Combine `oldest_id` / `newest_id` from the response with `before_id` / `after_id` for efficient pagination and live tailing.

#### Severity integer values

| Name | Integer |
|------|---------|
| TRACE | 1 |
| DEBUG | 5 |
| INFO | 9 |
| WARN | 13 |
| ERROR | 17 |
| FATAL | 21 |

### `:memory:` vs file path

| Mode | `db_path` value | Durability | WAL enabled |
|------|-----------------|------------|-------------|
| Ephemeral | `:memory:` | Lost on restart | No |
| Durable | `/path/to/logs.db` | Persists across restarts | Yes (WAL + NORMAL sync) |

Use `:memory:` for development or short-lived containers. Use a file path for
any environment where log history must survive a restart.

### Run with no Elasticsearch (minimal config)

```yaml
server:
  listen_addr: "[::]:9090"
viewer:
  enabled: true
  db_path: "/var/lib/watchtower/logs.db"
```

No `sinks` key is needed. All ingested logs are stored in SQLite and browsable
at `http://127.0.0.1:9092`.

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Controls log verbosity for the Watchtower agent itself. Examples: `info` (default), `debug`, `watchtower=debug,tower=warn` |

---

## Minimal Configurations

### Sidecar forwarding to aggregator

```yaml
server:
  listen_addr: "[::]:9090"
sinks:
  - type: watchtower
    target: "aggregator.internal:9090"
```

### Direct to Elasticsearch

```yaml
server:
  listen_addr: "[::]:9090"
sinks:
  - type: elasticsearch
    addresses: ["http://localhost:9200"]
```

### Fan-out to both

```yaml
server:
  listen_addr: "[::]:9090"
sinks:
  - type: watchtower
    target: "aggregator.internal:9090"
  - type: elasticsearch
    addresses: ["https://es.internal:9200"]
    username: "elastic"
    password: "changeme"
    tls: true
```
