# Watchtower

High-performance gRPC log collection sidecar for Tellurian Corp.

Watchtower runs as a lightweight agent next to your application, collects
structured logs over gRPC, and delivers them to Elasticsearch, OpenSearch, or a
larger Watchtower aggregation instance. Built in Rust for minimal RAM, CPU, and
network overhead.

## Architecture

```
                          gRPC (protobuf + gzip)
┌─────────────┐      ┌─────────────────────────────────────────────┐
│ Application │─────►│                 Watchtower                  │
│             │      │                                             │
│  uses the   │      │  ┌──────────┐    ┌────────┐    ┌────────┐  │
│  client SDK │      │  │  gRPC    │───►│Pipeline│───►│ Sinks  │  │
│             │      │  │  Server  │    │(buffer)│    │(fan-out│  │
└─────────────┘      │  └──────────┘    └───┬────┘    └───┬────┘  │
                     │                      │             │       │
                     │               ┌──────▼──────┐      │       │
                     │               │  Spillover  │      │       │
                     │               │  (disk WAL) │      │       │
                     │               └─────────────┘      │       │
                     │                                    │       │
                     │  HTTP :9091                        │       │
                     │  /healthz /readyz /metrics          │       │
                     └────────────────────────────────────┘       │
                                                                  │
                     ┌────────────────┬───────────────────────────┘
                     ▼                ▼                    ▼
              ┌─────────────┐  ┌─────────────┐   ┌──────────────┐
              │Elasticsearch│  │  OpenSearch  │   │  Watchtower  │
              │ (HTTP bulk) │  │ (HTTP bulk)  │   │  (gRPC fwd)  │
              └─────────────┘  └─────────────┘   └──────────────┘
```

## Features

- **gRPC ingestion** — unary and bidirectional streaming RPCs with gzip
  compression and protobuf encoding
- **Flexible log schema** — typed `KeyValue` attributes, trace/span correlation,
  schema URL for downstream consumers
- **Batched pipeline** — bounded async channel with configurable workers and
  flush intervals
- **Multiple sinks** — fan-out to Elasticsearch, OpenSearch, and upstream
  Watchtower instances simultaneously
- **TLS / mTLS** — optional server-side TLS and mutual TLS with client
  certificate verification
- **Prometheus metrics** — `/metrics` endpoint with counters for records
  received/dropped, sink deliveries, errors, active streams
- **Health probes** — `/healthz` (liveness) and `/readyz` (readiness) for
  Kubernetes
- **Disk spillover** — when the in-memory buffer is full, batches are written to
  a CRC32-checked append-only file and replayed on startup
- **Client SDK** — ergonomic Rust library with `info()`, `warn()`, `error()`
  helpers and typed attribute builders
- **Optimized for sidecar deployment** — release binary is ~4 MB (LTO, stripped,
  single codegen unit), no GC, predictable latency

## Quick Start

### 1. Build

```bash
# Debug build
make build

# Optimized release build (~4 MB binary)
make release
```

### 2. Configure

Copy the example configuration and edit it:

```bash
cp watchtower.example.yaml watchtower.yaml
```

At minimum, configure at least one sink:

```yaml
server:
  listen_addr: "[::]:9090"

sinks:
  - type: watchtower
    target: "aggregator.internal:9090"
```

See [docs/configuration.md](docs/configuration.md) for the full reference.

### 3. Run

```bash
./target/release/watchtower --config watchtower.yaml
```

Or with a custom log level:

```bash
RUST_LOG=debug ./target/release/watchtower --config watchtower.yaml
```

### 4. Send logs from your application

Add `watchtower` as a dependency in your Rust application:

```toml
[dependencies]
watchtower = { path = "../Watchtower" }
```

```rust
use watchtower::client::{WatchtowerClient, ClientBuilder, attr};

#[tokio::main]
async fn main() {
    let mut client = ClientBuilder::new("http://localhost:9090")
        .resource("my-service", "host-1", "instance-abc")
        .connect()
        .await
        .expect("failed to connect to watchtower");

    // Simple log
    client.info("user logged in", vec![
        attr("user_id", "usr_12345"),
        attr("ip", "10.0.0.1"),
    ]).await.unwrap();

    // With different severity levels
    client.error("payment failed", vec![
        attr("order_id", "ord_999"),
        attr("amount_cents", 4999i64),
        attr("retry", true),
    ]).await.unwrap();
}
```

See [docs/client-sdk.md](docs/client-sdk.md) for the full SDK guide.

## Endpoints

| Endpoint | Port | Description |
|----------|------|-------------|
| gRPC `Ingest` | 9090 | Unary batch ingestion |
| gRPC `IngestStream` | 9090 | Bidirectional streaming ingestion |
| `GET /healthz` | 9091 | Liveness probe — always returns `200 OK` |
| `GET /readyz` | 9091 | Readiness probe — `200` when accepting traffic, `503` during startup/shutdown |
| `GET /metrics` | 9091 | Prometheus metrics in text exposition format |

## Project Structure

```
.
├── proto/
│   └── watchtower.proto        # Protobuf schema + gRPC service definition
├── src/
│   ├── main.rs                 # CLI entrypoint, TLS setup, signal handling
│   ├── lib.rs                  # Module exports
│   ├── config/mod.rs           # YAML configuration with defaults
│   ├── server/mod.rs           # gRPC WatchtowerService implementation
│   ├── pipeline/mod.rs         # Bounded channel + worker fan-out
│   ├── sink/
│   │   ├── mod.rs              # Sink trait definition
│   │   ├── elastic.rs          # Elasticsearch/OpenSearch HTTP bulk sink
│   │   └── forward.rs          # Upstream Watchtower gRPC forwarding
│   ├── metrics.rs              # Prometheus counters and gauges
│   ├── health.rs               # HTTP health/readiness/metrics server
│   ├── spillover.rs            # Disk-backed overflow buffer
│   └── client/mod.rs           # Application-side client SDK
├── tests/
│   └── integration_test.rs     # gRPC integration tests
├── docs/
│   ├── configuration.md        # Full configuration reference
│   ├── client-sdk.md           # Client SDK usage guide
│   └── deployment.md           # Docker and Kubernetes deployment
├── Cargo.toml
├── build.rs                    # Protobuf code generation
├── Makefile
└── watchtower.example.yaml     # Example configuration
```

## Testing

```bash
# Run all tests
make test

# Run with output
cargo test -- --nocapture
```

## License

MIT — Tellurian Corp
