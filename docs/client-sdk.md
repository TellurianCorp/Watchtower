# Client SDK Guide

The Watchtower client SDK is a Rust library for sending structured logs to a
local Watchtower sidecar. It handles connection management, gzip compression,
protobuf serialization, and provides an ergonomic API.

## Installation

Add Watchtower as a dependency in your `Cargo.toml`:

```toml
[dependencies]
watchtower = { path = "../Watchtower" }
tokio = { version = "1", features = ["full"] }
```

In the future this will be published as a crate. For now, use a path or git
dependency.

## Quick Start

```rust
use watchtower::client::{WatchtowerClient, ClientBuilder, attr};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the local Watchtower sidecar.
    let mut client = WatchtowerClient::connect("http://localhost:9090").await?;

    // Send a log.
    client.info("request completed", vec![
        attr("method", "GET"),
        attr("path", "/api/users"),
        attr("status", 200i64),
        attr("duration_ms", 42.5f64),
    ]).await?;

    Ok(())
}
```

## API Reference

### Connecting

#### Simple connect

```rust
let mut client = WatchtowerClient::connect("http://localhost:9090").await?;
```

#### Builder pattern (full control)

```rust
use std::time::Duration;
use watchtower::client::ClientBuilder;

let mut client = ClientBuilder::new("http://localhost:9090")
    .resource("my-service", "host-1", "instance-abc")
    .timeout(Duration::from_secs(5))
    .compression(true)
    .connect()
    .await?;
```

| Builder method | Description |
|---------------|-------------|
| `resource(service, host, instance)` | Attach identity to every log record. Strongly recommended |
| `timeout(duration)` | Per-RPC timeout (default: 5s) |
| `compression(bool)` | Enable/disable gzip (default: true) |

### Sending Logs

#### Convenience methods

```rust
client.trace("entering function", vec![attr("fn", "process_order")]).await?;
client.debug("cache hit", vec![attr("key", "user:123")]).await?;
client.info("order created", vec![attr("order_id", "ord_456")]).await?;
client.warn("rate limit approaching", vec![attr("current", 980i64)]).await?;
client.error("payment failed", vec![attr("reason", "card_declined")]).await?;
client.fatal("database unreachable", vec![]).await?;
```

Each method:
1. Creates a `LogRecord` with the current timestamp
2. Sets the appropriate severity level
3. Attaches the resource identity (if configured via builder)
4. Sends a single-record batch to the sidecar
5. Returns an `IngestResponse` with the accepted count

#### Generic log method

```rust
use watchtower::proto::Severity;

client.log(Severity::Info, "custom message", vec![
    attr("key", "value"),
]).await?;
```

#### Batch send (high throughput)

For high-volume logging, build records manually and send them in bulk:

```rust
use watchtower::proto::{LogRecord, Severity};
use watchtower::client::attr;

let records: Vec<LogRecord> = events.iter().map(|event| {
    LogRecord {
        timestamp: Some(prost_types::Timestamp {
            seconds: event.timestamp_secs,
            nanos: 0,
        }),
        severity: Severity::Info as i32,
        body: event.message.clone(),
        attributes: vec![
            attr("event_type", event.event_type.as_str()),
            attr("user_id", event.user_id.as_str()),
        ],
        ..Default::default()
    }
}).collect();

let response = client.log_batch(records).await?;
println!("accepted: {}", response.accepted_count);
```

### Building Attributes

The `attr()` helper function creates typed `KeyValue` attributes without
boilerplate:

```rust
use watchtower::client::attr;

// String
attr("user_id", "usr_123")
attr("user_id", String::from("usr_123"))

// Integer
attr("status_code", 200i64)

// Float
attr("duration_ms", 42.5f64)

// Boolean
attr("is_admin", true)
```

Supported types via the `AttrValue` trait:

| Rust type | Protobuf type |
|-----------|---------------|
| `&str` | `string_value` |
| `String` | `string_value` |
| `i64` | `int_value` |
| `f64` | `double_value` |
| `bool` | `bool_value` |

For complex types (arrays, nested maps, bytes), construct `KeyValue` directly:

```rust
use watchtower::proto::{AnyValue, KeyValue, any_value};

let kv = KeyValue {
    key: "raw_data".into(),
    value: Some(AnyValue {
        value: Some(any_value::Value::BytesValue(vec![0x01, 0x02, 0x03])),
    }),
};
```

### Error Handling

The client returns `ClientError` for all failures:

```rust
use watchtower::client::ClientError;

match client.info("msg", vec![]).await {
    Ok(response) => {
        println!("accepted: {}", response.accepted_count);
        if !response.errors.is_empty() {
            // Partial failure — some records were rejected.
            for (idx, msg) in &response.errors {
                eprintln!("record {idx} rejected: {msg}");
            }
        }
    }
    Err(ClientError::Connection(msg)) => {
        eprintln!("cannot reach watchtower: {msg}");
    }
    Err(ClientError::Rpc(msg)) => {
        // Server responded with an error (e.g., pipeline full).
        eprintln!("server error: {msg}");
    }
}
```

| Error variant | When it happens |
|--------------|-----------------|
| `ClientError::Connection` | Cannot establish or maintain a gRPC connection |
| `ClientError::Rpc` | Server returned a gRPC error status (e.g., `RESOURCE_EXHAUSTED` when the pipeline buffer is full) |

## Best Practices

### 1. Create one client per process

The client holds a gRPC channel with connection pooling and keepalive. Creating
one at startup and reusing it is the most efficient pattern:

```rust
lazy_static::lazy_static! {
    static ref WATCHTOWER: tokio::sync::Mutex<WatchtowerClient> = {
        // Initialize in main() and store globally.
        todo!()
    };
}
```

### 2. Always set a resource

The resource identifies *which* service/host/instance produced the log. Without
it, logs in Elasticsearch are hard to filter:

```rust
ClientBuilder::new("http://localhost:9090")
    .resource("payment-service", &hostname, &instance_id)
    .connect()
    .await?;
```

### 3. Use structured attributes, not string interpolation

```rust
// Bad — hard to query in Elasticsearch
client.info(format!("user {} logged in from {}", user_id, ip), vec![]).await?;

// Good — searchable, aggregatable
client.info("user logged in", vec![
    attr("user_id", user_id),
    attr("ip", ip),
]).await?;
```

### 4. Batch when possible

If you're logging inside a loop, collect records and send them in one batch
rather than one-at-a-time:

```rust
let records: Vec<LogRecord> = items.iter().map(|item| {
    // build record...
}).collect();

client.log_batch(records).await?;
```

### 5. Handle backpressure gracefully

When the Watchtower pipeline is full, the server returns `RESOURCE_EXHAUSTED`.
Your application should decide whether to retry, buffer locally, or drop:

```rust
match client.info("msg", attrs).await {
    Err(ClientError::Rpc(msg)) if msg.contains("RESOURCE_EXHAUSTED") => {
        // Pipeline full — log locally or retry later.
        eprintln!("watchtower backpressure, logging locally");
    }
    other => { /* handle normally */ }
}
```

## Severity Levels

| Level | Value | Use for |
|-------|-------|---------|
| `Trace` | 1 | Fine-grained debugging (function entry/exit) |
| `Debug` | 5 | Diagnostic information (cache hits, query plans) |
| `Info` | 9 | Normal operations (request completed, job started) |
| `Warn` | 13 | Unexpected but recoverable (rate limit approaching, retrying) |
| `Error` | 17 | Failures requiring attention (payment declined, API error) |
| `Fatal` | 21 | Unrecoverable errors (database unreachable, OOM) |

Values are aligned with OpenTelemetry severity numbers.
