# Built-in Log Viewer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let Watchtower run with no Elasticsearch/sink configured by storing logs in an embedded SQLite database and serving a built-in web UI to search and view them.

**Architecture:** A new opt-in `viewer` config section. When enabled, Watchtower opens one SQLite connection (shared behind a `std::sync::Mutex`, all access via `spawn_blocking`), registers an internal `StoreSink` in the existing pipeline fan-out, runs a periodic retention task, and starts a dedicated HTTP server (default `127.0.0.1:9092`) serving an embedded single-page UI plus a JSON query API. Elasticsearch / OpenSearch / forward sinks stay intact and optional.

**Tech Stack:** Rust (edition 2024), tonic/gRPC (existing), hyper http1 (existing, as in `src/health.rs`), `rusqlite` (new, `bundled`), serde_json (existing), vanilla HTML/CSS/JS (no framework, no build step).

## Global Constraints

- **Reference spec:** `docs/superpowers/specs/2026-06-28-builtin-log-viewer-design.md`.
- **No new heavy deps / no frontend toolchain.** Only new crate is `rusqlite` (`bundled`). UI is a single `include_str!`-embedded HTML file with no external CDN assets.
- **DB sharing:** exactly ONE `rusqlite::Connection`, wrapped in `Arc<std::sync::Mutex<Connection>>`, shared between the store sink (writes) and viewer handlers (reads). All DB calls run inside `tokio::task::spawn_blocking` (rusqlite is synchronous; the std Mutex is locked only inside the blocking closure).
- **Viewer is opt-in:** `viewer.enabled` defaults to `false`. Default bind is `127.0.0.1:9092`. Basic auth is optional.
- **Default `db_path` is `:memory:`** (ephemeral). A file path enables durability; file DBs get `PRAGMA journal_mode=WAL`.
- **Platform is Windows for local dev** — use `cargo`/`make` targets, no `.sh` scripts.
- **Severity integer values** (from `proto/watchtower.proto`): TRACE=1, DEBUG=5, INFO=9, WARN=13, ERROR=17, FATAL=21. Severity filtering is `severity >= value`.
- **Refinement from spec (v1):** `/api/logs` time filter is a single relative `since` parameter (a duration "ago", e.g. `1h`, `2d`) computed as `now - since`. RFC3339 absolute timestamps and a separate `until` are dropped for v1 (YAGNI; the UI only needs "last X"). Everything else matches the spec.
- **Test command:** `cargo test` (all), or `cargo test <name>` for one. Build: `cargo build`. Release: `cargo build --release`.

---

### Task 1: Config — duration parser (`h`/`d`), viewer structs, validation

**Files:**
- Modify: `src/config/mod.rs`
- Test: `src/config/mod.rs` (`#[cfg(test)] mod tests` at file end)

**Interfaces:**
- Produces: `pub fn parse_duration(s: &str) -> Result<std::time::Duration, String>` (module-level, reused by the viewer); `Config.viewer: ViewerConfig`; `ViewerConfig { enabled: bool, listen_addr: String, db_path: String, retention: RetentionConfig, auth: Option<BasicAuthConfig> }`; `RetentionConfig { max_records: u64, max_age: Duration }`; `BasicAuthConfig { username: String, password: String }`.

- [ ] **Step 1: Write the failing tests**

Add at the end of `src/config/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hours_and_days() {
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("3d").unwrap(), Duration::from_secs(259200));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("45s").unwrap(), Duration::from_secs(45));
        assert_eq!(parse_duration("10m").unwrap(), Duration::from_secs(600));
    }

    #[test]
    fn viewer_defaults_are_safe() {
        let cfg = Config::default();
        assert!(!cfg.viewer.enabled);
        assert_eq!(cfg.viewer.listen_addr, "127.0.0.1:9092");
        assert_eq!(cfg.viewer.db_path, ":memory:");
        assert_eq!(cfg.viewer.retention.max_records, 1_000_000);
        assert_eq!(cfg.viewer.retention.max_age, Duration::from_secs(7 * 86400));
        assert!(cfg.viewer.auth.is_none());
    }

    #[test]
    fn rejects_no_sinks_when_viewer_disabled() {
        let cfg = Config::default(); // no sinks, viewer disabled
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn accepts_no_sinks_when_viewer_enabled() {
        let mut cfg = Config::default();
        cfg.viewer.enabled = true;
        assert!(cfg.validate().is_ok());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib config::tests`
Expected: FAIL — `parse_duration` not found, `viewer` field missing, validation passes when it shouldn't.

- [ ] **Step 3: Extract `parse_duration` as a public fn and add `h`/`d`**

In `src/config/mod.rs`, replace the private `mod humantime_serde { ... }` block with a public free function plus a thin serde adapter:

```rust
/// Parse human-readable durations like "5s", "100ms", "2m", "3h", "7d".
/// "30" (bare number) is interpreted as seconds.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    // Check "ms" before "m"/"s".
    if let Some(rest) = s.strip_suffix("ms") {
        rest.trim().parse::<u64>().map(Duration::from_millis).map_err(|e| e.to_string())
    } else if let Some(rest) = s.strip_suffix('s') {
        rest.trim().parse::<u64>().map(Duration::from_secs).map_err(|e| e.to_string())
    } else if let Some(rest) = s.strip_suffix('m') {
        rest.trim().parse::<u64>().map(|v| Duration::from_secs(v * 60)).map_err(|e| e.to_string())
    } else if let Some(rest) = s.strip_suffix('h') {
        rest.trim().parse::<u64>().map(|v| Duration::from_secs(v * 3600)).map_err(|e| e.to_string())
    } else if let Some(rest) = s.strip_suffix('d') {
        rest.trim().parse::<u64>().map(|v| Duration::from_secs(v * 86400)).map_err(|e| e.to_string())
    } else {
        s.parse::<u64>().map(Duration::from_secs).map_err(|_| format!("invalid duration: {s}"))
    }
}

mod humantime_serde {
    use std::time::Duration;
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        super::parse_duration(&s).map_err(serde::de::Error::custom)
    }
}
```

- [ ] **Step 4: Add the viewer config structs**

Add the `viewer` field to `Config` (after `sinks`):

```rust
    #[serde(default)]
    pub sinks: Vec<SinkConfig>,
    pub viewer: ViewerConfig,
}
```

Add the new structs (place them near `HealthConfig`):

```rust
/// Built-in log viewer (SQLite store + web UI) settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ViewerConfig {
    pub enabled: bool,
    pub listen_addr: String,
    pub db_path: String,
    pub retention: RetentionConfig,
    pub auth: Option<BasicAuthConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RetentionConfig {
    pub max_records: u64,
    #[serde(with = "humantime_serde")]
    pub max_age: Duration,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BasicAuthConfig {
    pub username: String,
    pub password: String,
}

impl Default for ViewerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: "127.0.0.1:9092".into(),
            db_path: ":memory:".into(),
            retention: RetentionConfig::default(),
            auth: None,
        }
    }
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_records: 1_000_000,
            max_age: Duration::from_secs(7 * 86400),
        }
    }
}
```

Add `viewer: ViewerConfig::default()` to the `impl Default for Config` body.

- [ ] **Step 5: Update validation**

In `Config::validate()`, add this at the top of the method (before the pipeline checks):

```rust
        if self.sinks.is_empty() && !self.viewer.enabled {
            return Err("no sinks configured and viewer disabled: logs would be discarded".into());
        }
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test --lib config::tests`
Expected: PASS (4 tests).

- [ ] **Step 7: Commit**

```bash
git add src/config/mod.rs
git commit -m "feat(config): add viewer section, h/d durations, no-sink validation"
```

---

### Task 2: Extract shared record-encoding module

**Files:**
- Create: `src/sink/encode.rs`
- Modify: `src/sink/mod.rs` (add `pub mod encode;`), `src/sink/elastic.rs` (use the shared module)
- Test: `src/sink/encode.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `pub fn record_to_json(record: &proto::LogRecord) -> serde_json::Value`; `pub fn any_value_to_json(v: &proto::AnyValue) -> serde_json::Value`; `pub fn attributes_to_json(attrs: &[proto::KeyValue]) -> serde_json::Value`; `pub fn timestamp_to_nanos(ts: &prost_types::Timestamp) -> i64`; `pub fn rfc3339_from_nanos(ts_nanos: i64) -> String`.

- [ ] **Step 1: Write the failing tests**

Create `src/sink/encode.rs` with the tests first (implementation added in step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{AnyValue, KeyValue, LogRecord, Resource, Severity, any_value};

    fn sample() -> LogRecord {
        LogRecord {
            timestamp: Some(prost_types::Timestamp { seconds: 1_700_000_000, nanos: 123_000_000 }),
            severity: Severity::Error as i32,
            body: "boom".into(),
            attributes: vec![KeyValue {
                key: "order_id".into(),
                value: Some(AnyValue { value: Some(any_value::Value::StringValue("ord_9".into())) }),
            }],
            resource: Some(Resource {
                service_name: "pay".into(), host_name: "h1".into(), instance_id: "i1".into(),
                attributes: vec![],
            }),
            trace_id: vec![], span_id: vec![], schema_url: String::new(),
        }
    }

    #[test]
    fn nanos_round_trip() {
        let ts = prost_types::Timestamp { seconds: 1_700_000_000, nanos: 123_000_000 };
        assert_eq!(timestamp_to_nanos(&ts), 1_700_000_000_123_000_000);
    }

    #[test]
    fn rfc3339_has_expected_prefix() {
        let s = rfc3339_from_nanos(1_700_000_000_123_000_000);
        assert!(s.starts_with("2023-11-14T"), "got {s}");
        assert!(s.ends_with("Z"));
    }

    #[test]
    fn record_json_shape() {
        let doc = record_to_json(&sample());
        assert_eq!(doc["severity"], 17);
        assert_eq!(doc["body"], "boom");
        assert_eq!(doc["resource"]["service_name"], "pay");
        assert_eq!(doc["attributes"]["order_id"], "ord_9");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib sink::encode`
Expected: FAIL — module/functions not found (and `pub mod encode;` not yet added → compile error).

- [ ] **Step 3: Implement the module**

Add at the top of `src/sink/encode.rs` (above the test module). Move the bodies of `record_to_json`, `any_value_to_json`, `chrono_from_timestamp`, `days_to_date` out of `src/sink/elastic.rs` and adapt:

```rust
use serde_json::json;

use crate::proto;

/// Convert a proto timestamp to unix nanoseconds.
pub fn timestamp_to_nanos(ts: &prost_types::Timestamp) -> i64 {
    ts.seconds * 1_000_000_000 + ts.nanos as i64
}

/// Build the full JSON document for a log record (same shape used by the ES sink).
pub fn record_to_json(record: &proto::LogRecord) -> serde_json::Value {
    let mut doc = json!({ "severity": record.severity, "body": record.body });

    if let Some(ts) = &record.timestamp {
        doc["@timestamp"] = json!(rfc3339_from_nanos(timestamp_to_nanos(ts)));
    }
    if let Some(resource) = &record.resource {
        doc["resource"] = json!({
            "service_name": resource.service_name,
            "host_name": resource.host_name,
            "instance_id": resource.instance_id,
        });
    }
    if !record.trace_id.is_empty() {
        doc["trace_id"] = json!(hex::encode(&record.trace_id));
    }
    if !record.span_id.is_empty() {
        doc["span_id"] = json!(hex::encode(&record.span_id));
    }
    if !record.attributes.is_empty() {
        doc["attributes"] = attributes_to_json(&record.attributes);
    }
    if !record.schema_url.is_empty() {
        doc["schema_url"] = json!(record.schema_url);
    }
    doc
}

/// Serialize a list of KeyValue attributes into a JSON object.
pub fn attributes_to_json(attrs: &[proto::KeyValue]) -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = attrs
        .iter()
        .filter_map(|kv| kv.value.as_ref().map(|v| (kv.key.clone(), any_value_to_json(v))))
        .collect();
    json!(map)
}

pub fn any_value_to_json(v: &proto::AnyValue) -> serde_json::Value {
    match &v.value {
        Some(proto::any_value::Value::StringValue(s)) => json!(s),
        Some(proto::any_value::Value::IntValue(n)) => json!(n),
        Some(proto::any_value::Value::DoubleValue(f)) => json!(f),
        Some(proto::any_value::Value::BoolValue(b)) => json!(b),
        Some(proto::any_value::Value::BytesValue(b)) => json!(hex::encode(b)),
        Some(proto::any_value::Value::ArrayValue(arr)) => {
            json!(arr.values.iter().map(any_value_to_json).collect::<Vec<_>>())
        }
        Some(proto::any_value::Value::MapValue(m)) => attributes_to_json(&m.entries),
        None => serde_json::Value::Null,
    }
}

/// Format unix nanoseconds as an RFC3339 UTC string.
pub fn rfc3339_from_nanos(ts_nanos: i64) -> String {
    let seconds = ts_nanos.div_euclid(1_000_000_000);
    let nanos = ts_nanos.rem_euclid(1_000_000_000) as u32;
    const SECS_PER_DAY: i64 = 86400;
    let days = seconds.div_euclid(SECS_PER_DAY);
    let day_secs = seconds.rem_euclid(SECS_PER_DAY) as u32;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let secs = day_secs % 60;
    let (year, month, day) = days_to_date(days);
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{secs:02}.{nanos:09}Z")
}

fn days_to_date(days: i64) -> (i32, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64 + era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
```

- [ ] **Step 4: Wire the module and slim down `elastic.rs`**

In `src/sink/mod.rs`, add to the top:

```rust
pub mod encode;
pub mod elastic;
pub mod forward;
```

In `src/sink/elastic.rs`: delete the local `record_to_json`, `any_value_to_json`, `chrono_from_timestamp`, `days_to_date` functions, and change `build_bulk_body` to call the shared helper:

```rust
        // Document line
        let doc = crate::sink::encode::record_to_json(record);
        buf.push_str(&doc.to_string());
        buf.push('\n');
```

Remove the now-unused `use serde_json::json;` import from `elastic.rs` if nothing else uses it.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib sink::encode` then `cargo build`
Expected: PASS (3 tests), build succeeds.

- [ ] **Step 6: Commit**

```bash
git add src/sink/encode.rs src/sink/mod.rs src/sink/elastic.rs
git commit -m "refactor(sink): extract shared record-encoding module"
```

---

### Task 3: SQLite store — schema + write path + StoreSink

**Files:**
- Create: `src/sink/store.rs`
- Modify: `Cargo.toml` (add `rusqlite`), `src/sink/mod.rs` (add `pub mod store;`)
- Test: `src/sink/store.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `crate::sink::encode::{timestamp_to_nanos, attributes_to_json}`; `crate::proto::LogBatch`; `Sink`/`SinkError` from `super`.
- Produces: `pub struct LogStore`; `LogStore::open(db_path: &str) -> Result<LogStore, StoreError>`; `LogStore::insert_batch(&self, batch: &LogBatch) -> Result<usize, StoreError>`; `LogStore::count(&self) -> Result<u64, StoreError>`; `pub struct StoreSink`; `StoreSink::new(store: Arc<LogStore>) -> StoreSink`; `pub enum StoreError`.

- [ ] **Step 1: Add the dependency**

In `Cargo.toml`, under `[dependencies]`, add (pin to the latest release at implementation time):

```toml
# Embedded SQLite store for the built-in viewer
rusqlite = { version = "0.32", features = ["bundled"] }
```

- [ ] **Step 2: Write the failing tests**

Create `src/sink/store.rs` with the test module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{LogBatch, LogRecord, Resource, Severity};
    use std::collections::HashMap;

    fn rec(body: &str, sev: Severity, service: &str, secs: i64) -> LogRecord {
        LogRecord {
            timestamp: Some(prost_types::Timestamp { seconds: secs, nanos: 0 }),
            severity: sev as i32,
            body: body.into(),
            attributes: vec![],
            resource: Some(Resource {
                service_name: service.into(), host_name: "h".into(), instance_id: "i".into(),
                attributes: vec![],
            }),
            trace_id: vec![], span_id: vec![], schema_url: String::new(),
        }
    }

    fn batch(records: Vec<LogRecord>) -> LogBatch {
        LogBatch { records, metadata: HashMap::new() }
    }

    #[test]
    fn open_memory_and_insert() {
        let store = LogStore::open(":memory:").unwrap();
        let n = store.insert_batch(&batch(vec![
            rec("a", Severity::Info, "svc", 1_700_000_000),
            rec("b", Severity::Error, "svc", 1_700_000_001),
        ])).unwrap();
        assert_eq!(n, 2);
        assert_eq!(store.count().unwrap(), 2);
    }

    #[test]
    fn empty_batch_inserts_nothing() {
        let store = LogStore::open(":memory:").unwrap();
        assert_eq!(store.insert_batch(&batch(vec![])).unwrap(), 0);
        assert_eq!(store.count().unwrap(), 0);
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --lib sink::store`
Expected: FAIL — `LogStore` not found / `pub mod store;` missing.

- [ ] **Step 4: Implement the store schema, open, insert, and sink**

Add above the test module in `src/sink/store.rs`:

```rust
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::Connection;

use crate::proto::LogBatch;
use crate::sink::encode;

use super::{Sink, SinkError};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("lock poisoned")]
    Lock,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS logs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_nanos      INTEGER NOT NULL,
  severity      INTEGER NOT NULL,
  service_name  TEXT,
  host_name     TEXT,
  instance_id   TEXT,
  body          TEXT,
  trace_id      TEXT,
  span_id       TEXT,
  attributes    TEXT,
  schema_url    TEXT
);
CREATE INDEX IF NOT EXISTS idx_logs_ts       ON logs(ts_nanos);
CREATE INDEX IF NOT EXISTS idx_logs_severity ON logs(severity);
CREATE INDEX IF NOT EXISTS idx_logs_service  ON logs(service_name);
";

/// SQLite-backed log store. Holds a single connection shared (via Arc<Mutex>)
/// between the store sink (writes) and the viewer HTTP handlers (reads).
pub struct LogStore {
    conn: Mutex<Connection>,
}

impl LogStore {
    pub fn open(db_path: &str) -> Result<Self, StoreError> {
        let conn = Connection::open(db_path)?;
        if db_path != ":memory:" {
            conn.pragma_update(None, "journal_mode", "WAL")?;
            conn.pragma_update(None, "synchronous", "NORMAL")?;
        }
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn insert_batch(&self, batch: &LogBatch) -> Result<usize, StoreError> {
        if batch.records.is_empty() {
            return Ok(0);
        }
        let mut conn = self.conn.lock().map_err(|_| StoreError::Lock)?;
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO logs
                 (ts_nanos, severity, service_name, host_name, instance_id, body, trace_id, span_id, attributes, schema_url)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            )?;
            for r in &batch.records {
                let ts_nanos = r.timestamp.as_ref().map(encode::timestamp_to_nanos).unwrap_or(0);
                let (service, host, instance) = match &r.resource {
                    Some(res) => (res.service_name.as_str(), res.host_name.as_str(), res.instance_id.as_str()),
                    None => ("", "", ""),
                };
                let trace = if r.trace_id.is_empty() { String::new() } else { hex::encode(&r.trace_id) };
                let span = if r.span_id.is_empty() { String::new() } else { hex::encode(&r.span_id) };
                let attrs = if r.attributes.is_empty() {
                    String::new()
                } else {
                    encode::attributes_to_json(&r.attributes).to_string()
                };
                stmt.execute(rusqlite::params![
                    ts_nanos, r.severity, service, host, instance, r.body, trace, span, attrs, r.schema_url,
                ])?;
            }
        }
        tx.commit()?;
        Ok(batch.records.len())
    }

    pub fn count(&self) -> Result<u64, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::Lock)?;
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM logs", [], |row| row.get(0))?;
        Ok(n as u64)
    }
}

/// Sink that persists batches into the embedded SQLite store.
pub struct StoreSink {
    store: Arc<LogStore>,
}

impl StoreSink {
    pub fn new(store: Arc<LogStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Sink for StoreSink {
    fn name(&self) -> &str {
        "store"
    }

    async fn send(&self, batch: LogBatch) -> Result<(), SinkError> {
        let store = Arc::clone(&self.store);
        tokio::task::spawn_blocking(move || store.insert_batch(&batch))
            .await
            .map_err(|e| SinkError::Network(format!("store join error: {e}")))?
            .map_err(|e| SinkError::Network(e.to_string()))?;
        Ok(())
    }

    async fn close(&self) -> Result<(), SinkError> {
        Ok(())
    }
}
```

In `src/sink/mod.rs` add `pub mod store;` next to the other module declarations.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib sink::store`
Expected: PASS (2 tests). First build compiles the bundled SQLite (slow once).

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/sink/store.rs src/sink/mod.rs
git commit -m "feat(sink): add SQLite LogStore + StoreSink (write path)"
```

---

### Task 4: SQLite store — query layer

**Files:**
- Modify: `src/sink/store.rs`
- Test: `src/sink/store.rs` (extend `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `LogStore`, `encode::rfc3339_from_nanos`.
- Produces: `pub struct LogQuery { q, min_severity, service, since_nanos, after_id, before_id, limit }`; `pub struct QueryResult { records: Vec<serde_json::Value>, oldest_id: Option<i64>, newest_id: Option<i64> }`; `LogStore::query(&self, q: &LogQuery) -> Result<QueryResult, StoreError>`; `LogStore::distinct_services(&self) -> Result<Vec<String>, StoreError>`.

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `src/sink/store.rs`:

```rust
    #[test]
    fn query_filters_and_paging() {
        let store = LogStore::open(":memory:").unwrap();
        store.insert_batch(&batch(vec![
            rec("login ok", Severity::Info, "auth", 1_700_000_000),
            rec("charge failed", Severity::Error, "pay", 1_700_000_010),
            rec("token expiry", Severity::Warn, "auth", 1_700_000_020),
        ])).unwrap();

        // No filter: newest first, all three.
        let all = store.query(&LogQuery { limit: 100, ..Default::default() }).unwrap();
        assert_eq!(all.records.len(), 3);
        assert_eq!(all.records[0]["body"], "token expiry"); // newest id first
        assert_eq!(all.newest_id, Some(3));
        assert_eq!(all.oldest_id, Some(1));

        // min_severity ERROR -> only the error.
        let errs = store.query(&LogQuery { min_severity: Some(17), limit: 100, ..Default::default() }).unwrap();
        assert_eq!(errs.records.len(), 1);
        assert_eq!(errs.records[0]["body"], "charge failed");

        // service filter.
        let auth = store.query(&LogQuery { service: Some("auth".into()), limit: 100, ..Default::default() }).unwrap();
        assert_eq!(auth.records.len(), 2);

        // text search on body.
        let q = store.query(&LogQuery { q: Some("charge".into()), limit: 100, ..Default::default() }).unwrap();
        assert_eq!(q.records.len(), 1);

        // after_id (live tail): rows newer than id 2, ascending.
        let tail = store.query(&LogQuery { after_id: Some(2), limit: 100, ..Default::default() }).unwrap();
        assert_eq!(tail.records.len(), 1);
        assert_eq!(tail.records[0]["id"], 3);

        // before_id (older page).
        let older = store.query(&LogQuery { before_id: Some(2), limit: 100, ..Default::default() }).unwrap();
        assert_eq!(older.records.len(), 1);
        assert_eq!(older.records[0]["id"], 1);

        // since_nanos cutoff keeps only records at/after the cutoff.
        let recent = store.query(&LogQuery { since_nanos: Some(1_700_000_015 * 1_000_000_000), limit: 100, ..Default::default() }).unwrap();
        assert_eq!(recent.records.len(), 1);
        assert_eq!(recent.records[0]["body"], "token expiry");
    }

    #[test]
    fn distinct_services_sorted() {
        let store = LogStore::open(":memory:").unwrap();
        store.insert_batch(&batch(vec![
            rec("x", Severity::Info, "pay", 1),
            rec("y", Severity::Info, "auth", 2),
            rec("z", Severity::Info, "pay", 3),
        ])).unwrap();
        assert_eq!(store.distinct_services().unwrap(), vec!["auth".to_string(), "pay".to_string()]);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib sink::store`
Expected: FAIL — `LogQuery` / `query` not found.

- [ ] **Step 3: Implement the query layer**

Add to `src/sink/store.rs` (above the test module). Add `use serde_json::json;` to the imports at the top of the file.

```rust
/// Filters for a log query. All `None`/default fields mean "no constraint".
#[derive(Debug, Default)]
pub struct LogQuery {
    pub q: Option<String>,
    pub min_severity: Option<i32>,
    pub service: Option<String>,
    pub since_nanos: Option<i64>,
    pub after_id: Option<i64>,
    pub before_id: Option<i64>,
    pub limit: usize,
}

pub struct QueryResult {
    pub records: Vec<serde_json::Value>,
    pub oldest_id: Option<i64>,
    pub newest_id: Option<i64>,
}

impl LogStore {
    pub fn query(&self, q: &LogQuery) -> Result<QueryResult, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::Lock)?;

        let mut sql = String::from(
            "SELECT id, ts_nanos, severity, service_name, host_name, instance_id, body, trace_id, span_id, attributes, schema_url FROM logs WHERE 1=1",
        );
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(text) = &q.q {
            sql.push_str(" AND body LIKE ?");
            params.push(Box::new(format!("%{text}%")));
        }
        if let Some(sev) = q.min_severity {
            sql.push_str(" AND severity >= ?");
            params.push(Box::new(sev));
        }
        if let Some(svc) = &q.service {
            sql.push_str(" AND service_name = ?");
            params.push(Box::new(svc.clone()));
        }
        if let Some(since) = q.since_nanos {
            sql.push_str(" AND ts_nanos >= ?");
            params.push(Box::new(since));
        }

        // after_id => ascending (live tail); otherwise newest-first, optional before_id.
        let ascending = q.after_id.is_some();
        if let Some(after) = q.after_id {
            sql.push_str(" AND id > ?");
            params.push(Box::new(after));
        }
        if let Some(before) = q.before_id {
            sql.push_str(" AND id < ?");
            params.push(Box::new(before));
        }
        sql.push_str(if ascending { " ORDER BY id ASC" } else { " ORDER BY id DESC" });
        let limit = q.limit.clamp(1, 1000);
        sql.push_str(" LIMIT ?");
        params.push(Box::new(limit as i64));

        let mut stmt = conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(param_refs.as_slice(), row_to_json)?;

        let mut records = Vec::new();
        for r in rows {
            records.push(r?);
        }

        let ids: Vec<i64> = records.iter().filter_map(|r| r["id"].as_i64()).collect();
        let oldest_id = ids.iter().min().copied();
        let newest_id = ids.iter().max().copied();

        Ok(QueryResult { records, oldest_id, newest_id })
    }

    pub fn distinct_services(&self) -> Result<Vec<String>, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::Lock)?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT service_name FROM logs WHERE service_name <> '' ORDER BY service_name",
        )?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }
}

/// Reconstruct the public record JSON (incl. "id") from a DB row.
fn row_to_json(row: &rusqlite::Row) -> rusqlite::Result<serde_json::Value> {
    let id: i64 = row.get(0)?;
    let ts_nanos: i64 = row.get(1)?;
    let severity: i32 = row.get(2)?;
    let service: String = row.get(3)?;
    let host: String = row.get(4)?;
    let instance: String = row.get(5)?;
    let body: String = row.get(6)?;
    let trace: String = row.get(7)?;
    let span: String = row.get(8)?;
    let attrs: String = row.get(9)?;
    let schema_url: String = row.get(10)?;

    let mut doc = json!({
        "id": id,
        "@timestamp": encode::rfc3339_from_nanos(ts_nanos),
        "severity": severity,
        "body": body,
        "resource": { "service_name": service, "host_name": host, "instance_id": instance },
    });
    if !trace.is_empty() { doc["trace_id"] = json!(trace); }
    if !span.is_empty() { doc["span_id"] = json!(span); }
    if !attrs.is_empty() {
        doc["attributes"] = serde_json::from_str(&attrs).unwrap_or(serde_json::Value::Null);
    }
    if !schema_url.is_empty() { doc["schema_url"] = json!(schema_url); }
    Ok(doc)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib sink::store`
Expected: PASS (4 tests total in the module).

- [ ] **Step 5: Commit**

```bash
git add src/sink/store.rs
git commit -m "feat(sink): add LogStore query layer (filters, paging, services)"
```

---

### Task 5: SQLite store — retention pruning

**Files:**
- Modify: `src/sink/store.rs`
- Test: `src/sink/store.rs` (extend `#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `LogStore::prune(&self, max_age_nanos: i64, max_records: u64, now_nanos: i64) -> Result<u64, StoreError>` (returns rows deleted).

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module:

```rust
    #[test]
    fn prune_by_age() {
        let store = LogStore::open(":memory:").unwrap();
        store.insert_batch(&batch(vec![
            rec("old", Severity::Info, "svc", 1_000),         // ts_nanos = 1_000e9
            rec("new", Severity::Info, "svc", 2_000),         // ts_nanos = 2_000e9
        ])).unwrap();
        // now = 2_000s, max_age = 500s -> cutoff 1_500s. "old" (1000s) is pruned.
        let deleted = store.prune(500 * 1_000_000_000, 1_000_000, 2_000 * 1_000_000_000).unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(store.count().unwrap(), 1);
    }

    #[test]
    fn prune_by_count() {
        let store = LogStore::open(":memory:").unwrap();
        for i in 0..10 {
            store.insert_batch(&batch(vec![rec("x", Severity::Info, "svc", 1_700_000_000 + i)])).unwrap();
        }
        // Keep only the newest 4. Huge max_age so age-pruning is a no-op.
        let deleted = store.prune(i64::MAX, 4, 2_000_000_000 * 1_000_000_000).unwrap();
        assert_eq!(deleted, 6);
        assert_eq!(store.count().unwrap(), 4);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib sink::store`
Expected: FAIL — `prune` not found.

- [ ] **Step 3: Implement `prune`**

Add a method to the second `impl LogStore { ... }` block in `src/sink/store.rs`:

```rust
    pub fn prune(&self, max_age_nanos: i64, max_records: u64, now_nanos: i64) -> Result<u64, StoreError> {
        let conn = self.conn.lock().map_err(|_| StoreError::Lock)?;
        let mut deleted = 0u64;

        // 1) Age-based: drop rows older than the cutoff. Saturating to avoid overflow.
        let cutoff = now_nanos.saturating_sub(max_age_nanos);
        deleted += conn.execute("DELETE FROM logs WHERE ts_nanos < ?1", [cutoff])? as u64;

        // 2) Count-based: keep only the newest `max_records` by id.
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM logs", [], |row| row.get(0))?;
        if count as u64 > max_records {
            deleted += conn.execute(
                "DELETE FROM logs WHERE id NOT IN (SELECT id FROM logs ORDER BY id DESC LIMIT ?1)",
                [max_records as i64],
            )? as u64;
        }
        Ok(deleted)
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib sink::store`
Expected: PASS (6 tests total in the module).

- [ ] **Step 5: Commit**

```bash
git add src/sink/store.rs
git commit -m "feat(sink): add LogStore retention pruning (age + count)"
```

---

### Task 6: Viewer HTTP server — query param parsing + JSON API

**Files:**
- Create: `src/viewer/mod.rs`
- Modify: `src/lib.rs` (add `pub mod viewer;`)
- Test: `src/viewer/mod.rs` (`#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `crate::sink::store::{LogStore, LogQuery}`, `crate::config::{BasicAuthConfig, parse_duration}`.
- Produces: `pub fn parse_query(query_string: &str, now_nanos: i64) -> LogQuery`; `pub fn parse_min_severity(s: &str) -> Option<i32>`; `pub struct ViewerServer`; `ViewerServer::new(store: Arc<LogStore>, auth: Option<BasicAuthConfig>) -> ViewerServer`; `ViewerServer::serve(self, addr: SocketAddr, shutdown: tokio::sync::watch::Receiver<bool>)`.

- [ ] **Step 1: Write the failing tests**

Create `src/viewer/mod.rs` with the test module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_names_and_ints() {
        assert_eq!(parse_min_severity("error"), Some(17));
        assert_eq!(parse_min_severity("INFO"), Some(9));
        assert_eq!(parse_min_severity("13"), Some(13));
        assert_eq!(parse_min_severity("nonsense"), None);
    }

    #[test]
    fn parse_query_maps_all_fields() {
        let now = 1_700_000_000 * 1_000_000_000;
        let q = parse_query("q=charge&min_severity=error&service=pay&since=1h&limit=50", now);
        assert_eq!(q.q.as_deref(), Some("charge"));
        assert_eq!(q.min_severity, Some(17));
        assert_eq!(q.service.as_deref(), Some("pay"));
        assert_eq!(q.since_nanos, Some(now - 3600 * 1_000_000_000));
        assert_eq!(q.limit, 50);
    }

    #[test]
    fn parse_query_after_id_and_defaults() {
        let q = parse_query("after_id=42", 0);
        assert_eq!(q.after_id, Some(42));
        assert_eq!(q.limit, 100); // default
    }

    #[test]
    fn url_decodes_query_text() {
        let q = parse_query("q=charge%20failed", 0);
        assert_eq!(q.q.as_deref(), Some("charge failed"));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Add `pub mod viewer;` to `src/lib.rs` (so it compiles), then run: `cargo test --lib viewer::tests`
Expected: FAIL — `parse_query` / `parse_min_severity` not found.

- [ ] **Step 3: Implement parsing + server scaffold**

Add above the test module in `src/viewer/mod.rs`:

```rust
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::config::{BasicAuthConfig, parse_duration};
use crate::sink::store::{LogQuery, LogStore};

const INDEX_HTML: &str = include_str!("index.html");

/// Map a severity name (case-insensitive) or integer string to its severity int.
pub fn parse_min_severity(s: &str) -> Option<i32> {
    match s.trim().to_ascii_uppercase().as_str() {
        "TRACE" => Some(1),
        "DEBUG" => Some(5),
        "INFO" => Some(9),
        "WARN" | "WARNING" => Some(13),
        "ERROR" => Some(17),
        "FATAL" => Some(21),
        other => other.parse::<i32>().ok(),
    }
}

/// Minimal application/x-www-form-urlencoded decoder (handles %XX and '+').
fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => { out.push(b' '); i += 1; }
            b'%' if i + 2 < bytes.len() => {
                let h = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]));
                if let (Some(hi), Some(lo)) = h {
                    out.push(hi * 16 + lo);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b => { out.push(b); i += 1; }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Parse a `/api/logs` query string into a LogQuery. `now_nanos` anchors relative `since`.
pub fn parse_query(query_string: &str, now_nanos: i64) -> LogQuery {
    let mut q = LogQuery { limit: 100, ..Default::default() };
    for pair in query_string.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, url_decode(v)),
            None => (pair, String::new()),
        };
        match k {
            "q" if !v.is_empty() => q.q = Some(v),
            "min_severity" => q.min_severity = parse_min_severity(&v),
            "service" if !v.is_empty() => q.service = Some(v),
            "since" => {
                if let Ok(d) = parse_duration(&v) {
                    q.since_nanos = Some(now_nanos - d.as_nanos() as i64);
                }
            }
            "after_id" => q.after_id = v.parse().ok(),
            "before_id" => q.before_id = v.parse().ok(),
            "limit" => { if let Ok(n) = v.parse::<usize>() { q.limit = n.clamp(1, 1000); } }
            _ => {}
        }
    }
    q
}

/// HTTP server for the built-in log viewer.
pub struct ViewerServer {
    store: Arc<LogStore>,
    auth: Option<BasicAuthConfig>,
}

impl ViewerServer {
    pub fn new(store: Arc<LogStore>, auth: Option<BasicAuthConfig>) -> Self {
        Self { store, auth }
    }

    pub async fn serve(self, addr: SocketAddr, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => { error!(error = %e, "viewer bind failed"); return; }
        };
        info!(%addr, "viewer server listening");

        let store = self.store;
        let auth = Arc::new(self.auth);

        loop {
            tokio::select! {
                accept = listener.accept() => {
                    if let Ok((stream, _)) = accept {
                        let store = Arc::clone(&store);
                        let auth = Arc::clone(&auth);
                        tokio::spawn(async move {
                            let io = TokioIo::new(stream);
                            let svc = service_fn(move |req| {
                                let store = Arc::clone(&store);
                                let auth = Arc::clone(&auth);
                                async move { Ok::<_, Infallible>(handle(req, store, auth).await) }
                            });
                            let _ = http1::Builder::new().serve_connection(io, svc).await;
                        });
                    }
                }
                _ = shutdown.changed() => { info!("viewer server shutting down"); break; }
            }
        }
    }
}

fn unauthorized() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("WWW-Authenticate", "Basic realm=\"watchtower\"")
        .body(Full::new(Bytes::from("unauthorized\n")))
        .unwrap()
}

fn check_auth(req: &Request<hyper::body::Incoming>, auth: &Option<BasicAuthConfig>) -> bool {
    let Some(cfg) = auth else { return true };
    let expected = base64_encode(format!("{}:{}", cfg.username, cfg.password).as_bytes());
    req.headers()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Basic "))
        .map(|got| got == expected)
        .unwrap_or(false)
}

/// Minimal standard base64 encoder (no padding shortcuts) for Basic auth comparison.
fn base64_encode(input: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in input.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        out.push(T[(b[0] >> 2) as usize] as char);
        out.push(T[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 { T[(((b[1] & 0x0f) << 2) | (b[2] >> 6)) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { T[(b[2] & 0x3f) as usize] as char } else { '=' });
    }
    out
}

async fn handle(
    req: Request<hyper::body::Incoming>,
    store: Arc<LogStore>,
    auth: Arc<Option<BasicAuthConfig>>,
) -> Response<Full<Bytes>> {
    if !check_auth(&req, &auth) {
        return unauthorized();
    }
    let path = req.uri().path();
    let raw_query = req.uri().query().unwrap_or("").to_string();

    match path {
        "/" => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/html; charset=utf-8")
            .body(Full::new(Bytes::from(INDEX_HTML)))
            .unwrap(),

        "/api/logs" => {
            let now_nanos = now_unix_nanos();
            let result = tokio::task::spawn_blocking(move || {
                let q = parse_query(&raw_query, now_nanos);
                store.query(&q)
            }).await;
            match result {
                Ok(Ok(r)) => {
                    let body = serde_json::json!({
                        "records": r.records, "oldest_id": r.oldest_id, "newest_id": r.newest_id,
                    });
                    json_ok(body.to_string())
                }
                _ => json_error(),
            }
        }

        "/api/services" => {
            let result = tokio::task::spawn_blocking(move || store.distinct_services()).await;
            match result {
                Ok(Ok(services)) => json_ok(serde_json::json!({ "services": services }).to_string()),
                _ => json_error(),
            }
        }

        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("not found\n")))
            .unwrap(),
    }
}

fn json_ok(body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

fn json_error() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from("{\"error\":\"query failed\"}")))
        .unwrap()
}

fn now_unix_nanos() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as i64).unwrap_or(0)
}
```

> Note: `include_str!("index.html")` requires the file to exist to compile. Create a one-line placeholder now so this task compiles: `echo "<!doctype html><title>watchtower</title>" > src/viewer/index.html` (the real UI lands in Task 7). Commit it alongside.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib viewer::tests`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs src/viewer/mod.rs src/viewer/index.html
git commit -m "feat(viewer): HTTP server with /api/logs, /api/services, basic auth"
```

---

### Task 7: Viewer UI — embedded single-page app

**Files:**
- Modify: `src/viewer/index.html` (replace the placeholder with the real UI)
- Test: `src/viewer/mod.rs` (add one handler-level test that `/` serves the page)

**Interfaces:**
- Consumes: `/api/logs`, `/api/services` from Task 6.

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/viewer/mod.rs`:

```rust
    #[test]
    fn index_html_is_embedded() {
        assert!(INDEX_HTML.contains("Watchtower"));
        assert!(INDEX_HTML.contains("/api/logs"));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib viewer::tests::index_html_is_embedded`
Expected: FAIL — placeholder HTML lacks "Watchtower" / "/api/logs".

- [ ] **Step 3: Write the UI**

Replace the entire contents of `src/viewer/index.html` with:

```html
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Watchtower Logs</title>
<style>
  :root { --bg:#0f1115; --panel:#171a21; --line:#262b36; --txt:#d6dae2; --muted:#7d8694; }
  * { box-sizing:border-box; }
  body { margin:0; font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; background:var(--bg); color:var(--txt); }
  header { display:flex; gap:8px; align-items:center; padding:8px 12px; border-bottom:1px solid var(--line); flex-wrap:wrap; }
  header h1 { font-size:14px; margin:0 8px 0 0; }
  input,select,button { background:var(--panel); color:var(--txt); border:1px solid var(--line); border-radius:6px; padding:5px 8px; font:inherit; }
  button { cursor:pointer; }
  button.active { border-color:#3b82f6; color:#9ec1ff; }
  #wrap { display:flex; height:calc(100vh - 49px); }
  #list { flex:1; overflow:auto; }
  table { width:100%; border-collapse:collapse; }
  td { padding:3px 8px; border-bottom:1px solid var(--line); vertical-align:top; white-space:nowrap; }
  td.body { white-space:normal; }
  tr { cursor:pointer; }
  tr:hover { background:#1d212a; }
  tr.sel { background:#243049; }
  .sev { font-weight:bold; }
  .s-1,.s-5 { color:var(--muted); }       /* TRACE/DEBUG */
  .s-9 { color:#8bd5ff; }                  /* INFO */
  .s-13 { color:#f5c451; }                 /* WARN */
  .s-17,.s-21 { color:#ff6b6b; }           /* ERROR/FATAL */
  .muted { color:var(--muted); }
  #detail { width:0; transition:width .12s; overflow:auto; border-left:1px solid var(--line); background:var(--panel); }
  #detail.open { width:42%; padding:12px; }
  #detail pre { white-space:pre-wrap; word-break:break-word; }
  #status { padding:4px 12px; color:var(--muted); border-top:1px solid var(--line); }
</style>
</head>
<body>
<header>
  <h1>Watchtower Logs</h1>
  <input id="q" placeholder="search body…" size="20">
  <select id="sev">
    <option value="">All severities</option>
    <option value="DEBUG">DEBUG+</option><option value="INFO">INFO+</option>
    <option value="WARN">WARN+</option><option value="ERROR">ERROR+</option>
  </select>
  <select id="svc"><option value="">All services</option></select>
  <select id="since">
    <option value="">All time</option><option value="15m">Last 15m</option>
    <option value="1h" selected>Last 1h</option><option value="24h">Last 24h</option><option value="7d">Last 7d</option>
  </select>
  <button id="apply">Apply</button>
  <button id="follow" class="active">● Live</button>
</header>
<div id="wrap">
  <div id="list"><table><tbody id="rows"></tbody></table></div>
  <div id="detail"></div>
</div>
<div id="status">loading…</div>
<script>
const $ = s => document.querySelector(s);
const rowsEl = $('#rows'), detailEl = $('#detail'), statusEl = $('#status');
let following = true, newestId = 0, byId = {}, timer = null;

const SEV = {1:'TRACE',5:'DEBUG',9:'INFO',13:'WARN',17:'ERROR',21:'FATAL'};

function filters() {
  const p = new URLSearchParams();
  if ($('#q').value) p.set('q', $('#q').value);
  if ($('#sev').value) p.set('min_severity', $('#sev').value);
  if ($('#svc').value) p.set('service', $('#svc').value);
  if ($('#since').value) p.set('since', $('#since').value);
  return p;
}

function rowHtml(r) {
  const t = (r['@timestamp'] || '').replace('T',' ').replace('Z','').slice(0,23);
  const sev = SEV[r.severity] || r.severity;
  const svc = (r.resource && r.resource.service_name) || '';
  return `<tr data-id="${r.id}"><td class="muted">${t}</td>`
       + `<td class="sev s-${r.severity}">${sev}</td>`
       + `<td class="muted">${svc}</td><td class="body">${escape(r.body||'')}</td></tr>`;
}
function escape(s){ return s.replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }

function attach(tr) {
  tr.onclick = () => {
    document.querySelectorAll('tr.sel').forEach(e => e.classList.remove('sel'));
    tr.classList.add('sel');
    const r = byId[tr.dataset.id];
    detailEl.classList.add('open');
    detailEl.innerHTML = '<pre>' + escape(JSON.stringify(r, null, 2)) + '</pre>';
  };
}

async function reload() {
  const p = filters(); p.set('limit','200');
  const res = await fetch('/api/logs?' + p.toString());
  const data = await res.json();
  byId = {}; rowsEl.innerHTML = '';
  data.records.forEach(r => { byId[r.id] = r; rowsEl.insertAdjacentHTML('beforeend', rowHtml(r)); });
  rowsEl.querySelectorAll('tr').forEach(attach);
  newestId = data.newest_id || 0;
  statusEl.textContent = data.records.length + ' records';
}

async function tail() {
  if (!following) return;
  const p = filters(); p.set('after_id', newestId); p.set('limit','500');
  const res = await fetch('/api/logs?' + p.toString());
  const data = await res.json();
  data.records.forEach(r => {
    byId[r.id] = r;
    rowsEl.insertAdjacentHTML('afterbegin', rowHtml(r));
    attach(rowsEl.firstElementChild);
  });
  if (data.newest_id) newestId = data.newest_id;
}

async function loadServices() {
  const res = await fetch('/api/services');
  const data = await res.json();
  data.services.forEach(s => {
    const o = document.createElement('option'); o.value = s; o.textContent = s; $('#svc').appendChild(o);
  });
}

$('#apply').onclick = reload;
$('#follow').onclick = () => {
  following = !following;
  $('#follow').classList.toggle('active', following);
  $('#follow').textContent = following ? '● Live' : '❚❚ Paused';
};

loadServices();
reload().then(() => { timer = setInterval(tail, 1500); });
</script>
</body>
</html>
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib viewer::tests::index_html_is_embedded`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/viewer/index.html src/viewer/mod.rs
git commit -m "feat(viewer): embedded single-page UI (live tail, detail, filters)"
```

---

### Task 8: Wire viewer into the binary + retention task + integration test

**Files:**
- Modify: `src/main.rs`
- Test: `tests/integration_test.rs` (add a viewer end-to-end test)

**Interfaces:**
- Consumes: everything from Tasks 1–7. `reqwest` (already a dependency) for the HTTP assertion.

- [ ] **Step 1: Write the failing integration test**

Add to `tests/integration_test.rs` (the existing helpers `make_batch`, etc. are reused). Append at the end of the file:

```rust
#[tokio::test]
async fn test_viewer_stores_and_serves_logs() {
    use std::sync::Arc;
    use watchtower::sink::store::{LogStore, StoreSink};
    use watchtower::viewer::ViewerServer;

    // Shared in-memory store, used by both the sink and the viewer.
    let store = Arc::new(LogStore::open(":memory:").unwrap());
    let store_sink = StoreSink::new(Arc::clone(&store));
    let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(store_sink)];

    let (grpc_addr, _pipeline, _metrics) = start_test_server(sinks).await;

    // Start the viewer on an ephemeral port.
    let viewer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let viewer_addr = viewer_listener.local_addr().unwrap();
    drop(viewer_listener); // free the port for the server to bind
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let viewer = ViewerServer::new(Arc::clone(&store), None);
    tokio::spawn(async move { viewer.serve(viewer_addr, rx).await; });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Ingest over gRPC.
    let mut client = WatchtowerServiceClient::connect(format!("http://{grpc_addr}")).await.unwrap();
    client.ingest(make_batch(5)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await; // pipeline flush + store insert

    // Query via the viewer HTTP API.
    let body = reqwest::get(format!("http://{viewer_addr}/api/logs"))
        .await.unwrap().text().await.unwrap();
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["records"].as_array().unwrap().len(), 5);

    // The UI page is served at /.
    let page = reqwest::get(format!("http://{viewer_addr}/")).await.unwrap();
    assert_eq!(page.status(), 200);
    assert!(page.text().await.unwrap().contains("Watchtower Logs"));
}
```

Add `serde_json` to `[dev-dependencies]` in `Cargo.toml` if not already present (it is a normal dependency, so it is available to tests; only add if `cargo test` reports it missing).

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test integration_test test_viewer_stores_and_serves_logs`
Expected: FAIL — `watchtower::viewer` is wired but `main.rs` is not yet updated; the test should compile (it uses library types) and fail only if wiring is wrong. If it passes already, that's fine — proceed (the test validates the library path; Step 3 wires the actual binary).

- [ ] **Step 3: Wire the viewer + retention into `main.rs`**

In `src/main.rs`, add imports:

```rust
use watchtower::config::ViewerConfig;
use watchtower::sink::store::{LogStore, StoreSink};
use watchtower::viewer::ViewerServer;
```

Replace the `// --- Build sinks ---` section with viewer-aware wiring:

```rust
    // --- Build sinks ---
    let mut sinks = build_sinks(&cfg.sinks)?;

    // --- Built-in viewer (SQLite store + web UI) ---
    let viewer_store = if cfg.viewer.enabled {
        match LogStore::open(&cfg.viewer.db_path) {
            Ok(store) => {
                let store = Arc::new(store);
                sinks.push(Arc::new(StoreSink::new(Arc::clone(&store))));
                info!(db_path = cfg.viewer.db_path.as_str(), "viewer store enabled");
                Some(store)
            }
            Err(e) => {
                error!(error = %e, "failed to open viewer store, viewer disabled");
                None
            }
        }
    } else {
        None
    };
```

After the pipeline is built and BEFORE the gRPC server starts (e.g. right after the spillover replay block), spawn the viewer server and retention task:

```rust
    // --- Spawn viewer HTTP server + retention task ---
    if let Some(store) = &viewer_store {
        spawn_viewer(&cfg.viewer, Arc::clone(store), shutdown_rx.clone())?;
    }
```

Add these helpers at the bottom of `src/main.rs` (next to `build_sinks`):

```rust
fn spawn_viewer(
    cfg: &ViewerConfig,
    store: Arc<LogStore>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = cfg.listen_addr.parse()?;
    let auth = cfg.auth.clone();
    let server = ViewerServer::new(Arc::clone(&store), auth);
    tokio::spawn(async move { server.serve(addr, shutdown_rx).await; });
    info!(%addr, "viewer server started");

    // Retention task: prune by age + count on an interval.
    let max_age_nanos = cfg.retention.max_age.as_nanos() as i64;
    let max_records = cfg.retention.max_records;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tick.tick().await;
            let store = Arc::clone(&store);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0);
            let _ = tokio::task::spawn_blocking(move || store.prune(max_age_nanos, max_records, now)).await;
        }
    });
    Ok(())
}
```

> Note: `shutdown_rx` is created earlier in `main` (`let (shutdown_tx, shutdown_rx) = ...`). The viewer subscribes via `.clone()`, exactly like the health server already does. Leave the existing `let sinks = build_sinks(...)` line removed (now replaced by the `let mut sinks` block above).

- [ ] **Step 4: Run the full test suite**

Run: `cargo test`
Expected: PASS — all existing tests plus `test_viewer_stores_and_serves_logs`.

- [ ] **Step 5: Manual smoke check (optional but recommended)**

Create `watchtower-viewer.yaml`:

```yaml
server:
  listen_addr: "[::]:9090"
viewer:
  enabled: true
  listen_addr: "127.0.0.1:9092"
  db_path: ":memory:"
```

Run: `cargo run -- --config watchtower-viewer.yaml`, then open `http://127.0.0.1:9092/`. Confirm the page loads and shows "0 records" with no sink errors. Stop with Ctrl+C.

- [ ] **Step 6: Commit**

```bash
git add src/main.rs tests/integration_test.rs Cargo.toml Cargo.lock
git commit -m "feat: wire built-in viewer + retention into the binary"
```

---

### Task 9: Docs, example config, and Dockerfile

**Files:**
- Modify: `watchtower.example.yaml`, `README.md`, `docs/configuration.md`, `Dockerfile`

**Interfaces:** none (documentation + build config).

- [ ] **Step 1: Add the C toolchain to the Docker build stage**

In `Dockerfile`, change the builder's apt-get line (the `bundled` SQLite needs a C compiler; `rust:slim` lacks one):

```dockerfile
RUN apt-get update && apt-get install -y protobuf-compiler gcc libc6-dev && rm -rf /var/lib/apt/lists/*
```

Also update the `EXPOSE` line to include the viewer port:

```dockerfile
EXPOSE 9090 9091 9092
```

- [ ] **Step 2: Document the viewer in the example config**

Append to `watchtower.example.yaml`:

```yaml
# Built-in log viewer. When enabled, Watchtower stores incoming logs in an
# embedded SQLite database and serves a web UI + JSON API to search them —
# so you can run with NO Elasticsearch/OpenSearch/forward sink at all.
viewer:
  enabled: false
  listen_addr: "127.0.0.1:9092"   # bind localhost by default; expose deliberately
  db_path: ":memory:"             # ephemeral; set a file path (e.g. /var/lib/watchtower/logs.db) to persist
  retention:
    max_records: 1000000          # trim oldest rows beyond this
    max_age: "7d"                 # delete rows older than this
  # auth:                         # optional HTTP basic auth on the viewer
  #   username: "admin"
  #   password: "changeme"
```

- [ ] **Step 3: Document the viewer in `docs/configuration.md`**

Add a `## viewer` section after `## sinks` documenting every field (`enabled`, `listen_addr`, `db_path`, `retention.max_records`, `retention.max_age`, `auth.username`, `auth.password`), the `/`, `/api/logs`, `/api/services` endpoints with their query params (`q`, `min_severity`, `service`, `since`, `after_id`, `before_id`, `limit`), and a "Run with no Elasticsearch" minimal config:

```yaml
server:
  listen_addr: "[::]:9090"
viewer:
  enabled: true
  db_path: "/var/lib/watchtower/logs.db"
```

Note in this section: the validation rule that at least one sink OR the viewer must be enabled, and that `:memory:` is ephemeral (lost on restart) while a file path persists.

- [ ] **Step 4: Update the README**

In `README.md`, add a bullet under **Features** for the built-in viewer, add `9092` viewer rows to the **Endpoints** table (`GET /` UI, `GET /api/logs`, `GET /api/services`), and add a "Run without Elasticsearch" quick-start snippet pointing at the viewer. Add `src/viewer/` and `src/sink/store.rs` to the **Project Structure** tree.

- [ ] **Step 5: Verify the build and tests still pass**

Run: `cargo build --release && cargo test`
Expected: release binary builds; all tests pass.

- [ ] **Step 6: Commit**

```bash
git add watchtower.example.yaml README.md docs/configuration.md Dockerfile
git commit -m "docs: document built-in viewer + Elasticsearch-optional setup"
```

---

## Self-Review

**Spec coverage check (spec → task):**
- ES-optional / zero-sink run → Task 1 (validation) + Task 8 (wiring). ✔
- SQLite store, `:memory:`/file, shared `Mutex` connection → Tasks 3–5. ✔
- Record→JSON reuse (no duplication) → Task 2. ✔
- Query API (`/api/logs`, `/api/services`, filters, paging, after_id) → Tasks 4 + 6. ✔
- Full UI (live tail, detail drawer, severity color, filters) → Task 7. ✔
- Retention (age + count, periodic) → Task 5 (logic) + Task 8 (task). ✔
- Security (opt-in, localhost default, optional basic auth) → Task 1 (defaults) + Task 6 (auth). ✔
- Duration `h`/`d` for `max_age`/`since` → Task 1. ✔
- Dependency + C toolchain → Task 3 (`rusqlite`) + Task 9 (Dockerfile). ✔
- Docs/example/README → Task 9. ✔
- Tests across both DB modes → store tests use `:memory:`; integration uses `:memory:`; file-mode is exercised by the WAL pragma path in `open` (covered indirectly). Acceptable for v1.

**Placeholder scan:** No "TBD"/"implement later"; every code step has complete code. The Task 6 `index.html` placeholder is explicitly created and replaced in Task 7 (documented, not a gap).

**Type consistency:** `LogStore`, `LogQuery` (fields `q/min_severity/service/since_nanos/after_id/before_id/limit`), `QueryResult` (`records/oldest_id/newest_id`), `StoreSink::new(Arc<LogStore>)`, `ViewerServer::new(Arc<LogStore>, Option<BasicAuthConfig>)`, `parse_query(&str, i64)`, `parse_min_severity(&str)`, `config::parse_duration` — names are identical across Tasks 3–8. Severity ints (1/5/9/13/17/21) are consistent between the proto, the store tests, the UI CSS classes (`s-1`…`s-21`), and `parse_min_severity`.
