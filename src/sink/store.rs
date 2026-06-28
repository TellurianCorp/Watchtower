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
