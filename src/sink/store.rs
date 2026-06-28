use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::Connection;
use serde_json::json;

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

/// Filters for a log query. All `None`/default fields mean "no constraint".
#[derive(Debug)]
pub struct LogQuery {
    pub q: Option<String>,
    pub min_severity: Option<i32>,
    pub service: Option<String>,
    pub since_nanos: Option<i64>,
    pub after_id: Option<i64>,
    pub before_id: Option<i64>,
    pub limit: usize,
}

impl Default for LogQuery {
    fn default() -> Self {
        Self {
            q: None,
            min_severity: None,
            service: None,
            since_nanos: None,
            after_id: None,
            before_id: None,
            limit: 100,
        }
    }
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

    #[test]
    fn limit_zero_is_clamped_to_one() {
        let store = LogStore::open(":memory:").unwrap();
        store.insert_batch(&batch(vec![
            rec("a", Severity::Info, "svc", 1),
            rec("b", Severity::Info, "svc", 2),
        ])).unwrap();
        let r = store.query(&LogQuery { limit: 0, ..Default::default() }).unwrap();
        assert_eq!(r.records.len(), 1);
    }
}
