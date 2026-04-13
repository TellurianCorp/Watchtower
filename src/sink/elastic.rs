use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;
use tracing::{debug, warn};

use crate::config::ElasticSinkConfig;
use crate::proto::{self, LogBatch};

use super::{Sink, SinkError};

/// Elasticsearch / OpenSearch sink using the HTTP Bulk API.
/// Works identically for both engines since the /_bulk endpoint is compatible.
pub struct ElasticSink {
    label: String,
    client: Client,
    addresses: Vec<String>,
    index: String,
    username: Option<String>,
    password: Option<String>,
    round_robin: AtomicUsize,
    retry_attempts: u32,
    retry_backoff: Duration,
}

impl ElasticSink {
    pub fn new(label: &str, cfg: &ElasticSinkConfig) -> Result<Self, SinkError> {
        let builder = Client::builder()
            .pool_max_idle_per_host(4)
            .timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(!cfg.tls);

        let client = builder.build().map_err(|e| SinkError::Network(e.to_string()))?;

        Ok(Self {
            label: label.to_string(),
            client,
            addresses: cfg.addresses.clone(),
            index: cfg.index.clone(),
            username: cfg.username.clone(),
            password: cfg.password.clone(),
            round_robin: AtomicUsize::new(0),
            retry_attempts: cfg.retry_attempts,
            retry_backoff: cfg.retry_backoff,
        })
    }

    fn next_address(&self) -> &str {
        let idx = self.round_robin.fetch_add(1, Ordering::Relaxed) % self.addresses.len();
        &self.addresses[idx]
    }
}

#[async_trait]
impl Sink for ElasticSink {
    fn name(&self) -> &str {
        &self.label
    }

    async fn send(&self, batch: LogBatch) -> Result<(), SinkError> {
        if batch.records.is_empty() {
            return Ok(());
        }

        let body = build_bulk_body(&self.index, &batch);

        let mut last_err = None;
        for attempt in 0..=self.retry_attempts {
            if attempt > 0 {
                let delay = self.retry_backoff * 2u32.saturating_pow(attempt - 1);
                tokio::time::sleep(delay).await;
                warn!(
                    sink = self.label.as_str(),
                    attempt,
                    "retrying bulk request"
                );
            }

            let addr = self.next_address();
            let url = format!("{}/_bulk", addr.trim_end_matches('/'));

            let mut req = self
                .client
                .post(&url)
                .header("Content-Type", "application/x-ndjson");

            if let (Some(user), Some(pass)) = (&self.username, &self.password) {
                req = req.basic_auth(user, Some(pass));
            }

            let result = req.body(body.clone()).send().await;

            match result {
                Ok(resp) if resp.status().is_success() => {
                    debug!(
                        sink = self.label.as_str(),
                        records = batch.records.len(),
                        "bulk indexed"
                    );
                    return Ok(());
                }
                Ok(resp) => {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    last_err = Some(SinkError::Rejected(format!("{status}: {text}")));
                }
                Err(e) => {
                    last_err = Some(SinkError::Network(e.to_string()));
                }
            }
        }

        Err(last_err.unwrap())
    }

    async fn close(&self) -> Result<(), SinkError> {
        Ok(())
    }
}

/// Build an NDJSON bulk payload from a LogBatch.
fn build_bulk_body(index: &str, batch: &LogBatch) -> String {
    let mut buf = String::with_capacity(batch.records.len() * 256);

    for record in &batch.records {
        // Action line
        buf.push_str(&format!("{{\"index\":{{\"_index\":\"{index}\"}}}}\n"));

        // Document line
        let doc = record_to_json(record);
        buf.push_str(&doc.to_string());
        buf.push('\n');
    }

    buf
}

fn record_to_json(record: &proto::LogRecord) -> serde_json::Value {
    let mut doc = json!({
        "severity": record.severity,
        "body": record.body,
    });

    if let Some(ts) = &record.timestamp {
        // Convert protobuf timestamp to RFC3339
        let secs = ts.seconds;
        let nanos = ts.nanos as u32;
        if let Some(dt) = chrono_from_timestamp(secs, nanos) {
            doc["@timestamp"] = json!(dt);
        }
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
        let attrs: serde_json::Map<String, serde_json::Value> = record
            .attributes
            .iter()
            .filter_map(|kv| {
                kv.value
                    .as_ref()
                    .map(|v| (kv.key.clone(), any_value_to_json(v)))
            })
            .collect();
        doc["attributes"] = json!(attrs);
    }

    if !record.schema_url.is_empty() {
        doc["schema_url"] = json!(record.schema_url);
    }

    doc
}

fn any_value_to_json(v: &proto::AnyValue) -> serde_json::Value {
    match &v.value {
        Some(proto::any_value::Value::StringValue(s)) => json!(s),
        Some(proto::any_value::Value::IntValue(n)) => json!(n),
        Some(proto::any_value::Value::DoubleValue(f)) => json!(f),
        Some(proto::any_value::Value::BoolValue(b)) => json!(b),
        Some(proto::any_value::Value::BytesValue(b)) => json!(hex::encode(b)),
        Some(proto::any_value::Value::ArrayValue(arr)) => {
            json!(arr.values.iter().map(any_value_to_json).collect::<Vec<_>>())
        }
        Some(proto::any_value::Value::MapValue(m)) => {
            let map: serde_json::Map<String, serde_json::Value> = m
                .entries
                .iter()
                .filter_map(|kv| {
                    kv.value
                        .as_ref()
                        .map(|v| (kv.key.clone(), any_value_to_json(v)))
                })
                .collect();
            json!(map)
        }
        None => serde_json::Value::Null,
    }
}

/// Convert protobuf seconds + nanos to an RFC3339 string.
fn chrono_from_timestamp(seconds: i64, nanos: u32) -> Option<String> {
    // Manual RFC3339 without pulling in the chrono crate.
    // Unix epoch: 1970-01-01T00:00:00Z
    const SECS_PER_DAY: i64 = 86400;
    let days = seconds / SECS_PER_DAY;
    let day_secs = (seconds % SECS_PER_DAY) as u32;
    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let secs = day_secs % 60;

    // Simple days-since-epoch to Y-M-D (good enough for log timestamps).
    let (year, month, day) = days_to_date(days);

    Some(format!(
        "{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{secs:02}.{nanos:09}Z"
    ))
}

fn days_to_date(days: i64) -> (i32, u32, u32) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
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
