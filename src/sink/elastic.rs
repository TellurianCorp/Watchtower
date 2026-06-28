use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use tracing::{debug, warn};

use crate::config::ElasticSinkConfig;
use crate::proto::LogBatch;

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
        let doc = crate::sink::encode::record_to_json(record);
        buf.push_str(&doc.to_string());
        buf.push('\n');
    }

    buf
}
