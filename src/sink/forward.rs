use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, warn};

use crate::config::ForwardSinkConfig;
use crate::proto::watchtower_service_client::WatchtowerServiceClient;
use crate::proto::LogBatch;

use super::{Sink, SinkError};

/// ForwardSink sends log batches to a larger upstream Watchtower instance
/// via the same gRPC WatchtowerService.Ingest RPC.
pub struct ForwardSink {
    target: String,
    client: Mutex<Option<WatchtowerServiceClient<Channel>>>,
    endpoint: Endpoint,
    enable_compression: bool,
    retry_attempts: u32,
    retry_backoff: Duration,
}

impl ForwardSink {
    pub fn new(cfg: &ForwardSinkConfig) -> Result<Self, SinkError> {
        let uri = if cfg.target.contains("://") {
            cfg.target.clone()
        } else {
            format!("http://{}", cfg.target)
        };

        let endpoint = Channel::from_shared(uri.clone())
            .map_err(|e| SinkError::Network(e.to_string()))?
            .timeout(cfg.timeout)
            .keep_alive_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true);

        Ok(Self {
            target: cfg.target.clone(),
            client: Mutex::new(None),
            endpoint,
            enable_compression: cfg.enable_compression,
            retry_attempts: cfg.retry_attempts,
            retry_backoff: cfg.retry_backoff,
        })
    }

    async fn get_client(&self) -> Result<WatchtowerServiceClient<Channel>, SinkError> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        let channel = self
            .endpoint
            .connect()
            .await
            .map_err(|e| SinkError::Network(format!("connect to {}: {e}", self.target)))?;

        let mut client = WatchtowerServiceClient::new(channel);
        if self.enable_compression {
            client = client
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
        }
        *guard = Some(client.clone());
        Ok(client)
    }

    /// Clear the cached client so the next call reconnects.
    async fn reset_client(&self) {
        let mut guard = self.client.lock().await;
        *guard = None;
    }
}

#[async_trait]
impl Sink for ForwardSink {
    fn name(&self) -> &str {
        "watchtower-forward"
    }

    async fn send(&self, batch: LogBatch) -> Result<(), SinkError> {
        if batch.records.is_empty() {
            return Ok(());
        }

        let record_count = batch.records.len();
        let mut last_err = None;

        for attempt in 0..=self.retry_attempts {
            if attempt > 0 {
                let delay = self.retry_backoff * 2u32.saturating_pow(attempt - 1);
                tokio::time::sleep(delay).await;
                warn!(attempt, target = self.target.as_str(), "retrying forward");
                self.reset_client().await;
            }

            match self.get_client().await {
                Ok(mut client) => {
                    let mut request = tonic::Request::new(batch.clone());
                    // Request server-side gzip compression on the response.
                    request
                        .metadata_mut()
                        .insert("grpc-accept-encoding", "gzip".parse().unwrap());

                    match client.ingest(request).await {
                        Ok(resp) => {
                            let inner = resp.into_inner();
                            debug!(
                                target_addr = self.target.as_str(),
                                accepted = inner.accepted_count,
                                records = record_count,
                                "forwarded"
                            );
                            return Ok(());
                        }
                        Err(status) => {
                            last_err =
                                Some(SinkError::Rejected(format!("gRPC {}: {}", status.code(), status.message())));
                        }
                    }
                }
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap())
    }

    async fn close(&self) -> Result<(), SinkError> {
        self.reset_client().await;
        Ok(())
    }
}
