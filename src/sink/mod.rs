pub mod elastic;
pub mod forward;

use async_trait::async_trait;

use crate::proto::LogBatch;

/// Sink is the trait that all delivery targets implement.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Human-readable name for logging.
    fn name(&self) -> &str;

    /// Send a batch of log records to the downstream target.
    async fn send(&self, batch: LogBatch) -> Result<(), SinkError>;

    /// Gracefully close the sink, flushing any remaining data.
    async fn close(&self) -> Result<(), SinkError>;
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("network error: {0}")]
    Network(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("upstream rejected: {0}")]
    Rejected(String),
}
