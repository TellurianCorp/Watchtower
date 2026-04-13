use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use tonic::transport::{Channel, Endpoint};
use tracing::debug;

use crate::proto::watchtower_service_client::WatchtowerServiceClient;
use crate::proto::{
    AnyValue, IngestResponse, KeyValue, LogBatch, LogRecord, Resource, Severity,
    any_value,
};

/// WatchtowerClient is the application-side SDK for sending logs to a
/// local Watchtower sidecar. Designed to be dead-simple: create one per
/// process, call `log()` or `log_batch()`.
///
/// # Example
/// ```ignore
/// let client = WatchtowerClient::connect("http://localhost:9090").await?;
/// client.info("user logged in", attrs! { "user_id" => "abc123" }).await?;
/// ```
pub struct WatchtowerClient {
    inner: WatchtowerServiceClient<Channel>,
    resource: Option<Resource>,
}

/// Builder for configuring a WatchtowerClient.
pub struct ClientBuilder {
    target: String,
    timeout: Duration,
    enable_compression: bool,
    resource: Option<Resource>,
}

impl ClientBuilder {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            timeout: Duration::from_secs(5),
            enable_compression: true,
            resource: None,
        }
    }

    /// Set the gRPC call timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Enable or disable gzip compression on the wire.
    pub fn compression(mut self, enabled: bool) -> Self {
        self.enable_compression = enabled;
        self
    }

    /// Attach a resource identity (service name, host, instance) to every log.
    pub fn resource(mut self, service_name: &str, host_name: &str, instance_id: &str) -> Self {
        self.resource = Some(Resource {
            service_name: service_name.into(),
            host_name: host_name.into(),
            instance_id: instance_id.into(),
            attributes: vec![],
        });
        self
    }

    /// Connect and return a ready-to-use client.
    pub async fn connect(self) -> Result<WatchtowerClient, ClientError> {
        let uri = if self.target.contains("://") {
            self.target.clone()
        } else {
            format!("http://{}", self.target)
        };

        let endpoint = Endpoint::from_shared(uri)
            .map_err(|e| ClientError::Connection(e.to_string()))?
            .timeout(self.timeout)
            .keep_alive_while_idle(true);

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| ClientError::Connection(e.to_string()))?;

        let mut client = WatchtowerServiceClient::new(channel);
        if self.enable_compression {
            client = client
                .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip);
        }

        Ok(WatchtowerClient {
            inner: client,
            resource: self.resource,
        })
    }
}

impl WatchtowerClient {
    /// Quick connect with defaults — just provide the address.
    pub async fn connect(target: impl Into<String>) -> Result<Self, ClientError> {
        ClientBuilder::new(target).connect().await
    }

    /// Send a single log record.
    pub async fn log(
        &mut self,
        severity: Severity,
        body: impl Into<String>,
        attributes: Vec<KeyValue>,
    ) -> Result<IngestResponse, ClientError> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();

        let record = LogRecord {
            timestamp: Some(prost_types::Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
            severity: severity as i32,
            body: body.into(),
            attributes,
            resource: self.resource.clone(),
            ..Default::default()
        };

        self.log_batch(vec![record]).await
    }

    /// Send a batch of pre-built log records.
    pub async fn log_batch(&mut self, records: Vec<LogRecord>) -> Result<IngestResponse, ClientError> {
        let count = records.len();
        let batch = LogBatch {
            records,
            metadata: HashMap::new(),
        };

        let resp = self
            .inner
            .ingest(batch)
            .await
            .map_err(|e| ClientError::Rpc(e.to_string()))?;

        debug!(records = count, "sent batch to watchtower");
        Ok(resp.into_inner())
    }

    // --- Convenience methods ---

    pub async fn trace(&mut self, body: impl Into<String>, attrs: Vec<KeyValue>) -> Result<IngestResponse, ClientError> {
        self.log(Severity::Trace, body, attrs).await
    }

    pub async fn debug(&mut self, body: impl Into<String>, attrs: Vec<KeyValue>) -> Result<IngestResponse, ClientError> {
        self.log(Severity::Debug, body, attrs).await
    }

    pub async fn info(&mut self, body: impl Into<String>, attrs: Vec<KeyValue>) -> Result<IngestResponse, ClientError> {
        self.log(Severity::Info, body, attrs).await
    }

    pub async fn warn(&mut self, body: impl Into<String>, attrs: Vec<KeyValue>) -> Result<IngestResponse, ClientError> {
        self.log(Severity::Warn, body, attrs).await
    }

    pub async fn error(&mut self, body: impl Into<String>, attrs: Vec<KeyValue>) -> Result<IngestResponse, ClientError> {
        self.log(Severity::Error, body, attrs).await
    }

    pub async fn fatal(&mut self, body: impl Into<String>, attrs: Vec<KeyValue>) -> Result<IngestResponse, ClientError> {
        self.log(Severity::Fatal, body, attrs).await
    }
}

/// Helper to build a KeyValue attribute.
pub fn attr(key: &str, value: impl Into<AttrValue>) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(value.into().0),
    }
}

/// Wrapper for ergonomic attribute construction.
pub struct AttrValue(AnyValue);

impl From<&str> for AttrValue {
    fn from(s: &str) -> Self {
        AttrValue(AnyValue {
            value: Some(any_value::Value::StringValue(s.into())),
        })
    }
}

impl From<String> for AttrValue {
    fn from(s: String) -> Self {
        AttrValue(AnyValue {
            value: Some(any_value::Value::StringValue(s)),
        })
    }
}

impl From<i64> for AttrValue {
    fn from(n: i64) -> Self {
        AttrValue(AnyValue {
            value: Some(any_value::Value::IntValue(n)),
        })
    }
}

impl From<f64> for AttrValue {
    fn from(f: f64) -> Self {
        AttrValue(AnyValue {
            value: Some(any_value::Value::DoubleValue(f)),
        })
    }
}

impl From<bool> for AttrValue {
    fn from(b: bool) -> Self {
        AttrValue(AnyValue {
            value: Some(any_value::Value::BoolValue(b)),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("RPC error: {0}")]
    Rpc(String),
}
