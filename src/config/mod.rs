use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

/// Top-level Watchtower agent configuration.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub pipeline: PipelineConfig,
    #[serde(default)]
    pub sinks: Vec<SinkConfig>,
}

/// gRPC listener settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub max_recv_msg_size: usize,
    pub max_concurrent_streams: u32,
    #[serde(with = "humantime_serde")]
    pub keepalive_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub keepalive_timeout: Duration,
    pub enable_compression: bool,
    /// Path to TLS certificate (PEM). If set, the server requires mTLS.
    pub tls_cert: Option<String>,
    /// Path to TLS private key (PEM).
    pub tls_key: Option<String>,
    /// Path to CA certificate for verifying client certs.
    pub tls_ca: Option<String>,
}

/// Pipeline batching and buffering between ingestion and sinks.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct PipelineConfig {
    pub batch_size: usize,
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
    pub buffer_size: usize,
    pub workers: usize,
}

/// Downstream delivery target.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum SinkConfig {
    #[serde(rename = "elasticsearch")]
    Elasticsearch(ElasticSinkConfig),
    #[serde(rename = "opensearch")]
    OpenSearch(ElasticSinkConfig),
    #[serde(rename = "watchtower")]
    Watchtower(ForwardSinkConfig),
}

/// Settings shared by Elasticsearch and OpenSearch (same bulk API).
#[derive(Debug, Clone, Deserialize)]
pub struct ElasticSinkConfig {
    pub addresses: Vec<String>,
    #[serde(default = "default_index")]
    pub index: String,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(default)]
    pub tls: bool,
    #[serde(default = "default_sink_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_flush_interval", with = "humantime_serde")]
    pub flush_interval: Duration,
    #[serde(default = "default_retry_attempts")]
    pub retry_attempts: u32,
    #[serde(default = "default_retry_backoff", with = "humantime_serde")]
    pub retry_backoff: Duration,
}

/// Settings for forwarding to a larger Watchtower instance.
#[derive(Debug, Clone, Deserialize)]
pub struct ForwardSinkConfig {
    pub target: String,
    #[serde(default = "default_true")]
    pub enable_compression: bool,
    #[serde(default = "default_forward_timeout", with = "humantime_serde")]
    pub timeout: Duration,
    pub tls_cert: Option<String>,
    pub tls_ca: Option<String>,
    #[serde(default = "default_retry_attempts")]
    pub retry_attempts: u32,
    #[serde(default = "default_retry_backoff", with = "humantime_serde")]
    pub retry_backoff: Duration,
}

// --- Defaults ---

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            pipeline: PipelineConfig::default(),
            sinks: Vec::new(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "[::]:9090".into(),
            max_recv_msg_size: 4 * 1024 * 1024,
            max_concurrent_streams: 100,
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
            enable_compression: true,
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
        }
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            flush_interval: Duration::from_secs(2),
            buffer_size: 8192,
            workers: 2,
        }
    }
}

fn default_index() -> String {
    "watchtower-logs".into()
}
fn default_sink_batch_size() -> usize {
    512
}
fn default_flush_interval() -> Duration {
    Duration::from_secs(5)
}
fn default_retry_attempts() -> u32 {
    3
}
fn default_retry_backoff() -> Duration {
    Duration::from_secs(1)
}
fn default_forward_timeout() -> Duration {
    Duration::from_secs(10)
}
fn default_true() -> bool {
    true
}

/// Deserialize human-readable durations like "5s", "100ms", "2m".
mod humantime_serde {
    use std::time::Duration;

    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_duration(&s).map_err(serde::de::Error::custom)
    }

    fn parse_duration(s: &str) -> Result<Duration, String> {
        let s = s.trim();
        if let Some(rest) = s.strip_suffix("ms") {
            rest.trim()
                .parse::<u64>()
                .map(Duration::from_millis)
                .map_err(|e| e.to_string())
        } else if let Some(rest) = s.strip_suffix('s') {
            rest.trim()
                .parse::<u64>()
                .map(Duration::from_secs)
                .map_err(|e| e.to_string())
        } else if let Some(rest) = s.strip_suffix('m') {
            rest.trim()
                .parse::<u64>()
                .map(|v| Duration::from_secs(v * 60))
                .map_err(|e| e.to_string())
        } else {
            // Fallback: try as seconds
            s.parse::<u64>()
                .map(Duration::from_secs)
                .map_err(|_| format!("invalid duration: {s}"))
        }
    }
}

impl Config {
    /// Load configuration from a YAML file, merging onto defaults.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let data = std::fs::read_to_string(path.as_ref())?;
        let cfg: Config = serde_yaml::from_str(&data)?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), String> {
        if self.pipeline.batch_size == 0 {
            return Err("pipeline.batch_size must be > 0".into());
        }
        if self.pipeline.buffer_size == 0 {
            return Err("pipeline.buffer_size must be > 0".into());
        }
        if self.pipeline.workers == 0 {
            return Err("pipeline.workers must be > 0".into());
        }
        for (i, sink) in self.sinks.iter().enumerate() {
            match sink {
                SinkConfig::Elasticsearch(c) | SinkConfig::OpenSearch(c) => {
                    if c.addresses.is_empty() {
                        return Err(format!("sink[{i}]: addresses required"));
                    }
                }
                SinkConfig::Watchtower(c) => {
                    if c.target.is_empty() {
                        return Err(format!("sink[{i}]: target required"));
                    }
                }
            }
        }
        Ok(())
    }
}
