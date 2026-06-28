use std::env;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

/// Top-level Watchtower agent configuration.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub pipeline: PipelineConfig,
    pub health: HealthConfig,
    pub spillover: SpilloverConfig,
    #[serde(default)]
    pub sinks: Vec<SinkConfig>,
    pub viewer: ViewerConfig,
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

/// Health/metrics HTTP server settings.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct HealthConfig {
    pub enabled: bool,
    pub listen_addr: String,
}

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

/// Disk spillover settings for crash resilience.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct SpilloverConfig {
    pub enabled: bool,
    pub path: String,
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
            health: HealthConfig::default(),
            spillover: SpilloverConfig::default(),
            sinks: Vec::new(),
            viewer: ViewerConfig::default(),
        }
    }
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_addr: "[::]:9091".into(),
        }
    }
}

impl Default for SpilloverConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: "/var/lib/watchtower/spillover.bin".into(),
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

impl Config {
    /// Load configuration from a YAML file, then apply PORT env var override.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let data = std::fs::read_to_string(path.as_ref())?;
        let mut cfg: Config = serde_yaml::from_str(&data)?;
        cfg.apply_env_overrides();
        cfg.validate()?;
        Ok(cfg)
    }

    /// Build a complete configuration purely from environment variables.
    /// Used when no config file is available (e.g., Railway deployments).
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let mut cfg = Config::default();

        // --- Server ---
        if let Ok(port) = env::var("PORT") {
            cfg.server.listen_addr = format!("[::]:{port}");
        }

        // --- Health ---
        if let Ok(port) = env::var("WATCHTOWER_HEALTH_PORT") {
            cfg.health.listen_addr = format!("[::]:{port}");
        }
        if let Ok(v) = env::var("WATCHTOWER_HEALTH_ENABLED") {
            cfg.health.enabled = v != "0" && v.to_lowercase() != "false";
        }

        // --- Pipeline ---
        if let Ok(v) = env::var("WATCHTOWER_WORKERS") {
            cfg.pipeline.workers = v.parse().map_err(|_| "invalid WATCHTOWER_WORKERS")?;
        }
        if let Ok(v) = env::var("WATCHTOWER_BATCH_SIZE") {
            cfg.pipeline.batch_size = v.parse().map_err(|_| "invalid WATCHTOWER_BATCH_SIZE")?;
        }
        if let Ok(v) = env::var("WATCHTOWER_BUFFER_SIZE") {
            cfg.pipeline.buffer_size = v.parse().map_err(|_| "invalid WATCHTOWER_BUFFER_SIZE")?;
        }
        if let Ok(v) = env::var("WATCHTOWER_FLUSH_INTERVAL") {
            cfg.pipeline.flush_interval = parse_duration_str(&v)?;
        }

        // --- Spillover ---
        if let Ok(v) = env::var("WATCHTOWER_SPILLOVER_ENABLED") {
            cfg.spillover.enabled = v != "0" && v.to_lowercase() != "false";
        }
        if let Ok(v) = env::var("WATCHTOWER_SPILLOVER_PATH") {
            cfg.spillover.path = v;
        }

        // --- Viewer (built-in SQLite log viewer) ---
        Self::apply_viewer_env(&mut cfg.viewer, |k| env::var(k).ok())?;

        // --- Sinks (env vars) ---
        cfg.sinks = Self::sinks_from_env();

        cfg.validate()?;
        Ok(cfg)
    }

    /// Apply `WATCHTOWER_VIEWER_*` environment variables onto a ViewerConfig.
    /// `get` resolves a variable by name (None if unset); it is injected so the
    /// parsing is unit-testable without mutating the process environment.
    fn apply_viewer_env<F>(viewer: &mut ViewerConfig, get: F) -> Result<(), Box<dyn std::error::Error>>
    where
        F: Fn(&str) -> Option<String>,
    {
        if let Some(v) = get("WATCHTOWER_VIEWER_ENABLED") {
            viewer.enabled = v != "0" && v.to_lowercase() != "false";
        }
        // PORT binds all interfaces (needed behind Railway's proxy); LISTEN_ADDR overrides.
        if let Some(port) = get("WATCHTOWER_VIEWER_PORT") {
            viewer.listen_addr = format!("[::]:{port}");
        }
        if let Some(addr) = get("WATCHTOWER_VIEWER_LISTEN_ADDR") {
            viewer.listen_addr = addr;
        }
        if let Some(v) = get("WATCHTOWER_VIEWER_DB_PATH") {
            viewer.db_path = v;
        }
        if let Some(v) = get("WATCHTOWER_VIEWER_MAX_RECORDS") {
            viewer.retention.max_records =
                v.parse().map_err(|_| "invalid WATCHTOWER_VIEWER_MAX_RECORDS")?;
        }
        if let Some(v) = get("WATCHTOWER_VIEWER_MAX_AGE") {
            viewer.retention.max_age =
                parse_duration(&v).map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
        }
        // Basic auth only when BOTH username and password are present.
        if let (Some(username), Some(password)) = (
            get("WATCHTOWER_VIEWER_AUTH_USERNAME"),
            get("WATCHTOWER_VIEWER_AUTH_PASSWORD"),
        ) {
            viewer.auth = Some(BasicAuthConfig { username, password });
        }
        Ok(())
    }

    /// Apply environment variable overrides on top of a YAML-loaded config.
    /// This ensures `PORT` always wins (Railway injects it).
    fn apply_env_overrides(&mut self) {
        if let Ok(port) = env::var("PORT") {
            self.server.listen_addr = format!("[::]:{port}");
        }
        if let Ok(port) = env::var("WATCHTOWER_HEALTH_PORT") {
            self.health.listen_addr = format!("[::]:{port}");
        }
    }

    /// Parse sink configuration from environment variables.
    /// Supports a single-sink shorthand (WATCHTOWER_SINK_*) and
    /// multi-sink indexed form (WATCHTOWER_SINK_0_*, WATCHTOWER_SINK_1_*, ...).
    fn sinks_from_env() -> Vec<SinkConfig> {
        let mut sinks = Vec::new();

        // Try single-sink shorthand first.
        if let Some(sink) = Self::parse_sink_env("WATCHTOWER_SINK") {
            sinks.push(sink);
        }

        // Then try indexed sinks: WATCHTOWER_SINK_0, WATCHTOWER_SINK_1, ...
        for i in 0..16 {
            let prefix = format!("WATCHTOWER_SINK_{i}");
            if let Some(sink) = Self::parse_sink_env(&prefix) {
                sinks.push(sink);
            }
        }

        sinks
    }

    fn parse_sink_env(prefix: &str) -> Option<SinkConfig> {
        let sink_type = env::var(format!("{prefix}_TYPE")).ok()?;

        match sink_type.to_lowercase().as_str() {
            "elasticsearch" | "opensearch" => {
                let addresses_raw = env::var(format!("{prefix}_ADDRESSES")).unwrap_or_default();
                let addresses: Vec<String> = addresses_raw
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                if addresses.is_empty() {
                    return None;
                }

                let es_cfg = ElasticSinkConfig {
                    addresses,
                    index: env::var(format!("{prefix}_INDEX")).unwrap_or_else(|_| default_index()),
                    username: env::var(format!("{prefix}_USERNAME")).ok(),
                    password: env::var(format!("{prefix}_PASSWORD")).ok(),
                    tls: env::var(format!("{prefix}_TLS"))
                        .map(|v| v != "0" && v.to_lowercase() != "false")
                        .unwrap_or(false),
                    batch_size: env::var(format!("{prefix}_BATCH_SIZE"))
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(default_sink_batch_size),
                    flush_interval: env::var(format!("{prefix}_FLUSH_INTERVAL"))
                        .ok()
                        .and_then(|v| parse_duration_str(&v).ok())
                        .unwrap_or_else(default_flush_interval),
                    retry_attempts: env::var(format!("{prefix}_RETRY_ATTEMPTS"))
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(default_retry_attempts),
                    retry_backoff: default_retry_backoff(),
                };

                if sink_type.to_lowercase() == "opensearch" {
                    Some(SinkConfig::OpenSearch(es_cfg))
                } else {
                    Some(SinkConfig::Elasticsearch(es_cfg))
                }
            }
            "watchtower" => {
                let target = env::var(format!("{prefix}_TARGET")).ok()?;
                if target.is_empty() {
                    return None;
                }

                Some(SinkConfig::Watchtower(ForwardSinkConfig {
                    target,
                    enable_compression: env::var(format!("{prefix}_COMPRESSION"))
                        .map(|v| v != "0" && v.to_lowercase() != "false")
                        .unwrap_or(true),
                    timeout: env::var(format!("{prefix}_TIMEOUT"))
                        .ok()
                        .and_then(|v| parse_duration_str(&v).ok())
                        .unwrap_or_else(default_forward_timeout),
                    tls_cert: env::var(format!("{prefix}_TLS_CERT")).ok(),
                    tls_ca: env::var(format!("{prefix}_TLS_CA")).ok(),
                    retry_attempts: env::var(format!("{prefix}_RETRY_ATTEMPTS"))
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(default_retry_attempts),
                    retry_backoff: default_retry_backoff(),
                }))
            }
            _ => None,
        }
    }

    fn validate(&self) -> Result<(), String> {
        if self.sinks.is_empty() && !self.viewer.enabled {
            return Err("no sinks configured and viewer disabled: logs would be discarded".into());
        }
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

/// Parse a duration string like "5s", "100ms", "2m" outside of serde context.
fn parse_duration_str(s: &str) -> Result<Duration, Box<dyn std::error::Error>> {
    let s = s.trim();
    if let Some(rest) = s.strip_suffix("ms") {
        Ok(Duration::from_millis(rest.trim().parse()?))
    } else if let Some(rest) = s.strip_suffix('s') {
        Ok(Duration::from_secs(rest.trim().parse()?))
    } else if let Some(rest) = s.strip_suffix('m') {
        Ok(Duration::from_secs(rest.trim().parse::<u64>()? * 60))
    } else {
        Ok(Duration::from_secs(s.parse()?))
    }
}

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

    #[test]
    fn viewer_env_vars_parse() {
        let env = std::collections::HashMap::from([
            ("WATCHTOWER_VIEWER_ENABLED", "true"),
            ("WATCHTOWER_VIEWER_PORT", "9092"),
            ("WATCHTOWER_VIEWER_DB_PATH", "/data/logs.db"),
            ("WATCHTOWER_VIEWER_MAX_RECORDS", "500000"),
            ("WATCHTOWER_VIEWER_MAX_AGE", "3d"),
            ("WATCHTOWER_VIEWER_AUTH_USERNAME", "admin"),
            ("WATCHTOWER_VIEWER_AUTH_PASSWORD", "secret"),
        ]);
        let mut v = ViewerConfig::default();
        Config::apply_viewer_env(&mut v, |k| env.get(k).map(|s| s.to_string())).unwrap();
        assert!(v.enabled);
        assert_eq!(v.listen_addr, "[::]:9092"); // PORT binds all interfaces (Railway)
        assert_eq!(v.db_path, "/data/logs.db");
        assert_eq!(v.retention.max_records, 500_000);
        assert_eq!(v.retention.max_age, Duration::from_secs(3 * 86400)); // "3d" needs h/d parser
        let auth = v.auth.expect("auth should be set when both user+pass present");
        assert_eq!(auth.username, "admin");
        assert_eq!(auth.password, "secret");
    }

    #[test]
    fn viewer_env_listen_addr_overrides_port() {
        let env = std::collections::HashMap::from([
            ("WATCHTOWER_VIEWER_PORT", "9092"),
            ("WATCHTOWER_VIEWER_LISTEN_ADDR", "127.0.0.1:7000"),
        ]);
        let mut v = ViewerConfig::default();
        Config::apply_viewer_env(&mut v, |k| env.get(k).map(|s| s.to_string())).unwrap();
        assert_eq!(v.listen_addr, "127.0.0.1:7000");
    }

    #[test]
    fn viewer_env_partial_auth_is_ignored() {
        let env = std::collections::HashMap::from([("WATCHTOWER_VIEWER_AUTH_USERNAME", "admin")]);
        let mut v = ViewerConfig::default();
        Config::apply_viewer_env(&mut v, |k| env.get(k).map(|s| s.to_string())).unwrap();
        assert!(!v.enabled);
        assert!(v.auth.is_none()); // password missing -> no auth
    }
}
