use std::convert::Infallible;
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tracing::info;

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

    pub async fn serve(self, listener: TcpListener, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        info!(addr = ?listener.local_addr().ok(), "viewer server listening");
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
    let header = req.headers().get("authorization").and_then(|h| h.to_str().ok());
    auth_matches(header, auth)
}

/// Pure auth decision: `None` config = open; otherwise the header must carry the
/// matching `Basic <base64(user:pass)>` credential.
fn auth_matches(header: Option<&str>, auth: &Option<BasicAuthConfig>) -> bool {
    let Some(cfg) = auth else { return true };
    let expected = base64_encode(format!("{}:{}", cfg.username, cfg.password).as_bytes());
    header
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BasicAuthConfig;

    #[test]
    fn base64_known_vectors() {
        assert_eq!(base64_encode(b"a:b"), "YTpi");
        assert_eq!(base64_encode(b"admin:changeme"), "YWRtaW46Y2hhbmdlbWU=");
        assert_eq!(base64_encode(b"a"), "YQ==");
        assert_eq!(base64_encode(b"ab"), "YWI=");
    }

    #[test]
    fn auth_matches_logic() {
        assert!(auth_matches(None, &None));
        assert!(auth_matches(Some("anything"), &None));
        let cfg = Some(BasicAuthConfig { username: "admin".into(), password: "changeme".into() });
        assert!(auth_matches(Some("Basic YWRtaW46Y2hhbmdlbWU="), &cfg));   // correct
        assert!(!auth_matches(Some("Basic d3Jvbmc="), &cfg));               // wrong creds
        assert!(!auth_matches(None, &cfg));                                 // missing header
        assert!(!auth_matches(Some("YWRtaW46Y2hhbmdlbWU="), &cfg));         // missing "Basic " prefix
    }

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

    #[test]
    fn index_html_is_embedded() {
        assert!(INDEX_HTML.contains("Watchtower"));
        assert!(INDEX_HTML.contains("/api/logs"));
    }
}
