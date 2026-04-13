use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::metrics::Metrics;

/// HTTP server that exposes /healthz, /readyz, and /metrics endpoints.
pub struct HealthServer {
    ready: Arc<AtomicBool>,
    metrics: Metrics,
}

impl HealthServer {
    pub fn new(metrics: Metrics) -> (Self, ReadyHandle) {
        let ready = Arc::new(AtomicBool::new(false));
        let handle = ReadyHandle(Arc::clone(&ready));
        (Self { ready, metrics }, handle)
    }

    pub async fn serve(self, addr: SocketAddr, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                error!(error = %e, "health server bind failed");
                return;
            }
        };

        info!(%addr, "health/metrics server listening");

        let ready = self.ready;
        let metrics = self.metrics;

        loop {
            tokio::select! {
                accept = listener.accept() => {
                    match accept {
                        Ok((stream, _)) => {
                            let ready = Arc::clone(&ready);
                            let metrics = metrics.clone();
                            tokio::spawn(async move {
                                let io = TokioIo::new(stream);
                                let svc = service_fn(move |req| {
                                    let ready = Arc::clone(&ready);
                                    let metrics = metrics.clone();
                                    async move { handle_request(req, ready, metrics) }
                                });
                                if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                                    error!(error = %e, "health server connection error");
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "health server accept error");
                        }
                    }
                }
                _ = shutdown.changed() => {
                    info!("health server shutting down");
                    break;
                }
            }
        }
    }
}

/// Handle for marking the service as ready/not-ready.
#[derive(Clone)]
pub struct ReadyHandle(Arc<AtomicBool>);

impl ReadyHandle {
    pub fn set_ready(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn set_not_ready(&self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

fn handle_request(
    req: Request<hyper::body::Incoming>,
    ready: Arc<AtomicBool>,
    metrics: Metrics,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = match req.uri().path() {
        "/healthz" => Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::from("ok\n")))
            .unwrap(),

        "/readyz" => {
            if ready.load(Ordering::Relaxed) {
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Full::new(Bytes::from("ready\n")))
                    .unwrap()
            } else {
                Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(Full::new(Bytes::from("not ready\n")))
                    .unwrap()
            }
        }

        "/metrics" => {
            let body = metrics.render();
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain; version=0.0.4")
                .body(Full::new(Bytes::from(body)))
                .unwrap()
        }

        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("not found\n")))
            .unwrap(),
    };

    Ok(response)
}
