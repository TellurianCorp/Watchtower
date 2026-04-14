use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tokio::signal;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tracing::{error, info, warn};

use watchtower::config::{Config, SinkConfig};
use watchtower::health::HealthServer;
use watchtower::metrics::Metrics;
use watchtower::pipeline::Pipeline;
use watchtower::proto::watchtower_service_server::WatchtowerServiceServer;
use watchtower::server::WatchtowerServer;
use watchtower::sink::elastic::ElasticSink;
use watchtower::sink::forward::ForwardSink;
use watchtower::sink::Sink;
use watchtower::spillover::SpilloverBuffer;

#[derive(Parser)]
#[command(name = "watchtower", about = "High-performance gRPC log collection sidecar")]
struct Cli {
    /// Path to the configuration file.
    #[arg(short, long, default_value = "watchtower.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured logging for the agent itself.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .json()
        .init();

    let cli = Cli::parse();

    // Load config: try YAML file first, fall back to env vars (for Railway/containers).
    let cfg = if std::path::Path::new(&cli.config).exists() {
        info!(path = cli.config.as_str(), "loading config from file");
        Config::load(&cli.config)?
    } else {
        warn!(
            path = cli.config.as_str(),
            "config file not found, configuring from environment variables"
        );
        Config::from_env()?
    };

    info!(
        listen_addr = cfg.server.listen_addr.as_str(),
        sinks = cfg.sinks.len(),
        workers = cfg.pipeline.workers,
        "starting watchtower"
    );

    // --- Metrics ---
    let metrics = Metrics::new();

    // --- Shutdown coordination ---
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // --- Health/Metrics HTTP server ---
    let ready_handle = if cfg.health.enabled {
        let health_addr: SocketAddr = cfg.health.listen_addr.parse()?;
        let (health_server, ready_handle) = HealthServer::new(metrics.clone());
        let health_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            health_server.serve(health_addr, health_shutdown_rx).await;
        });
        Some(ready_handle)
    } else {
        None
    };

    // --- Spillover buffer ---
    let spillover = if cfg.spillover.enabled {
        match SpilloverBuffer::new(&cfg.spillover.path) {
            Ok(buf) => {
                info!(path = cfg.spillover.path.as_str(), "spillover buffer enabled");
                Some(Arc::new(buf))
            }
            Err(e) => {
                error!(error = %e, "failed to open spillover buffer, continuing without");
                None
            }
        }
    } else {
        None
    };

    // --- Build sinks ---
    let sinks = build_sinks(&cfg.sinks)?;

    // --- Build pipeline ---
    let pipeline = Arc::new(Pipeline::new(
        cfg.pipeline.buffer_size,
        cfg.pipeline.workers,
        cfg.pipeline.batch_size,
        cfg.pipeline.flush_interval,
        sinks,
        metrics.clone(),
        spillover.clone(),
    ));

    // --- Replay any pending spillover records ---
    if let Some(ref spill) = spillover {
        if spill.has_pending() {
            info!("replaying spilled records from disk...");
            let pipeline_ref = Arc::clone(&pipeline);
            let spill_ref = Arc::clone(spill);
            tokio::task::spawn_blocking(move || {
                let _ = spill_ref.replay(|batch| pipeline_ref.submit(batch));
            })
            .await?;
        }
    }

    // --- Build gRPC server ---
    let grpc_server = WatchtowerServer::new(Arc::clone(&pipeline), metrics.clone());

    let addr: SocketAddr = cfg.server.listen_addr.parse()?;

    let svc = WatchtowerServiceServer::new(grpc_server)
        .max_decoding_message_size(cfg.server.max_recv_msg_size)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .send_compressed(tonic::codec::CompressionEncoding::Gzip);

    let mut server_builder = Server::builder();

    // --- TLS / mTLS ---
    if let (Some(cert_path), Some(key_path)) = (&cfg.server.tls_cert, &cfg.server.tls_key) {
        let cert = tokio::fs::read(cert_path).await?;
        let key = tokio::fs::read(key_path).await?;
        let identity = Identity::from_pem(cert, key);

        let mut tls_config = ServerTlsConfig::new().identity(identity);

        if let Some(ca_path) = &cfg.server.tls_ca {
            let ca = tokio::fs::read(ca_path).await?;
            let ca_cert = tonic::transport::Certificate::from_pem(ca);
            tls_config = tls_config.client_ca_root(ca_cert);
            info!("mTLS enabled (client certificate verification)");
        } else {
            info!("TLS enabled (server-side only)");
        }

        server_builder = server_builder.tls_config(tls_config)?;
    }

    let router = server_builder
        .max_concurrent_streams(cfg.server.max_concurrent_streams)
        .add_service(svc);

    // Mark ready once the server is about to accept connections.
    if let Some(ref rh) = ready_handle {
        rh.set_ready();
    }

    info!(%addr, "gRPC server listening");

    // --- Run server with graceful shutdown ---
    let shutdown_pipeline = pipeline;
    router
        .serve_with_shutdown(addr, async {
            shutdown_signal().await;
            info!("shutdown signal received");
        })
        .await?;

    // Signal health server to stop.
    let _ = shutdown_tx.send(true);

    if let Some(ref rh) = ready_handle {
        rh.set_not_ready();
    }

    // Drain pipeline after server stops accepting.
    info!("draining pipeline...");
    match Arc::try_unwrap(shutdown_pipeline) {
        Ok(p) => p.shutdown().await,
        Err(_) => error!("could not unwrap pipeline Arc for shutdown"),
    }

    info!("watchtower stopped");
    Ok(())
}

fn build_sinks(configs: &[SinkConfig]) -> Result<Vec<Arc<dyn Sink>>, Box<dyn std::error::Error>> {
    let mut sinks: Vec<Arc<dyn Sink>> = Vec::new();

    for (i, cfg) in configs.iter().enumerate() {
        match cfg {
            SinkConfig::Elasticsearch(c) => {
                let label = format!("elasticsearch-{i}");
                let sink = ElasticSink::new(&label, c)?;
                info!(sink = label.as_str(), addresses = ?c.addresses, "registered sink");
                sinks.push(Arc::new(sink));
            }
            SinkConfig::OpenSearch(c) => {
                let label = format!("opensearch-{i}");
                let sink = ElasticSink::new(&label, c)?;
                info!(sink = label.as_str(), addresses = ?c.addresses, "registered sink");
                sinks.push(Arc::new(sink));
            }
            SinkConfig::Watchtower(c) => {
                let sink = ForwardSink::new(c)?;
                info!(target = c.target.as_str(), "registered watchtower forward sink");
                sinks.push(Arc::new(sink));
            }
        }
    }

    Ok(sinks)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
