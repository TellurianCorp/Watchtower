use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tokio::signal;
use tonic::transport::Server;
use tracing::{error, info};

use watchtower::config::{Config, SinkConfig};
use watchtower::pipeline::Pipeline;
use watchtower::proto::watchtower_service_server::WatchtowerServiceServer;
use watchtower::server::WatchtowerServer;
use watchtower::sink::elastic::ElasticSink;
use watchtower::sink::forward::ForwardSink;
use watchtower::sink::Sink;

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
    let cfg = Config::load(&cli.config)?;

    info!(
        listen_addr = cfg.server.listen_addr.as_str(),
        sinks = cfg.sinks.len(),
        workers = cfg.pipeline.workers,
        "starting watchtower"
    );

    // Build sinks.
    let sinks = build_sinks(&cfg.sinks)?;

    // Build pipeline.
    let pipeline = Arc::new(Pipeline::new(
        cfg.pipeline.buffer_size,
        cfg.pipeline.workers,
        cfg.pipeline.batch_size,
        cfg.pipeline.flush_interval,
        sinks,
    ));

    // Build gRPC server.
    let grpc_server = WatchtowerServer::new(Arc::clone(&pipeline));

    let addr: SocketAddr = cfg.server.listen_addr.parse()?;

    let svc = WatchtowerServiceServer::new(grpc_server)
        .max_decoding_message_size(cfg.server.max_recv_msg_size)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .send_compressed(tonic::codec::CompressionEncoding::Gzip);

    let router = Server::builder()
        .max_concurrent_streams(cfg.server.max_concurrent_streams)
        .add_service(svc);

    info!(%addr, "gRPC server listening");

    // Run server with graceful shutdown on SIGINT/SIGTERM.
    let shutdown_pipeline = pipeline;
    router
        .serve_with_shutdown(addr, async {
            shutdown_signal().await;
            info!("shutdown signal received");
        })
        .await?;

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
