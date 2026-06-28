pub mod client;
pub mod config;
pub mod health;
pub mod metrics;
pub mod pipeline;
pub mod server;
pub mod sink;
pub mod spillover;
pub mod viewer;

/// Generated protobuf/gRPC types.
pub mod proto {
    tonic::include_proto!("watchtower.v1");
}
