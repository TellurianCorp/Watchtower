pub mod config;
pub mod pipeline;
pub mod server;
pub mod sink;

/// Generated protobuf/gRPC types.
pub mod proto {
    tonic::include_proto!("watchtower.v1");
}
