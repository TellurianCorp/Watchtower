use std::sync::Arc;

use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error};

use crate::metrics::Metrics;
use crate::pipeline::Pipeline;
use crate::proto::watchtower_service_server::WatchtowerService;
use crate::proto::{IngestResponse, LogBatch};

/// WatchtowerServer implements the gRPC WatchtowerService.
pub struct WatchtowerServer {
    pipeline: Arc<Pipeline>,
    metrics: Metrics,
}

impl WatchtowerServer {
    pub fn new(pipeline: Arc<Pipeline>, metrics: Metrics) -> Self {
        Self { pipeline, metrics }
    }
}

#[tonic::async_trait]
impl WatchtowerService for WatchtowerServer {
    /// Unary ingestion: receive a single batch, push to pipeline, ack.
    async fn ingest(
        &self,
        request: Request<LogBatch>,
    ) -> Result<Response<IngestResponse>, Status> {
        let batch = request.into_inner();
        let count = batch.records.len() as i64;

        if count == 0 {
            return Ok(Response::new(IngestResponse {
                accepted_count: 0,
                errors: Default::default(),
            }));
        }

        self.metrics.batches_received.inc();
        self.metrics.records_received.inc_by(count as u64);

        if self.pipeline.submit(batch) {
            debug!(records = count, "ingested batch");
            Ok(Response::new(IngestResponse {
                accepted_count: count,
                errors: Default::default(),
            }))
        } else {
            self.metrics.records_dropped.inc_by(count as u64);
            Err(Status::resource_exhausted("pipeline buffer full"))
        }
    }

    type IngestStreamStream =
        tokio_stream::wrappers::ReceiverStream<Result<IngestResponse, Status>>;

    /// Bidirectional streaming ingestion: receive a stream of batches,
    /// ack each one on the response stream.
    async fn ingest_stream(
        &self,
        request: Request<Streaming<LogBatch>>,
    ) -> Result<Response<Self::IngestStreamStream>, Status> {
        let mut stream = request.into_inner();
        let pipeline = Arc::clone(&self.pipeline);
        let metrics = self.metrics.clone();

        metrics.active_streams.inc();

        let (tx, rx) = tokio::sync::mpsc::channel(128);

        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(batch) => {
                        let count = batch.records.len() as i64;
                        metrics.batches_received.inc();
                        metrics.records_received.inc_by(count as u64);

                        let resp = if pipeline.submit(batch) {
                            debug!(records = count, "stream: ingested batch");
                            Ok(IngestResponse {
                                accepted_count: count,
                                errors: Default::default(),
                            })
                        } else {
                            metrics.records_dropped.inc_by(count as u64);
                            Err(Status::resource_exhausted("pipeline buffer full"))
                        };

                        if tx.send(resp).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "stream receive error");
                        metrics.grpc_errors.inc();
                        break;
                    }
                }
            }
            metrics.active_streams.dec();
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}
