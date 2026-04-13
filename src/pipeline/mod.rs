use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time;
use tracing::{error, info, warn};

use crate::metrics::Metrics;
use crate::proto::LogBatch;
use crate::sink::Sink;
use crate::spillover::SpilloverBuffer;

/// Pipeline receives log batches from the gRPC server and fans them out
/// to all configured sinks. Uses a bounded async channel to provide
/// backpressure without blocking the gRPC accept loop.
pub struct Pipeline {
    tx: mpsc::Sender<LogBatch>,
    workers: Vec<tokio::task::JoinHandle<()>>,
    metrics: Metrics,
    spillover: Option<Arc<SpilloverBuffer>>,
}

impl Pipeline {
    /// Create a new pipeline with the given buffer size, worker count,
    /// flush interval, set of sinks, metrics, and optional spillover.
    pub fn new(
        buffer_size: usize,
        workers: usize,
        batch_size: usize,
        flush_interval: Duration,
        sinks: Vec<Arc<dyn Sink>>,
        metrics: Metrics,
        spillover: Option<Arc<SpilloverBuffer>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<LogBatch>(buffer_size);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));

        let mut handles = Vec::with_capacity(workers);
        for id in 0..workers {
            let rx = Arc::clone(&rx);
            let sinks = sinks.clone();
            let m = metrics.clone();
            let handle = tokio::spawn(worker_loop(id, rx, sinks, batch_size, flush_interval, m));
            handles.push(handle);
        }

        info!(workers, buffer_size, "pipeline started");

        Self {
            tx,
            workers: handles,
            metrics,
            spillover,
        }
    }

    /// Submit a log batch into the pipeline. Returns false if the
    /// pipeline buffer is full (backpressure signal to the caller).
    /// When spillover is enabled, full-buffer batches are written to disk
    /// instead of being dropped.
    pub fn submit(&self, batch: LogBatch) -> bool {
        match self.tx.try_send(batch) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(batch)) => {
                // Try to spill to disk before dropping.
                if let Some(ref spill) = self.spillover {
                    if let Err(e) = spill.append(&batch) {
                        error!(error = %e, "spillover write failed, dropping batch");
                        self.metrics.records_dropped.inc_by(batch.records.len() as u64);
                        return false;
                    }
                    warn!("pipeline buffer full, spilled to disk");
                    return true;
                }

                warn!("pipeline buffer full, dropping batch");
                self.metrics.records_dropped.inc_by(batch.records.len() as u64);
                false
            }
            Err(mpsc::error::TrySendError::Closed(batch)) => {
                error!("pipeline channel closed");
                self.metrics.records_dropped.inc_by(batch.records.len() as u64);
                false
            }
        }
    }

    /// Gracefully shut down: close the channel and wait for workers
    /// to drain remaining batches.
    pub async fn shutdown(self) {
        drop(self.tx);
        for handle in self.workers {
            let _ = handle.await;
        }
        info!("pipeline shutdown complete");
    }
}

/// Each worker pulls batches from the shared receiver, re-batches them
/// into sink-optimal sizes, and fans out to every sink.
async fn worker_loop(
    id: usize,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<LogBatch>>>,
    sinks: Vec<Arc<dyn Sink>>,
    batch_size: usize,
    flush_interval: Duration,
    metrics: Metrics,
) {
    let mut pending: Vec<crate::proto::LogRecord> = Vec::with_capacity(batch_size);
    let mut flush_timer = time::interval(flush_interval);
    flush_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        let maybe_batch = {
            let mut guard = rx.lock().await;
            tokio::select! {
                batch = guard.recv() => batch,
                _ = flush_timer.tick() => None,
            }
        };

        match maybe_batch {
            Some(batch) => {
                pending.extend(batch.records);
                if pending.len() >= batch_size {
                    flush(&sinks, &mut pending, id, &metrics).await;
                }
            }
            None => {
                if !pending.is_empty() {
                    flush(&sinks, &mut pending, id, &metrics).await;
                }
                let guard = rx.lock().await;
                if guard.is_closed() && pending.is_empty() {
                    break;
                }
            }
        }
    }

    info!(worker = id, "worker exiting");
}

/// Flush pending records to all sinks.
async fn flush(
    sinks: &[Arc<dyn Sink>],
    pending: &mut Vec<crate::proto::LogRecord>,
    worker_id: usize,
    metrics: &Metrics,
) {
    if pending.is_empty() {
        return;
    }

    let batch = LogBatch {
        records: std::mem::take(pending),
        metadata: Default::default(),
    };

    let record_count = batch.records.len() as u64;
    metrics.pipeline_flushes.inc();

    for sink in sinks {
        if let Err(e) = sink.send(batch.clone()).await {
            error!(worker = worker_id, sink = sink.name(), error = %e, "sink delivery failed");
            metrics.sink_errors.with_label_values(&[sink.name()]).inc();
        } else {
            metrics
                .sink_records_sent
                .with_label_values(&[sink.name()])
                .inc_by(record_count);
        }
    }
}
