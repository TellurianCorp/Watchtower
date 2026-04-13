use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time;
use tracing::{error, info, warn};

use crate::proto::LogBatch;
use crate::sink::Sink;

/// Pipeline receives log batches from the gRPC server and fans them out
/// to all configured sinks. Uses a bounded async channel to provide
/// backpressure without blocking the gRPC accept loop.
pub struct Pipeline {
    tx: mpsc::Sender<LogBatch>,
    workers: Vec<tokio::task::JoinHandle<()>>,
}

impl Pipeline {
    /// Create a new pipeline with the given buffer size, worker count,
    /// flush interval, and set of sinks.
    pub fn new(
        buffer_size: usize,
        workers: usize,
        batch_size: usize,
        flush_interval: Duration,
        sinks: Vec<Arc<dyn Sink>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<LogBatch>(buffer_size);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));

        let mut handles = Vec::with_capacity(workers);
        for id in 0..workers {
            let rx = Arc::clone(&rx);
            let sinks = sinks.clone();
            let handle = tokio::spawn(worker_loop(id, rx, sinks, batch_size, flush_interval));
            handles.push(handle);
        }

        info!(workers, buffer_size, "pipeline started");

        Self {
            tx,
            workers: handles,
        }
    }

    /// Submit a log batch into the pipeline. Returns false if the
    /// pipeline buffer is full (backpressure signal to the caller).
    pub fn submit(&self, batch: LogBatch) -> bool {
        match self.tx.try_send(batch) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("pipeline buffer full, dropping batch");
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                error!("pipeline channel closed");
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
) {
    let mut pending: Vec<crate::proto::LogRecord> = Vec::with_capacity(batch_size);
    let mut flush_timer = time::interval(flush_interval);
    flush_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        let maybe_batch = {
            // Hold the lock only long enough to try_recv or await.
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
                    flush(&sinks, &mut pending, id).await;
                }
            }
            None => {
                // Either timer fired or channel closed. Flush what we have.
                if !pending.is_empty() {
                    flush(&sinks, &mut pending, id).await;
                }
                // Check if the channel is actually closed (sender dropped).
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
async fn flush(sinks: &[Arc<dyn Sink>], pending: &mut Vec<crate::proto::LogRecord>, worker_id: usize) {
    if pending.is_empty() {
        return;
    }

    let batch = LogBatch {
        records: std::mem::take(pending),
        metadata: Default::default(),
    };

    for sink in sinks {
        if let Err(e) = sink.send(batch.clone()).await {
            error!(worker = worker_id, sink = sink.name(), error = %e, "sink delivery failed");
        }
    }
}
