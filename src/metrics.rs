use prometheus::{
    Encoder, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};

/// Metrics collects Prometheus counters/gauges for Watchtower internals.
/// All fields are atomically updated — no locks on the hot path.
#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,

    // Ingestion
    pub records_received: IntCounter,
    pub batches_received: IntCounter,
    pub records_dropped: IntCounter,

    // Pipeline
    pub pipeline_buffer_used: IntGauge,
    pub pipeline_flushes: IntCounter,

    // Sink delivery
    pub sink_records_sent: IntCounterVec,
    pub sink_errors: IntCounterVec,
    pub sink_retries: IntCounterVec,

    // Server
    pub active_streams: IntGauge,
    pub grpc_errors: IntCounter,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let records_received = IntCounter::with_opts(
            Opts::new("watchtower_records_received_total", "Total log records received via gRPC"),
        )
        .unwrap();

        let batches_received = IntCounter::with_opts(
            Opts::new("watchtower_batches_received_total", "Total batches received via gRPC"),
        )
        .unwrap();

        let records_dropped = IntCounter::with_opts(
            Opts::new("watchtower_records_dropped_total", "Records dropped due to backpressure"),
        )
        .unwrap();

        let pipeline_buffer_used = IntGauge::with_opts(
            Opts::new("watchtower_pipeline_buffer_used", "Current pipeline buffer occupancy"),
        )
        .unwrap();

        let pipeline_flushes = IntCounter::with_opts(
            Opts::new("watchtower_pipeline_flushes_total", "Total pipeline flushes to sinks"),
        )
        .unwrap();

        let sink_records_sent = IntCounterVec::new(
            Opts::new("watchtower_sink_records_sent_total", "Records sent per sink"),
            &["sink"],
        )
        .unwrap();

        let sink_errors = IntCounterVec::new(
            Opts::new("watchtower_sink_errors_total", "Delivery errors per sink"),
            &["sink"],
        )
        .unwrap();

        let sink_retries = IntCounterVec::new(
            Opts::new("watchtower_sink_retries_total", "Retry attempts per sink"),
            &["sink"],
        )
        .unwrap();

        let active_streams = IntGauge::with_opts(
            Opts::new("watchtower_active_streams", "Active gRPC streaming connections"),
        )
        .unwrap();

        let grpc_errors = IntCounter::with_opts(
            Opts::new("watchtower_grpc_errors_total", "gRPC-level errors"),
        )
        .unwrap();

        // Register all metrics.
        registry.register(Box::new(records_received.clone())).unwrap();
        registry.register(Box::new(batches_received.clone())).unwrap();
        registry.register(Box::new(records_dropped.clone())).unwrap();
        registry.register(Box::new(pipeline_buffer_used.clone())).unwrap();
        registry.register(Box::new(pipeline_flushes.clone())).unwrap();
        registry.register(Box::new(sink_records_sent.clone())).unwrap();
        registry.register(Box::new(sink_errors.clone())).unwrap();
        registry.register(Box::new(sink_retries.clone())).unwrap();
        registry.register(Box::new(active_streams.clone())).unwrap();
        registry.register(Box::new(grpc_errors.clone())).unwrap();

        Self {
            registry,
            records_received,
            batches_received,
            records_dropped,
            pipeline_buffer_used,
            pipeline_flushes,
            sink_records_sent,
            sink_errors,
            sink_retries,
            active_streams,
            grpc_errors,
        }
    }

    /// Render all metrics in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&metric_families, &mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
