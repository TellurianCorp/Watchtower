use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tonic::transport::Server;

use watchtower::metrics::Metrics;
use watchtower::pipeline::Pipeline;
use watchtower::proto::watchtower_service_client::WatchtowerServiceClient;
use watchtower::proto::watchtower_service_server::WatchtowerServiceServer;
use watchtower::proto::{
    AnyValue, KeyValue, LogBatch, LogRecord, Resource, Severity, any_value,
};
use watchtower::server::WatchtowerServer;
use watchtower::sink::{Sink, SinkError};

// --- Mock sink that captures batches ---

#[derive(Clone)]
struct MockSink {
    name: String,
    received: Arc<Mutex<Vec<LogBatch>>>,
}

impl MockSink {
    fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            received: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn received_records(&self) -> usize {
        self.received
            .lock()
            .await
            .iter()
            .map(|b| b.records.len())
            .sum()
    }
}

#[async_trait::async_trait]
impl Sink for MockSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, batch: LogBatch) -> Result<(), SinkError> {
        self.received.lock().await.push(batch);
        Ok(())
    }

    async fn close(&self) -> Result<(), SinkError> {
        Ok(())
    }
}

// --- Helpers ---

fn make_record(i: usize) -> LogRecord {
    LogRecord {
        timestamp: Some(prost_types::Timestamp {
            seconds: 1700000000 + i as i64,
            nanos: 123_000_000,
        }),
        severity: Severity::Info as i32,
        body: format!("test log message {i}"),
        attributes: vec![KeyValue {
            key: "request_id".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(format!("req-{i}"))),
            }),
        }],
        resource: Some(Resource {
            service_name: "test-service".into(),
            host_name: "localhost".into(),
            instance_id: "test-1".into(),
            attributes: vec![],
        }),
        trace_id: vec![],
        span_id: vec![],
        schema_url: String::new(),
    }
}

fn make_batch(n: usize) -> LogBatch {
    LogBatch {
        records: (0..n).map(make_record).collect(),
        metadata: HashMap::new(),
    }
}

async fn start_test_server(
    sinks: Vec<Arc<dyn Sink>>,
) -> (SocketAddr, Arc<Pipeline>, Metrics) {
    let metrics = Metrics::new();
    let pipeline = Arc::new(Pipeline::new(
        128,
        2,
        16,
        Duration::from_millis(100),
        sinks,
        metrics.clone(),
        None,
    ));

    let server = WatchtowerServer::new(Arc::clone(&pipeline), metrics.clone());
    let svc = WatchtowerServiceServer::new(server)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .send_compressed(tonic::codec::CompressionEncoding::Gzip);

    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give the server a moment to start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    (addr, pipeline, metrics)
}

// --- Tests ---

#[tokio::test]
async fn test_unary_ingest() {
    let mock = MockSink::new("test-sink");
    let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(mock.clone())];

    let (addr, _pipeline, metrics) = start_test_server(sinks).await;

    let mut client = WatchtowerServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    let batch = make_batch(10);
    let resp = client.ingest(batch).await.unwrap().into_inner();

    assert_eq!(resp.accepted_count, 10);
    assert!(resp.errors.is_empty());

    // Wait for pipeline flush.
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(mock.received_records().await, 10);
    assert_eq!(metrics.records_received.get(), 10);
    assert_eq!(metrics.batches_received.get(), 1);
}

#[tokio::test]
async fn test_empty_batch_returns_zero() {
    let mock = MockSink::new("test-sink");
    let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(mock.clone())];

    let (addr, _pipeline, _metrics) = start_test_server(sinks).await;

    let mut client = WatchtowerServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    let batch = LogBatch {
        records: vec![],
        metadata: HashMap::new(),
    };
    let resp = client.ingest(batch).await.unwrap().into_inner();

    assert_eq!(resp.accepted_count, 0);
}

#[tokio::test]
async fn test_multiple_batches() {
    let mock = MockSink::new("multi-sink");
    let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(mock.clone())];

    let (addr, _pipeline, metrics) = start_test_server(sinks).await;

    let mut client = WatchtowerServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    for _ in 0..5 {
        let batch = make_batch(20);
        let resp = client.ingest(batch).await.unwrap().into_inner();
        assert_eq!(resp.accepted_count, 20);
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(mock.received_records().await, 100);
    assert_eq!(metrics.batches_received.get(), 5);
    assert_eq!(metrics.records_received.get(), 100);
}

#[tokio::test]
async fn test_streaming_ingest() {
    let mock = MockSink::new("stream-sink");
    let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(mock.clone())];

    let (addr, _pipeline, metrics) = start_test_server(sinks).await;

    let mut client = WatchtowerServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    let batches = vec![make_batch(5), make_batch(3), make_batch(7)];
    let stream = tokio_stream::iter(batches);

    let resp_stream = client.ingest_stream(stream).await.unwrap();
    let mut resp_inner = resp_stream.into_inner();

    let mut total_accepted = 0i64;
    while let Some(Ok(resp)) = tokio_stream::StreamExt::next(&mut resp_inner).await {
        total_accepted += resp.accepted_count;
    }

    assert_eq!(total_accepted, 15);

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(mock.received_records().await, 15);
    assert_eq!(metrics.records_received.get(), 15);
}

#[tokio::test]
async fn test_fan_out_to_multiple_sinks() {
    let sink_a = MockSink::new("sink-a");
    let sink_b = MockSink::new("sink-b");
    let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(sink_a.clone()), Arc::new(sink_b.clone())];

    let (addr, _pipeline, _metrics) = start_test_server(sinks).await;

    let mut client = WatchtowerServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    let batch = make_batch(8);
    client.ingest(batch).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Both sinks should receive the same records.
    assert_eq!(sink_a.received_records().await, 8);
    assert_eq!(sink_b.received_records().await, 8);
}

#[tokio::test]
async fn test_pipeline_submit_and_flush() {
    let mock = MockSink::new("direct-sink");
    let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(mock.clone())];
    let metrics = Metrics::new();

    let pipeline = Pipeline::new(
        64,
        1,
        4, // small batch_size to trigger flushes quickly
        Duration::from_millis(50),
        sinks,
        metrics.clone(),
        None,
    );

    // Submit two batches of 3 records each (below batch_size of 4).
    assert!(pipeline.submit(make_batch(3)));
    assert!(pipeline.submit(make_batch(3)));

    // Wait for flush timer.
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(mock.received_records().await, 6);
    assert!(metrics.pipeline_flushes.get() >= 1);

    pipeline.shutdown().await;
}

#[tokio::test]
async fn test_metrics_prometheus_render() {
    let metrics = Metrics::new();
    metrics.records_received.inc_by(42);
    metrics.batches_received.inc_by(3);

    let output = metrics.render();

    assert!(output.contains("watchtower_records_received_total 42"));
    assert!(output.contains("watchtower_batches_received_total 3"));
    assert!(output.contains("watchtower_records_dropped_total 0"));
}

#[tokio::test]
async fn test_viewer_stores_and_serves_logs() {
    use std::sync::Arc;
    use watchtower::sink::store::{LogStore, StoreSink};
    use watchtower::viewer::ViewerServer;

    // Shared in-memory store, used by both the sink and the viewer.
    let store = Arc::new(LogStore::open(":memory:").unwrap());
    let store_sink = StoreSink::new(Arc::clone(&store));
    let sinks: Vec<Arc<dyn Sink>> = vec![Arc::new(store_sink)];

    let (grpc_addr, _pipeline, _metrics) = start_test_server(sinks).await;

    // Start the viewer on an ephemeral port.
    let viewer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let viewer_addr = viewer_listener.local_addr().unwrap();
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let viewer = ViewerServer::new(Arc::clone(&store), None);
    tokio::spawn(async move { viewer.serve(viewer_listener, rx).await; });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Ingest over gRPC.
    let mut client = WatchtowerServiceClient::connect(format!("http://{grpc_addr}")).await.unwrap();
    client.ingest(make_batch(5)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await; // pipeline flush + store insert

    // Query via the viewer HTTP API.
    let body = reqwest::get(format!("http://{viewer_addr}/api/logs"))
        .await.unwrap().text().await.unwrap();
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["records"].as_array().unwrap().len(), 5);

    // The UI page is served at /.
    let page = reqwest::get(format!("http://{viewer_addr}/")).await.unwrap();
    assert_eq!(page.status(), 200);
    assert!(page.text().await.unwrap().contains("Watchtower Logs"));
}

#[tokio::test]
async fn test_viewer_basic_auth() {
    use std::sync::Arc;
    use watchtower::config::BasicAuthConfig;
    use watchtower::sink::store::LogStore;
    use watchtower::viewer::ViewerServer;

    let store = Arc::new(LogStore::open(":memory:").unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let auth = Some(BasicAuthConfig { username: "admin".into(), password: "changeme".into() });
    let viewer = ViewerServer::new(Arc::clone(&store), auth);
    tokio::spawn(async move { viewer.serve(listener, rx).await; });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let no_creds = client.get(format!("http://{addr}/api/logs")).send().await.unwrap();
    assert_eq!(no_creds.status(), 401);
    let with_creds = client
        .get(format!("http://{addr}/api/logs"))
        .basic_auth("admin", Some("changeme"))
        .send().await.unwrap();
    assert_eq!(with_creds.status(), 200);
}
