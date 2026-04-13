# Deployment Guide

## Docker

### Dockerfile

```dockerfile
# Build stage
FROM rust:1.94-slim AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/watchtower /usr/local/bin/watchtower

# Create directory for spillover and config
RUN mkdir -p /etc/watchtower /var/lib/watchtower

EXPOSE 9090 9091

ENTRYPOINT ["watchtower"]
CMD ["--config", "/etc/watchtower/watchtower.yaml"]
```

### Build and run

```bash
docker build -t watchtower:latest .

docker run -d \
  --name watchtower \
  -p 9090:9090 \
  -p 9091:9091 \
  -v ./watchtower.yaml:/etc/watchtower/watchtower.yaml:ro \
  watchtower:latest
```

### With TLS certificates

```bash
docker run -d \
  --name watchtower \
  -p 9090:9090 \
  -p 9091:9091 \
  -v ./watchtower.yaml:/etc/watchtower/watchtower.yaml:ro \
  -v ./certs:/etc/watchtower/certs:ro \
  watchtower:latest
```

---

## Kubernetes

### Sidecar pattern (recommended)

Watchtower is designed to run as a sidecar container alongside your application
pod. This minimizes network latency and simplifies log routing.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9091"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        # Your application
        - name: app
          image: my-app:latest
          ports:
            - containerPort: 8080
          env:
            - name: WATCHTOWER_ADDR
              value: "localhost:9090"

        # Watchtower sidecar
        - name: watchtower
          image: watchtower:latest
          ports:
            - containerPort: 9090
              name: grpc
            - containerPort: 9091
              name: health
          volumeMounts:
            - name: watchtower-config
              mountPath: /etc/watchtower
              readOnly: true
            - name: watchtower-spillover
              mountPath: /var/lib/watchtower
          livenessProbe:
            httpGet:
              path: /healthz
              port: health
            initialDelaySeconds: 2
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /readyz
              port: health
            initialDelaySeconds: 1
            periodSeconds: 5
          resources:
            requests:
              cpu: "50m"
              memory: "32Mi"
            limits:
              cpu: "200m"
              memory: "128Mi"

      volumes:
        - name: watchtower-config
          configMap:
            name: watchtower-config
        - name: watchtower-spillover
          emptyDir:
            sizeLimit: 256Mi
```

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: watchtower-config
data:
  watchtower.yaml: |
    server:
      listen_addr: "[::]:9090"
      enable_compression: true

    pipeline:
      batch_size: 1024
      flush_interval: "2s"
      buffer_size: 8192
      workers: 2

    health:
      enabled: true
      listen_addr: "[::]:9091"

    spillover:
      enabled: true
      path: "/var/lib/watchtower/spillover.bin"

    sinks:
      - type: watchtower
        target: "watchtower-aggregator.logging:9090"
        enable_compression: true
        timeout: "10s"
        retry_attempts: 3
        retry_backoff: "1s"
```

### Aggregator deployment

For the central aggregator that receives from all sidecars and delivers to
Elasticsearch/OpenSearch:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: watchtower-aggregator
  namespace: logging
spec:
  replicas: 2
  selector:
    matchLabels:
      app: watchtower-aggregator
  template:
    metadata:
      labels:
        app: watchtower-aggregator
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9091"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: watchtower
          image: watchtower:latest
          ports:
            - containerPort: 9090
              name: grpc
            - containerPort: 9091
              name: health
          volumeMounts:
            - name: config
              mountPath: /etc/watchtower
              readOnly: true
            - name: spillover
              mountPath: /var/lib/watchtower
          livenessProbe:
            httpGet:
              path: /healthz
              port: health
          readinessProbe:
            httpGet:
              path: /readyz
              port: health
          resources:
            requests:
              cpu: "500m"
              memory: "256Mi"
            limits:
              cpu: "2000m"
              memory: "1Gi"
      volumes:
        - name: config
          configMap:
            name: watchtower-aggregator-config
        - name: spillover
          persistentVolumeClaim:
            claimName: watchtower-aggregator-spillover
---
apiVersion: v1
kind: Service
metadata:
  name: watchtower-aggregator
  namespace: logging
spec:
  selector:
    app: watchtower-aggregator
  ports:
    - port: 9090
      targetPort: grpc
      name: grpc
    - port: 9091
      targetPort: health
      name: health
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: watchtower-aggregator-config
  namespace: logging
data:
  watchtower.yaml: |
    server:
      listen_addr: "[::]:9090"
      max_concurrent_streams: 500

    pipeline:
      batch_size: 4096
      flush_interval: "1s"
      buffer_size: 65536
      workers: 8

    health:
      enabled: true

    spillover:
      enabled: true
      path: "/var/lib/watchtower/spillover.bin"

    sinks:
      - type: elasticsearch
        addresses:
          - "https://elasticsearch-master.elastic:9200"
        index: "watchtower-logs"
        username: "elastic"
        password: "changeme"
        tls: true
        batch_size: 2048
        flush_interval: "1s"
        retry_attempts: 5
        retry_backoff: "2s"
```

---

## Topology Patterns

### Pattern 1: Direct to Elasticsearch

Simplest setup. Each sidecar sends directly to ES.

```
┌─────────┐    ┌────────────┐    ┌───────────────┐
│  App 1  │───►│ Watchtower │───►│               │
└─────────┘    └────────────┘    │               │
┌─────────┐    ┌────────────┐    │ Elasticsearch │
│  App 2  │───►│ Watchtower │───►│               │
└─────────┘    └────────────┘    │               │
┌─────────┐    ┌────────────┐    │               │
│  App 3  │───►│ Watchtower │───►│               │
└─────────┘    └────────────┘    └───────────────┘
```

**Pros**: Simple, no aggregator to manage.
**Cons**: Many connections to ES, no centralized buffering.

### Pattern 2: Aggregator (recommended for production)

Sidecars forward to a central aggregator which batches efficiently.

```
┌─────────┐    ┌────────────┐
│  App 1  │───►│ Watchtower │──┐
└─────────┘    └────────────┘  │
┌─────────┐    ┌────────────┐  │    ┌─────────────┐    ┌───────────────┐
│  App 2  │───►│ Watchtower │──┼───►│ Watchtower  │───►│ Elasticsearch │
└─────────┘    └────────────┘  │    │ Aggregator  │    └───────────────┘
┌─────────┐    ┌────────────┐  │    └─────────────┘
│  App 3  │───►│ Watchtower │──┘
└─────────┘    └────────────┘
```

**Pros**: Fewer ES connections, centralized retry/buffering, easier to scale.
**Cons**: One more hop, aggregator is a single point of failure (mitigate with
replicas).

### Pattern 3: Fan-out

Each sidecar sends to both an aggregator and ES (for redundancy).

```
┌─────────┐    ┌────────────┐───►┌─────────────┐
│  App 1  │───►│ Watchtower │    │ Aggregator  │
└─────────┘    └─────┬──────┘    └─────────────┘
                     └──────────►┌───────────────┐
                                 │ Elasticsearch │
                                 └───────────────┘
```

---

## Resource Sizing

### Sidecar (per application pod)

| Resource | Request | Limit |
|----------|---------|-------|
| CPU | 50m | 200m |
| Memory | 32Mi | 128Mi |

The Watchtower binary itself uses ~2-5 MB RSS. The rest is for the pipeline
buffer and in-flight batches.

### Aggregator (central instance)

| Resource | Request | Limit | Notes |
|----------|---------|-------|-------|
| CPU | 500m | 2000m | Scales with throughput |
| Memory | 256Mi | 1Gi | Scales with buffer_size and batch_size |
| Disk | 1Gi PVC | — | For spillover buffer (PVC recommended) |

### Scaling rules of thumb

- **1 aggregator replica per 50K records/sec** sustained throughput
- **Increase `workers`** if CPU is the bottleneck (more parallel sink delivery)
- **Increase `buffer_size`** if you see `records_dropped` in metrics
- **Enable `spillover`** if you cannot tolerate any log loss

---

## Monitoring

### Prometheus scrape config

```yaml
scrape_configs:
  - job_name: 'watchtower'
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: true
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_port]
        action: replace
        target_label: __address__
        regex: (.+)
        replacement: ${1}
```

### Key alerts

```yaml
groups:
  - name: watchtower
    rules:
      - alert: WatchtowerRecordsDropped
        expr: rate(watchtower_records_dropped_total[5m]) > 0
        for: 2m
        annotations:
          summary: "Watchtower is dropping logs"
          description: "{{ $labels.instance }} is dropping {{ $value }}/sec"

      - alert: WatchtowerSinkErrors
        expr: rate(watchtower_sink_errors_total[5m]) > 0
        for: 5m
        annotations:
          summary: "Watchtower sink delivery failing"

      - alert: WatchtowerNotReady
        expr: up{job="watchtower"} == 0
        for: 1m
        annotations:
          summary: "Watchtower instance is down"
```
