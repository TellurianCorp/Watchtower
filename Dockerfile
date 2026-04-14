# Build stage
FROM rust:1.94-slim AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
RUN cargo build --release

# Runtime stage — minimal image
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/watchtower /usr/local/bin/watchtower

RUN mkdir -p /etc/watchtower /var/lib/watchtower

EXPOSE 9090 9091

# Health check for local Docker usage.
# Railway uses its own health check (configured in railway.toml).
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["curl", "-fs", "http://localhost:9091/healthz"]

ENTRYPOINT ["watchtower"]
CMD ["--config", "/etc/watchtower/watchtower.yaml"]
