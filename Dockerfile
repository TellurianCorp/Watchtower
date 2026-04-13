# Build stage
FROM rust:1.94-slim AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .
RUN cargo build --release

# Runtime stage — minimal image
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/watchtower /usr/local/bin/watchtower

RUN mkdir -p /etc/watchtower /var/lib/watchtower

EXPOSE 9090 9091

ENTRYPOINT ["watchtower"]
CMD ["--config", "/etc/watchtower/watchtower.yaml"]
