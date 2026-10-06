# syntax=docker/dockerfile:1.7

# Build stage. No --platform pin: buildx runs this stage under QEMU on the
# target arch (arm64) so the binary matches the runtime stage. Slower than
# native cross-compilation but avoids the linker dance for one binary.
FROM rust:1-slim AS builder

RUN apt-get update \
  && apt-get install -y --no-install-recommends cmake make perl pkg-config g++ libcurl4-openssl-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release --bin flagsmith-analytics && rm -rf src

# Build the actual binary
COPY src ./src
RUN touch src/main.rs && cargo build --release --bin flagsmith-analytics

# Runtime stage
FROM debian:trixie-slim

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/flagsmith-analytics /usr/local/bin/flagsmith-analytics

ENV LISTEN_ADDR=0.0.0.0:8080
EXPOSE 8080

RUN useradd --system --uid 10001 --no-create-home app
USER 10001:10001

ENTRYPOINT ["/usr/local/bin/flagsmith-analytics"]
