FROM rust:1.92-slim-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src \
 && echo 'fn main() {}' > src/main.rs \
 && cargo build --release \
 && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 ssp \
 && useradd --system --uid 10001 --gid ssp --home-dir /data ssp \
 && install -d -o ssp -g ssp /data
COPY --from=builder /app/target/release/mutinynet-ssp /usr/local/bin/mutinynet-ssp
ENV SSP_DATA_DIR=/data
USER ssp
EXPOSE 5000
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl --fail --silent http://127.0.0.1:5000/health > /dev/null || exit 1
ENTRYPOINT ["mutinynet-ssp"]
