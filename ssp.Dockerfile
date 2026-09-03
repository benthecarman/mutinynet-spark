FROM rust:1.92-slim-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/mutinynet-ssp /usr/local/bin/mutinynet-ssp
EXPOSE 5000
ENTRYPOINT ["mutinynet-ssp"]
