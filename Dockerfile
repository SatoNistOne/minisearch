FROM rust:1-slim-trixie AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && touch src/lib.rs && cargo build --release && rm -rf src
COPY src src
COPY web web
RUN touch src/main.rs src/lib.rs && cargo build --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN useradd --system --uid 10001 --create-home app && mkdir -p /data && chown app:app /data
COPY --from=builder /app/target/release/minisearch /usr/local/bin/minisearch
COPY data/sample /app/data/sample
WORKDIR /app
USER app
ENTRYPOINT ["minisearch"]
