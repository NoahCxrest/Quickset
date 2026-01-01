# build stage
FROM rust:1.75-slim as builder

WORKDIR /app

# copy manifests
COPY Cargo.toml Cargo.lock* ./

# create dummy src to cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release && rm -rf src

# copy actual source
COPY src ./src

# build for release
RUN touch src/main.rs && cargo build --release

# runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/quickset /app/quickset

# default environment
ENV QUICKSET_HOST=0.0.0.0
ENV QUICKSET_PORT=8080
ENV QUICKSET_AUTH=false
ENV QUICKSET_LOG=info

EXPOSE 8080

CMD ["./quickset"]
