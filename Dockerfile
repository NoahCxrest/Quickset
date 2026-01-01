# build stage
FROM rust:1.75-slim AS builder

WORKDIR /app

# copy source
COPY Cargo.toml Cargo.lock* ./
COPY src ./src

# create dummy bench file so cargo doesn't complain
RUN mkdir -p benches && echo "fn main() {}" > benches/search_benchmarks.rs

# build for release
RUN cargo build --release --bin quickset

# runtime stage - small as fuck
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/quickset /app/quickset

# default environment
ENV QUICKSET_HOST=0.0.0.0
ENV QUICKSET_PORT=8080
ENV QUICKSET_AUTH_LEVEL=none
ENV QUICKSET_LOG=info

EXPOSE 8080

CMD ["./quickset"]
