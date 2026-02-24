FROM rust:1.91-bookworm as builder
RUN apt-get update && apt-get install -y protobuf-compiler git && rm -rf /var/lib/apt/lists/*
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
WORKDIR /app
COPY . .
ARG CARGO_FEATURES=""
RUN cargo build --release --features "${CARGO_FEATURES}"

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 libcurl4 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/dbmazz /usr/local/bin/dbmazz
ENTRYPOINT ["dbmazz"]
