# syntax=docker/dockerfile:1.6
#
# Official dbmazz runtime image.
#
# Context: this Dockerfile is NOT used to compile dbmazz. The binary is
# pre-compiled by the release workflow with `cargo build --release
# --target x86_64-unknown-linux-musl` (and aarch64 equivalent), and the
# resulting binaries are placed in the build context as
# `dbmazz-linux-amd64` and `dbmazz-linux-arm64`. `docker buildx build
# --platform linux/amd64,linux/arm64` then picks the correct one via
# TARGETARCH.
#
# The binary is fully static (musl libc + vendored openssl + static curl),
# so the base image only needs `ca-certificates` for TLS trust anchors
# (Snowflake sink uses rustls with webpki-roots so this is belt-and-
# suspenders) and `curl` for the HEALTHCHECK command.
FROM debian:bookworm-slim

ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --uid 65532 dbmazz

COPY dbmazz-linux-${TARGETARCH} /usr/local/bin/dbmazz
RUN chmod +x /usr/local/bin/dbmazz

USER dbmazz

# 8080 = HTTP API + web UI (feature http-api, now default)
# 50051 = gRPC server
EXPOSE 8080 50051

HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
  CMD curl -sf http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/dbmazz"]
