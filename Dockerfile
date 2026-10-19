FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p qdb-server --bin qdb \
    && strip /src/target/release/qdb

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin appuser \
    && mkdir -p /var/lib/qdb && chown appuser:appuser /var/lib/qdb
COPY --from=build /src/target/release/qdb /usr/local/bin/qdb
USER appuser
WORKDIR /var/lib/qdb
ENV QDB_HTTP_ADDR=0.0.0.0 \
    QDB_HTTP_PORT=9090
EXPOSE 9090
VOLUME /var/lib/qdb
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=12 \
    CMD curl -fsS http://127.0.0.1:9090/v1/snapshots || exit 1
CMD ["qdb", "/var/lib/qdb"]
