FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# 服务进程 + 只读 SQL 客户端（可与正在跑的 qdb 同开数据目录）
RUN cargo build --release -p qdb-server --bin qdb --bin qdb-client \
    && strip /src/target/release/qdb /src/target/release/qdb-client

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin appuser \
    && mkdir -p /var/lib/qdb && chown appuser:appuser /var/lib/qdb
COPY --from=build /src/target/release/qdb /usr/local/bin/qdb
COPY --from=build /src/target/release/qdb-client /usr/local/bin/qdb-client
USER appuser
WORKDIR /var/lib/qdb
ENV QDB_HTTP_ADDR=0.0.0.0 \
    QDB_HTTP_PORT=9090
EXPOSE 9090
VOLUME /var/lib/qdb
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=12 \
    CMD curl -fsS http://127.0.0.1:9090/v1/snapshots || exit 1
CMD ["qdb", "/var/lib/qdb"]
