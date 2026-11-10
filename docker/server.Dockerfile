# syntax=docker/dockerfile:1
FROM rust:1.99.0-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip && rm -rf /var/lib/apt/lists/*
COPY scripts/install-duckdb.sh /tmp/install-duckdb.sh
RUN sh /tmp/install-duckdb.sh
COPY scripts/install-extensions.sh /tmp/install-extensions.sh
RUN sh /tmp/install-extensions.sh
ENV DUCKDB_LIB_DIR=/usr/local/lib LD_LIBRARY_PATH=/usr/local/lib
WORKDIR /app/server
COPY server/Cargo.toml server/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/app/server/target cargo build --locked && cp target/debug/duckfleet-server /tmp/duckfleet-server
COPY server/src ./src
RUN touch src/main.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/app/server/target cargo build --locked && cp target/debug/duckfleet-server /tmp/duckfleet-server

FROM build AS check
RUN rustup component add rustfmt clippy
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/app/server/target cargo fmt --check && cargo clippy --locked --all-targets -- -D warnings && cargo test --locked

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 && rm -rf /var/lib/apt/lists/* && useradd -m -u 10001 duckfleet
COPY --from=build /usr/local/lib/libduckdb.so /usr/local/lib/libduckdb.so
RUN ldconfig
COPY --from=build /opt/duckdb /opt/duckdb
COPY --from=build /tmp/duckfleet-server /usr/local/bin/duckfleet-server
USER duckfleet
EXPOSE 8080
CMD ["duckfleet-server"]
