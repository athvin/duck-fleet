# syntax=docker/dockerfile:1
# The dbt test image: the forked dbt-core binary (which embeds the DuckFleet
# adapter) PLUS the duckfleet-server binary run as DUCKFLEET_ROLE=harness (the
# Rust integration harness, which shells out to `dbt`) PLUS the test fixtures.
# No Python. The Rust build stage mirrors docker/server.Dockerfile and shares its
# cargo/target cache id, so building this image reuses that compile.
FROM rust:1.99.0-bookworm AS build
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip && rm -rf /var/lib/apt/lists/*
COPY scripts/install-duckdb.sh /tmp/install-duckdb.sh
RUN sh /tmp/install-duckdb.sh
COPY scripts/install-extensions.sh /tmp/install-extensions.sh
RUN sh /tmp/install-extensions.sh
ENV DUCKDB_LIB_DIR=/usr/local/lib LD_LIBRARY_PATH=/usr/local/lib
WORKDIR /app/server
COPY server/Cargo.toml server/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,id=duckfleet-target-${TARGETARCH},target=/app/server/target cargo build --locked && cp target/debug/duckfleet-server /tmp/duckfleet-server
COPY server/src ./src
RUN touch src/main.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,id=duckfleet-target-${TARGETARCH},target=/app/server/target cargo build --locked && cp target/debug/duckfleet-server /tmp/duckfleet-server

# ubuntu 24.04 for glibc >= 2.39, which the forked dbt binary (built on that
# toolchain) requires. duckfleet-server (glibc 2.36, built on bookworm) runs here too.
FROM ubuntu:24.04 AS runtime
ARG DBT_RELEASE=duckfleet-v0.1.2
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 && rm -rf /var/lib/apt/lists/*
# The forked dbt-core binary (embeds AdapterType::DuckFleet).
RUN curl -fsSL -o /tmp/dbt.tgz "https://github.com/athvin/dbt/releases/download/${DBT_RELEASE}/dbt-linux-x86_64.tar.gz" \
    && tar -C /usr/local/bin -xzf /tmp/dbt.tgz dbt \
    && rm /tmp/dbt.tgz && chmod +x /usr/local/bin/dbt
# duckfleet-server is dynamically linked against libduckdb, so carry the lib even
# though the harness role never opens a local database.
COPY --from=build /usr/local/lib/libduckdb.so /usr/local/lib/libduckdb.so
RUN ldconfig
COPY --from=build /opt/duckdb /opt/duckdb
COPY --from=build /tmp/duckfleet-server /usr/local/bin/duckfleet-server
COPY tests /app/tests
ENV DBT_PROFILES_DIR=/app/tests/dbt DBT_SEND_ANONYMOUS_USAGE_STATS=false NO_COLOR=1 DUCKFLEET_ROLE=harness
WORKDIR /app/tests/dbt
# Default runs the harness main lifecycle; override with stage flags
# (`... dbt --concurrency`) or run dbt directly (`... dbt dbt build`).
CMD ["duckfleet-server"]
