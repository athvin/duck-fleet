# syntax=docker/dockerfile:1
# The dbt-free path, in an image where `dbt` genuinely is not installed.
#
# Separate from the dbt image on purpose: running the API workflow in an image
# that happens to have dbt present would prove the workflow works, not that it
# works *without dbt*. This image carries only duckfleet-server; the harness's
# `--api-workflow` stage reaches the gateway through the Rust client alone.
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
COPY server/ops ./ops
COPY server/cli ./cli
COPY server/vendor ./vendor
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,id=duckfleet-target-${TARGETARCH},target=/app/server/target cargo build --locked && cp target/debug/duckfleet-server /tmp/duckfleet-server
COPY server/src ./src
RUN touch src/main.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,id=duckfleet-target-${TARGETARCH},target=/app/server/target cargo build --locked && cp target/debug/duckfleet-server /tmp/duckfleet-server

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/lib/libduckdb.so /usr/local/lib/libduckdb.so
RUN ldconfig
COPY --from=build /opt/duckdb /opt/duckdb
COPY --from=build /tmp/duckfleet-server /usr/local/bin/duckfleet-server
COPY tests /app/tests
ENV NO_COLOR=1 DUCKFLEET_ROLE=harness
WORKDIR /app
CMD ["duckfleet-server", "--api-workflow"]
