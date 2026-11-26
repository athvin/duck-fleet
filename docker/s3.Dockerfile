# syntax=docker/dockerfile:1
FROM rust:1.99.0-bookworm AS build
ARG TARGETARCH
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip && rm -rf /var/lib/apt/lists/*
WORKDIR /app/server
COPY server/Cargo.toml server/Cargo.lock ./
COPY server/ops ./ops
COPY server/cli ./cli
COPY server/vendor ./vendor
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,id=duckfleet-target-${TARGETARCH},target=/app/server/target cargo build --locked --package duckfleet-ops && cp target/debug/duckfleet-ops /tmp/duckfleet-ops
RUN case "${TARGETARCH}" in \
      amd64) arch=amd64; expected=31e79fe7697c870e60f5b5c9008adbb3ad5af7f9f2ab45a3d68585c5f87238d1 ;; \
      arm64) arch=arm64; expected=a02091205fe4d9fbf0d16a0dac3d46ad7fac7d5266024b5b17e76327a4e88aa8 ;; \
      *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && version=0.1.37 \
    && url="https://github.com/rustfs/cli/releases/download/v${version}/rustfs-cli-linux-${arch}-v${version}.tar.gz" \
    && curl -fsSL "$url" -o /tmp/rc.tar.gz \
    && echo "$expected  /tmp/rc.tar.gz" | sha256sum --check \
    && tar -xzf /tmp/rc.tar.gz -C /usr/local/bin rc

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=build /tmp/duckfleet-ops /usr/local/bin/duckfleet-ops
COPY --from=build /usr/local/bin/rc /usr/local/bin/rc
ENTRYPOINT ["duckfleet-ops", "object-store"]
