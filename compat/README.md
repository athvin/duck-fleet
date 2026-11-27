# Third-party client compatibility

DuckFleet's own tooling is Rust-only. The `rust_only_tooling` check in `server/ops/src/aws_ci.rs` keeps Python out of `scripts/`, `tests/`, `docker/`, and `.github/`. This directory is the one deliberate exception. It holds programs that exercise **third-party clients** DuckFleet promises to work with, in the languages those clients are written in:

| Program | Client | Run by |
|---|---|---|
| `pgwire_clients.py` | psycopg 3.3.9 against the pgwire front | `make test-pgwire-clients` |

These programs never run on the host. Scripts start them in a pinned `ghcr.io/astral-sh/uv` container on the Compose network, with pinned client versions, so neither CI nor a developer machine needs a local Python.
