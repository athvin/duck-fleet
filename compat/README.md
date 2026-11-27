# Third-party client compatibility

DuckFleet's own tooling is Rust-only. The `rust_only_tooling` check in `server/ops/src/aws_ci.rs` keeps Python out of `scripts/`, `tests/`, `docker/`, and `.github/`. This directory is the one deliberate exception. It holds programs that exercise **third-party clients** DuckFleet promises to work with, in the languages those clients are written in:

| Program | Client | Run by |
|---|---|---|
| `pgwire_clients.py` | psycopg 3.3.9 against the pgwire front | `make test-pgwire-clients` |
| `adbc_load_smoke.py` | adbc-driver-manager 1.13.0 loading a released driver by manifest name | `.github/workflows/adbc-release.yml` (via `astral-sh/setup-uv`) |
| `adbc_suite.py` | adbc-driver-manager 1.13.0, pyarrow 26.0.1, pandas 3.1.1 against the DuckFleet ADBC driver | `make test-adbc` |
| `dbt-packages/` | dbt_utils, dbt_expectations, audit_helper, codegen through dbt-duckfleet (a dbt project, not Python) | `compat/dbt-packages/run.sh` against any gateway |

These programs never run on the host. Scripts start them in a pinned `ghcr.io/astral-sh/uv` container on the Compose network, with pinned client versions, so neither CI nor a developer machine needs a local Python.
