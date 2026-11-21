# DuckFleet's additive streaming patch

Base: crates.io `duckdb` 1.20001.0. The original license and source are retained.
Native `libduckdb-sys` / DuckDB are pinned at 1.20001.0 / 2.0.1.
The package records upstream commit `e3b2d244c90e9074db5e4af635bad58ab469b672`.
Its repository-root LICENSE and rustfmt.toml are retained from that commit.

Additions only:

- `Connection::prepare_single` rejects scripts before normal prepare can execute
  their preceding statements.
- `Statement::statement_type` exposes the public C category. DuckDB 2.0.1 maps
  MERGE to MERGE_INTO; DuckFleet's separate allowlist continues to reject it.
- `Statement::start_arrow_stream` returns a fallible result owner with separate
  engine completion, executed-result schema, and error-checked EOF.

The stream uses the stable C API v1 and public Arrow C Data Interface layouts. It
does not cast private Rust handles, run SQL to discover schema, or execute twice.
Existing interactive APIs are unchanged. Native engine/operator memory and
oversized individual values still require the worker's resource limits.

Remove this patch only when an upstream pinned version provides equivalent
single-statement and fallible completion semantics, after the DuckFleet
`engine_stream` regression suite passes against it. An engine upgrade must also
review DuckLake and installed extension compatibility.
