# DuckFleet's additive streaming patch

Base: crates.io `duckdb` 1.10504.0. The original license and source are retained.
Native `libduckdb-sys` / DuckDB remain pinned at 1.10504.0 / 1.5.4.
The package records upstream commit `452fc0b6650edd8c8aac5c83a98d320bc31cb5bc`.
Its repository-root LICENSE and rustfmt.toml are included from that exact commit
because the published crate omits them.

Additions only:

- `Connection::prepare_single` rejects scripts before normal prepare can execute
  their preceding statements.
- `Statement::statement_type` exposes the public C category, including INVALID
  for categories such as MERGE that 1.5.4 does not map.
- `Statement::start_arrow_stream` returns a fallible result owner with separate
  engine completion, executed-result schema, and error-checked EOF.

The stream uses only public C APIs and Arrow C Data Interface layouts. It does
not cast private Rust handles, run SQL to discover schema, or execute twice.
Existing interactive APIs are unchanged. Native engine/operator memory and
oversized individual values still require the worker's resource limits.

Remove this patch only when an upstream pinned version provides equivalent
single-statement and fallible completion semantics, after the DuckFleet
`engine_stream` regression suite passes against it. An engine upgrade must also
review DuckLake and installed extension compatibility.
