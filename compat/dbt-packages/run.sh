#!/usr/bin/env bash
# Prove popular dbt packages work against a DuckFleet gateway (#216):
# install them, build models and tests that use their macros on both the
# persistent tier and an isolated pod, then run codegen's introspective macros.
#
#   DUCKFLEET_URL=... DUCKFLEET_TOKEN=... compat/dbt-packages/run.sh
#
# Needs the DuckFleet dbt (`brew install athvin/duckfleet/dbt-duckfleet`).
# CI runs it in the harness's dbt image: `make test-dbt-packages`.
set -euo pipefail
cd "$(dirname "$0")"
: "${DUCKFLEET_TOKEN:?set DUCKFLEET_URL and DUCKFLEET_TOKEN for the gateway}"
dbt=(dbt --profiles-dir .)

"${dbt[@]}" deps
# DUCKFLEET_COMPAT_ISOLATED=false where the gateway has no isolated compute.
"${dbt[@]}" build --full-refresh --vars "{isolated: ${DUCKFLEET_COMPAT_ISOLATED:-true}}"

# codegen reads the catalog back through the adapter's introspection.
yaml=$("${dbt[@]}" run-operation codegen.generate_model_yaml --args '{"model_names": ["orders_enriched"]}')
grep -q "name: order_key" <<< "$yaml" || { echo "codegen.generate_model_yaml did not list order_key" >&2; echo "$yaml" >&2; exit 1; }
# table_names is required on DuckDB: without it codegen discovers tables through
# dbt_utils.get_relations_by_pattern, which reads <database>.information_schema,
# and DuckDB has no database-qualified information_schema (a dbt_utils gap on
# DuckDB generally, not DuckFleet-specific). See README.md.
source=$("${dbt[@]}" run-operation codegen.generate_source --args '{"schema_name": "package_compat", "database_name": "lake", "table_names": ["orders", "orders_enriched"], "generate_columns": true}')
grep -q "name: orders_enriched" <<< "$source" || { echo "codegen.generate_source did not find orders_enriched" >&2; echo "$source" >&2; exit 1; }
base=$("${dbt[@]}" run-operation codegen.generate_base_model --args '{"source_name": "package_compat", "table_name": "orders"}')
grep -q "amount" <<< "$base" || { echo "codegen.generate_base_model did not list amount" >&2; echo "$base" >&2; exit 1; }
echo "PASS: dbt_utils, dbt_expectations, audit_helper, and codegen on DuckFleet"
