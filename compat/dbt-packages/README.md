# dbt package compatibility

A dbt project that installs popular packages with `dbt deps` and uses their
macros and tests against a DuckFleet gateway, on the persistent tier and on an
isolated pod (`models/isolated/`). DuckFleet dispatches package macros as
`duckfleet__`, then `duckdb__`, then `default__`, so packages' DuckDB
implementations apply as they are.

```sh
brew install athvin/duckfleet/dbt-duckfleet
DUCKFLEET_URL=https://... DUCKFLEET_TOKEN=... compat/dbt-packages/run.sh
```

## Support matrix

Last run: 2026-11-27, `duckfleet-demo` on EKS (er-dev), dbt-duckfleet 0.1.7
(dbt-oss 2.0.11), as a named `analyst` principal, 4 threads.

| Package | Version | Exercised | Result |
| --- | --- | --- | --- |
| dbt-labs/dbt_utils | 1.4.1 | `generate_surrogate_key`, `star`, `safe_divide`, `pivot` + `get_column_values`, `date_spine`, `union_relations`, `deduplicate`; tests `unique_combination_of_columns`, `equal_rowcount`, `fewer_rows_than`, `accepted_range`, `not_empty_string`, `expression_is_true`, `recency` | pass |
| metaplane/dbt_expectations | 0.10.10 | row counts, column set, value ranges, column type, regex, distinct count, increasing, column sum | pass |
| godatadriven/dbt_date | 0.21.0 | installed as a dbt_expectations dependency | pass |
| dbt-labs/audit_helper | 0.14.0 | `compare_queries` | pass |
| dbt-labs/codegen | 0.14.1 | `generate_model_yaml`, `generate_source` (with `table_names`), `generate_base_model` | pass |

## Known gaps

- **`dbt_utils.get_relations_by_pattern`, and anything built on it.** That
  includes `codegen.generate_source` without `table_names` and
  `dbt_utils.union_relations` over a pattern. Its default implementation reads
  `<database>.information_schema.tables`, and DuckDB has no database-qualified
  `information_schema`. This is the same on plain dbt-duckdb. Pass explicit
  relations or `table_names`.
- **Column types of seeds.** DuckDB infers CSV numbers as `DOUBLE`, so set
  `column_types` when a test such as `expect_column_values_to_be_of_type`
  expects a decimal.
