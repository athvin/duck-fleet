"""Generates the Phase 1 acceptance project: 110 deterministic models.

Nothing random and nothing fetched — the same tree every run, so a failure
diffs against a known shape. The mix mirrors a small team's project: wide
staging, a joined core, incremental models, and wide fan-ins, with schema tests
so `dbt build` pushes test nodes through the platform too. Every model runs on
the shared persistent tier (no cpu/memory/disk named): sized models derive an
isolated pod, which needs Kubernetes, so the scale sweep stays persistent.
Generated at run time rather than committed: the generator is the artifact
worth reviewing, and 110 near-identical SQL files are not.
"""
import sys
from pathlib import Path

STAGING, CORE, INCREMENTAL, WIDE = 20, 50, 30, 10


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def main():
    root = Path(sys.argv[1])
    write(root / "dbt_project.yml",
          "name: duckfleet_scale\n"
          "version: '0.1.0'\n"
          "config-version: 2\n"
          "profile: duckfleet\n"
          "model-paths: [models]\n"
          "query-comment: null\n"
          "models:\n"
          "  duckfleet_scale:\n"
          "    +meta:\n"
          "      platform:\n"
          "        tags:\n"
          "          team: scale\n")
    for i in range(STAGING):
        write(root / f"models/staging/stg_{i:02}.sql",
              "{{ config(materialized='view') }}\n"
              f"select range as id, range % {7 + i} as bucket, "
              f"(range * {i + 3}) % 1000 as value\n"
              "from range(1500)\n")
    for i in range(CORE):
        first, second = i % STAGING, (i * 7 + 3) % STAGING
        write(root / f"models/core/core_{i:02}.sql",
              "{{ config(materialized='table') }}\n"
              "select a.bucket, count(*) as rows_in_bucket, "
              "sum(a.value + b.value) as total\n"
              f"from {{{{ ref('stg_{first:02}') }}}} a "
              f"join {{{{ ref('stg_{second:02}') }}}} b using (id)\n"
              "group by a.bucket\n")
    for i in range(INCREMENTAL):
        source = (i * 3 + 1) % STAGING
        write(root / f"models/incremental/inc_{i:02}.sql",
              "{{ config(materialized='incremental', unique_key='id', "
              "incremental_strategy='delete+insert') }}\n"
              f"select id, bucket, value from {{{{ ref('stg_{source:02}') }}}}\n"
              "{% if is_incremental() %}\n"
              "where id > (select coalesce(max(id), -1) - 100 from {{ this }})\n"
              "{% endif %}\n")
    for i in range(WIDE):
        parts = " union all ".join(
            f"select * from {{{{ ref('core_{(i * 5 + j) % CORE:02}') }}}}"
            for j in range(3))
        write(root / f"models/wide/wide_{i}.sql",
              "{{ config(materialized='table', meta={'platform': {"
              "'tags': {'team': 'scale'}}}) }}\n"
              + parts + "\n")
    schema = "version: 2\nmodels:\n" + "".join(
        f"  - name: stg_{i:02}\n"
        "    columns:\n"
        "      - name: id\n"
        "        data_tests: [not_null, unique]\n"
        for i in range(STAGING))
    write(root / "models/staging/schema.yml", schema)
    print(f"generated {STAGING + CORE + INCREMENTAL + WIDE} models at {root}")


if __name__ == "__main__":
    main()
