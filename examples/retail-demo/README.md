# Retail demo dbt project

A standalone example dbt project that runs against a local DuckFleet stack. Separate
from the platform's own test fixture in `tests/dbt/` — that one is asserted on
exactly by the harness, so this project exists to be read, changed, and re-run
without touching it.

## What it builds

Two seeds feed a seven-model DAG:

```
customers ─┐
           ├─ stg_customers ─┐
orders ────┴─ stg_orders ────┴─ fct_orders ─┬─ customer_ltv
                                            ├─ revenue_by_region
                                            └─ daily_revenue
```

`fct_orders` (the join) and `revenue_by_region` (the regional rollup) request the
**medium** class (2 CPU / 2Gi) through `meta.platform`; every other model takes the
project default **small** class (1 CPU / 1Gi). The gateway routes each model to a
worker reporting exactly those resources — `fct_orders` and `revenue_by_region`
record `worker_threads = 2`, the rest `1`.

## Run it

From the repo root, with the stack up (`make up`):

```sh
export DUCKFLEET_TOKEN=duckfleet-local-development-token
DEMO=$PWD/examples/retail-demo
run() { docker compose run --rm -v "$DEMO":/demo dbt dbt "$@" --project-dir /demo --profiles-dir /demo; }

run seed --full-refresh
run run
run test

# Read results back through the gateway:
docker compose run --rm api duckfleet sql 'select * from retail.revenue_by_region'
docker compose run --rm api duckfleet sql 'select * from retail.customer_ltv order by lifetime_value desc'
```

Re-run a model on a different size to watch routing change:

```sh
docker compose run --rm -v "$DEMO":/demo dbt dbt run --select revenue_by_region \
  --project-dir /demo --profiles-dir /demo
```

Everything lands in the platform's query history (per-model placement, tags),
usage ledger (one billable event per execution), and as real Parquet in the object
store under `lake/retail/`.
