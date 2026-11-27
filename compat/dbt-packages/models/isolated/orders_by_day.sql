-- Runs on an isolated pod (meta.platform in dbt_project.yml); dbt_utils + dbt_expectations below.
select
    cast(ordered_at as date) as day,
    count(*) as orders,
    sum(amount) as revenue
from {{ ref('orders_enriched') }}
group by 1
