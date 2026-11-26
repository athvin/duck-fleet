{{ config(
    materialized='table',
    meta={'platform': {'tags': {'team': 'retail', 'layer': 'mart'}}}
) }}
-- Regional revenue rollup. worker_threads records the persistent worker it ran on.
select
    region,
    sum(amount)::decimal(18,2) as revenue,
    count(*) as orders,
    current_setting('threads')::integer as worker_threads
from {{ ref('fct_orders') }}
group by region
order by revenue desc
