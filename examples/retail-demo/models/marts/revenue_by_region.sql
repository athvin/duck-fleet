{{ config(
    materialized='table',
    meta={'platform': {'compute': 'persistent', 'cpu': 2, 'memory': '2Gi',
                       'tags': {'team': 'retail', 'layer': 'mart'}}}
) }}
-- Regional revenue rollup, requested on the medium class to show a second model
-- routing to a different size than its siblings. worker_threads records where it ran.
select
    region,
    sum(amount)::decimal(18,2) as revenue,
    count(*) as orders,
    current_setting('threads')::integer as worker_threads
from {{ ref('fct_orders') }}
group by region
order by revenue desc
