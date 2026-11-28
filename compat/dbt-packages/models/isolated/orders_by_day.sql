-- Runs on an isolated pod, so package macros are proven on both tiers. The
-- `isolated` var turns that off where the cluster has no isolated compute
-- (the local Compose harness).
{{ config(meta={'platform': {'cpu': 1, 'memory': '2Gi'}} if var('isolated', true) else {}) }}
select
    cast(ordered_at as date) as day,
    count(*) as orders,
    sum(amount) as revenue
from {{ ref('orders_enriched') }}
group by 1
