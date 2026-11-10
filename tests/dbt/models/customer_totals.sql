{{ config(materialized='table') }}
select customer_id, sum(amount)::decimal(18,2) as lifetime_value, count(*) as order_count,
    current_setting('threads')::integer as worker_threads,
    current_setting('memory_limit') as worker_memory
from {{ ref('fct_orders') }}
group by customer_id
