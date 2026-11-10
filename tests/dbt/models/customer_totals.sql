{{ config(materialized='table') }}
select customer_id, sum(amount)::decimal(18,2) as lifetime_value, count(*) as order_count
from {{ ref('fct_orders') }}
group by customer_id
