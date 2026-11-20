{{ config(materialized='table') }}
-- Per-customer lifetime value. Default small class — a cheap grouped aggregate that
-- does not need the medium tier.
select
    customer_id,
    any_value(region) as region,
    any_value(tier) as tier,
    sum(amount)::decimal(18,2) as lifetime_value,
    count(*) as orders
from {{ ref('fct_orders') }}
group by customer_id
