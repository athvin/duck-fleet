{{ config(materialized='table') }}
-- Revenue per order date, on the default small class.
select
    order_date,
    sum(amount)::decimal(18,2) as revenue,
    count(*) as orders
from {{ ref('fct_orders') }}
group by order_date
order by order_date
