{{ config(materialized='view') }}
-- Normalizes the order grain and derives the date used downstream.
select
    order_id,
    customer_id,
    product,
    amount,
    status,
    ordered_at,
    cast(ordered_at as date) as order_date
from {{ ref('orders') }}
