{{ config(materialized='view') }}
select order_id, customer_id, amount, updated_at
from {{ ref('orders') }}
