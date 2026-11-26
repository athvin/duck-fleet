{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    meta={'platform': {'tags': {'team': 'retail', 'layer': 'mart'}}}
) }}
-- The join of completed orders to their customer. The selected-setting columns
-- record the persistent worker it ran on. Incremental, so a reseed only reprocesses
-- newer order dates.
select
    o.order_id,
    o.customer_id,
    c.region,
    c.tier,
    o.product,
    o.amount,
    o.order_date,
    current_setting('threads')::integer as worker_threads,
    current_setting('memory_limit') as worker_memory
from {{ ref('stg_orders') }} o
join {{ ref('stg_customers') }} c using (customer_id)
where o.status = 'completed'
{% if is_incremental() %}
  and o.order_date > (select coalesce(max(order_date), date '1900-01-01') from {{ this }})
{% endif %}
