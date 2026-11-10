{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    pre_hook='create or replace temp table hook_marker as select 42 as value',
    post_hook="select case when (select value from hook_marker) = 42 then 1 else error('hook lost its session') end"
) }}
select *, (select value from hook_marker) as hook_value
from {{ ref('stg_orders') }}
{% if is_incremental() %}
where updated_at > (select coalesce(max(updated_at), timestamp '1900-01-01') from {{ this }})
{% endif %}
