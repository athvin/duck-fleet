{{ config(
    materialized='table',
    enabled=var('exercise_isolated', false),
    meta={'platform': {'cpu': 1, 'memory': '1Gi', 'tags': {'team': 'harness'}}},
    pre_hook='create or replace temp table isolated_hook as select 7 as value',
    post_hook="select case when (select value from isolated_hook) = 7 then 1 else error('hook lost its isolated session') end"
) }}
select order_id, amount,
    (select value from isolated_hook) as hook_value,
    current_setting('threads')::integer as worker_threads
from {{ ref('stg_orders') }}
