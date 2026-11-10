{{ config(
    materialized='table',
    enabled=var('exercise_unsupported_size', false),
    meta={'platform': {'cpu': 32, 'memory': '128Gi'}},
    pre_hook='insert into checks.replay values (1000)'
) }}
select 1 as should_never_run
