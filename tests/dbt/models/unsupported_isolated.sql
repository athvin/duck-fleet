{{ config(
    materialized='table',
    enabled=var('exercise_unsupported_isolated', false),
    meta={'platform': {'compute': 'isolated', 'cpu': 1, 'memory': '1Gi'}},
    pre_hook='insert into checks.replay values (2000)'
) }}
select 1 as should_never_run
