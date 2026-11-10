{{ config(
    materialized='table',
    enabled=var('exercise_failure', false),
    pre_hook='insert into checks.replay values (999)'
) }}
select definitely_missing_column from {{ ref('stg_orders') }}
