-- Only ever appended to by downstream hooks.
{{ config(materialized='table') }}
select 0 as n
