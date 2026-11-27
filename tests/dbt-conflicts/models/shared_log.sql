-- One table that several independent branches rewrite from hooks.
{{ config(materialized='table') }}
select 'seed' as source, 0 as n
