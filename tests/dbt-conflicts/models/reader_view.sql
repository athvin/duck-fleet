-- Read-only: a view over another model.
{{ config(materialized='view') }}
select * from {{ ref('branch_a') }}
