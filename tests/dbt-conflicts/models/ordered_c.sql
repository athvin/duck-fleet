-- Also rewrites shared_log, but only after both branches: no overlap.
{{ config(materialized='table', post_hook="delete from conflict_fixture.shared_log where source = 'c'") }}
select a.n from {{ ref('branch_a') }} a cross join {{ ref('branch_b') }} b
