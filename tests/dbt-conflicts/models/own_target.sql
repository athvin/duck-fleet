-- Writes only its own relation; nothing else touches it.
{{ config(materialized='table', post_hook="delete from lake.conflict_fixture.own_target where n < 0") }}
select 1 as n
