-- Appends never conflict with appends.
{{ config(materialized='table', post_hook="insert into lake.conflict_fixture.events_log values (1)") }}
select n from {{ ref('events_log') }}
