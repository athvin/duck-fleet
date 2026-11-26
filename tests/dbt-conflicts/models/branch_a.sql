-- Rewrites shared_log rows with a fully qualified delete+insert.
{{ config(
    materialized='table',
    post_hook=[
      "delete from lake.conflict_fixture.shared_log where source = 'a'",
      "insert into lake.conflict_fixture.shared_log values ('a', 1)"
    ]
) }}
select n from {{ ref('shared_log') }} where source = 'seed'
