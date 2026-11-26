-- The same table through quoted, differently cased identifiers.
{{ config(
    materialized='table',
    post_hook=[
      "delete from \"lake\".\"conflict_fixture\".\"Shared_Log\" where source = 'b'",
      "insert into \"lake\".\"conflict_fixture\".\"Shared_Log\" values ('b', 2)"
    ]
) }}
select n from {{ ref('shared_log') }} where source = 'seed'
