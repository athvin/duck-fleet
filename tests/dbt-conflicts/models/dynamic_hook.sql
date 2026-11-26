-- The hook target is built at run time, so static analysis cannot know it.
{{ config(materialized='table', post_hook="{{ 'delete from ' ~ this.schema ~ '.shared_log where false' }}") }}
select n from {{ ref('shared_log') }} where false
