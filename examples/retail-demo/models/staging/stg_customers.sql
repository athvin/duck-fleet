{{ config(materialized='view') }}
-- A thin cleanup over the seed; runs on the default small class.
select customer_id, name, region, tier
from {{ ref('customers') }}
