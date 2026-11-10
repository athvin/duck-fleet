select * from {{ ref('fct_orders') }} where hook_value <> 42 or hook_value is null
