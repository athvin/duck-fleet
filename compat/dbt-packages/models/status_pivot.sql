-- dbt_utils: get_column_values (introspective query) + pivot
select
    customer_id,
    {{ dbt_utils.pivot('status', dbt_utils.get_column_values(ref('orders'), 'status')) }}
from {{ ref('orders') }}
group by customer_id
