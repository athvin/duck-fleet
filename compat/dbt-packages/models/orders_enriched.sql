-- dbt_utils: generate_surrogate_key, star, safe_divide
select
    {{ dbt_utils.generate_surrogate_key(['o.order_id', 'o.customer_id']) }} as order_key,
    {{ dbt_utils.star(from=ref('orders'), relation_alias='o') }},
    c.name,
    c.region,
    {{ dbt_utils.safe_divide('o.amount', '100') }} as amount_hundreds
from {{ ref('orders') }} o
join {{ ref('customers') }} c using (customer_id)
