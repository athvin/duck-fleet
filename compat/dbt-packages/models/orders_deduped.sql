-- dbt_utils: deduplicate
{{ dbt_utils.deduplicate(relation=ref('orders'), partition_by='customer_id', order_by='ordered_at desc') }}
