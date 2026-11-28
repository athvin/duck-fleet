-- dbt_utils: union_relations (column introspection on two relations)
{{ dbt_utils.union_relations(relations=[ref('customers'), ref('orders_enriched')]) }}
