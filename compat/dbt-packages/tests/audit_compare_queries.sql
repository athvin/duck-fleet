-- audit_helper: compare_queries. orders_enriched must keep every order unchanged;
-- any row not present on both sides fails the test.
{% set a %} select order_id, customer_id, amount from {{ ref('orders') }} {% endset %}
{% set b %} select order_id, customer_id, amount from {{ ref('orders_enriched') }} {% endset %}
with audit as (
    {{ audit_helper.compare_queries(a_query=a, b_query=b, primary_key='order_id', summarize=false) }}
)
select * from audit where not (in_a and in_b)
