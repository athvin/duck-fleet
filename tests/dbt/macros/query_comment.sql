{% macro duckfleet_query_comment(node) %}
  {{ return(tojson({
    'app': 'dbt',
    'invocation_id': invocation_id,
    'node_id': node.unique_id if node else none,
    'platform': node.config.meta.get('platform', {}) if node else {}
  })) }}
{% endmacro %}
