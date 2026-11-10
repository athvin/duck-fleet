{% macro duckfleet__load_csv_rows(model, agate_table) %}
  {# CSV paths belong to the dbt client. Send bound values, never server COPY paths. #}
  {% set column_count = agate_table.column_names | length %}
  {% if column_count > 10000 or column_count == 0 %}
    {{ exceptions.raise_compiler_error('DuckFleet seeds require 1–10000 columns') }}
  {% endif %}
  {% set batch_size = [1000, 10000 // column_count] | min %}
  {% set columns = get_seed_column_quoted_csv(model, agate_table.column_names) %}
  {% set statements = [] %}
  {% for chunk in agate_table.rows | batch(batch_size) %}
    {% set bindings = [] %}
    {% for row in chunk %}{% do bindings.extend(row) %}{% endfor %}
    {% set sql %}
      insert into {{ this.render() }} ({{ columns }}) values
      {% for row in chunk %}
        ({% for value in row %}?{% if not loop.last %},{% endif %}{% endfor %})
        {% if not loop.last %},{% endif %}
      {% endfor %}
    {% endset %}
    {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}
    {% if loop.first %}{% do statements.append(sql) %}{% endif %}
  {% endfor %}
  {{ return(statements[0] if statements else '-- empty seed') }}
{% endmacro %}
