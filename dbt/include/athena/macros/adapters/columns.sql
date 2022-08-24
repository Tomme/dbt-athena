{% macro athena__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}

      select
          column_name,
          data_type,
          null as character_maximum_length,
          null as numeric_precision,
          null as numeric_scale

      from {{ relation.information_schema('columns') }}
      where table_name = LOWER('{{ relation.identifier }}')
        {% if relation.schema %}
            and table_schema = LOWER('{{ relation.schema }}')
        {% endif %}
      order by ordinal_position

  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {% do return(sql_convert_columns_in_relation(table)) %}
{% endmacro %}

{% macro safe_athena_type(data_type) %}
  {% if 'varchar' in data_type or 'varying' in data_type %}
    {%- set safe_type = 'string' -%}
  {% elif data_type == 'integer' %}
    {%- set safe_type = 'int' -%}
  {% elif data_type in ['boolean', 'double', 'date', 'timestamp'] %}
    {%- set safe_type = data_type -%}
  {% else %}
    {%- set unknown_data_type = 'Unknown data type ' ~ data_type -%}
    {% do exceptions.raise_compiler_error(unknown_data_type) %}
  {% endif %}

  {% do return(safe_type) %}
{% endmacro %}
