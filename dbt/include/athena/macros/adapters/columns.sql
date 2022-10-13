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

{% macro safe_athena_type(data_type, is_seed=False) %}
  {% if 'varchar' in data_type or 'varying' in data_type or data_type == 'text' or data_type == 'string' %}
    {%- set safe_type = 'string' -%}
  {% elif '[]' in data_type %}
    {%- set inner_type = safe_athena_type(data_type | replace('[]', '')) %}
    {% if is_seed %}
      -- seed tables load everything with an insert pattern, complex types are loaded as strings
      {%- set safe_type = inner_type -%}
    {% else %}
      {%- set safe_type = 'array<' ~ inner_type ~ '>' -%}
    {% endif %}
  {% elif data_type == 'integer' or data_type == 'int' %}
    {%- set safe_type = 'int' -%}
  {% elif data_type == 'date' %}
    {% if is_seed %}
      -- Parquet doesn't support dates?
      {%- set safe_type = 'string' -%}
    {% else %}
      {%- set safe_type = data_type -%}
    {% endif %}
  {% elif 'decimal' in data_type %}
    {%- set safe_type = 'double' -%}
  {% elif data_type in ['boolean', 'double', 'timestamp', 'bigint'] %}
    {%- set safe_type = data_type -%}
  {% else %}
    {%- set unknown_data_type = 'Unknown data type ' ~ data_type -%}
    {% do exceptions.raise_compiler_error(unknown_data_type) %}
  {% endif %}

  {% do return(safe_type) %}
{% endmacro %}

{% macro alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

  {% if add_columns is none %}
    {% do return('Nothing to do') %}
  {% endif %}

  {% if remove_columns is not none %}
    {%- set error_msg = 'Removing columns not supported in Athena' -%}
    {% do exceptions.raise_compiler_error(error_msg) %}
  {% endif %}

  {% set sql -%}
    alter {{ relation.type }} {{ relation }}
      {% if add_columns != [] %}
        add columns (
          {% for column in add_columns %}
            {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
          {% endfor %}
        )
      {% endif %}

  {%- endset -%}

  {% do run_query(sql) %}
{% endmacro %}
