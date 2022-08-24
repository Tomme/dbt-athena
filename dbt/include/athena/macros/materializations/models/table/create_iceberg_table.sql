{% macro create_iceberg_table(relation, column_list, partitioned_by, external_location) -%}

  create table {{ relation.name }} ({{ column_list | join(', ') }})
  {% if partitioned_by is not none %}
    partitioned by ({{ partitioned_by | join(', ') }})
  {% endif %}
  location '{{ external_location }}'
  tblproperties ( 'table_type' = 'ICEBERG' )

{% endmacro %}
