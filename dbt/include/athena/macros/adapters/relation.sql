{% macro athena__drop_relation(relation) -%}
  {%- do adapter.clean_up_table(relation.schema, relation.table) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}
