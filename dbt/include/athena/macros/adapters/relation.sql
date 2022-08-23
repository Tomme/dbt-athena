{% macro athena__drop_relation(relation) -%}
  {% if config.get('incremental_strategy') != 'append' %}
    {%- do adapter.clean_up_table(relation.schema, relation.table) -%}
  {% endif %}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}


{% macro athena__unique_suffix() %}
  {%- set query -%}
  SELECT 
    '__tmp_' || 
    cast(cast(to_unixtime(now()) as int) as varchar) || '_' || 
    cast(cast(random() * 1000000 as int) as varchar)
  {%- endset -%}

  {{ return(run_query(query)[0][0]) }}
{% endmacro %}
