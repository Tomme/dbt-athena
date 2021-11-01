-- create new table with __data_creation_ts__ column
{% macro create_table(identifier, sql) -%}
  {{ config.set('partitioned_by',config.require('partitioned_by').append('__data_creation_ts__')) }}
  {%- set sql = "SELECT *,CAST(CAST( TO_UNIXTIME(NOW()) AS double)*1000 AS BIGINT) AS __data_creation_ts__ FROM ("~sql~")" -%}
  {{ create_table_as(False,identifier,sql) }}
{% endmacro %}

-- insert into table with new __data_creation_ts__
{% macro insert_into_table(identifier, sql) -%}
  INSERT INTO {{identifier}}
  SELECT *, CAST(CAST( TO_UNIXTIME(NOW()) AS double)*1000 AS BIGINT) AS __data_creation_ts__
  FROM ( {{ sql }} );
{% endmacro %}

-- remove old partions which do not have the newest __data_creation_ts__
{%- macro cleanup_partitions(identifier) -%}
  {% set partition_columns_string = config.require('partitioned_by')|join(',') %}
  {% set sql = "
  SELECT DISTINCT(__data_creation_ts__) FROM ( SELECT __data_creation_ts__, RANK() OVER (PARTITION BY "~partition_columns_string~" ORDER BY __data_creation_ts__ DESC) AS rank FROM "~identifier~" GROUP BY "~partition_columns_string~",__data_creation_ts__ ) WHERE rank>1;" -%}
  {%- set data_creation_timestamps = dbt_utils.get_query_results_as_dict(sql) -%}
  {%- if data_creation_timestamps['__data_creation_ts__']|length > 0 -%}
    ALTER TABLE {{ identifier }} DROP
    {%- for row in data_creation_timestamps['__data_creation_ts__'] -%}
     {% if loop.index > 1 %},{% endif %} PARTITION ( `__data_creation_ts__` = {{ row }} )
    {%- endfor -%};
  {%- endif -%}
{%- endmacro -%}


{% materialization incremental_partitions, adapter='athena' -%}
  {%- set identifier = model['alias'] -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}
  {%- call statement('main') -%}
    {%- if adapter.get_columns_in_relation(target_relation)|length == 0 -%}
      {{ create_table(identifier, sql) }}
    {%- else -%}
      {{ insert_into_table(identifier, sql) }}
    {%- endif -%}
  {%- endcall -%}
  {{ set_table_classification(target_relation, 'parquet') }}
  {{ run_hooks(post_hooks) }}
  {% do persist_docs(target_relation, model) %}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
