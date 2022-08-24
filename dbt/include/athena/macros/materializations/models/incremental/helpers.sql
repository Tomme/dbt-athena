{% macro validate_get_incremental_strategy(raw_strategy, format) %}
  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ raw_strategy }}
    Expected one of: 'append', 'insert_overwrite', 'merge'
  {%- endset %}

  {%- set invalid_merge_msg -%}
  Invalid incremental strategy provided: {{ raw_strategy }}
  Merge is only supported with Iceberg tables
  {%- endset %}

  {% if raw_strategy not in ['append', 'insert_overwrite', 'merge'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% if raw_strategy == 'merge' and format | lower != 'iceberg' %}
    {% do exceptions.raise_compiler_error(invalid_merge_msg) %}
  {% endif %}

  {% do return(raw_strategy) %}
{% endmacro %}

{% macro incremental_insert(tmp_relation, target_relation, statement_name="main") %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation }}
    );
{%- endmacro %}

{% macro merge_insert(tmp_relation_1, tmp_relation_2, target_relation, statement_name="main") %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation_1 }}

       union all

       select {{ dest_cols_csv }}
       from {{ tmp_relation_2 }}
    );
{%- endmacro %}

{% macro delete_overlapping_partitions(target_relation, tmp_relation, partitioned_by, table_format) %}
  {%- set partitioned_keys = partitioned_by | tojson | replace('\"', '') | replace('[', '') | replace(']', '') -%}
  {% call statement('get_partitions', fetch_result=True) %}
    select distinct {{partitioned_keys}} from {{ tmp_relation }};
  {% endcall %}
  {%- set table = load_result('get_partitions').table -%}
  {%- set rows = table.rows -%}
  {%- set partitions = [] -%}
  {%- for row in rows -%}
    {%- set single_partition = [] -%}
    {%- for col in row -%}
      {%- set column_type = adapter.convert_type(table, loop.index0) -%}
      {%- if column_type == 'integer' -%}
        {%- set value = col|string -%}
      {%- elif column_type == 'string' -%}
        {%- set value = "'" + col + "'" -%}
      {%- elif column_type == 'date' -%}
        {%- set value = "'" + col|string + "'" -%}
      {%- else -%}
        {%- do exceptions.raise_compiler_error('Need to add support for column type ' + column_type) -%}
      {%- endif -%}
      {%- do single_partition.append(partitioned_by[loop.index0] + '=' + value) -%}
    {%- endfor -%}
    {%- set single_partition_expression = single_partition | join(' and ') -%}
    {%- do partitions.append('(' + single_partition_expression + ')') -%}
  {%- endfor -%}
  {%- for i in range(partitions | length) %}
    {% if table_format | lower == 'iceberg' %}
      {% do run_query(iceberg_delete_where(target_relation, partitions[i])) %}
    {% else %}
      {%- do adapter.clean_up_partitions(target_relation.schema, target_relation.table, partitions[i]) -%}
    {% endif %}
  {%- endfor -%}
{%- endmacro %}

{% macro iceberg_delete_where(target_relation, where_condition) %}
  delete from {{ target_relation.schema }}.{{ target_relation.table }}
  where {{ where_condition }}
{% endmacro %}

{% macro merge_delete_existing(target_relation, tmp_relation, unique_key) %}
  -- This is the way we should be doing this merge, but this doesn't work:
  -- https://repost.aws/questions/QUMxjp0hqJTT-IfaXAOBqLag/athena-iceberg-delete-failing
  delete from {{ target_relation.schema }}.{{ target_relation.table }}
  where {{ unique_key }} in (select {{ unique_key }} from {{ tmp_relation.schema }}.{{ tmp_relation.table }})
{% endmacro %}

{% macro merge_insert_existing(target_relation, tmp_relation, unique_key) %}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  with existing as (
    select
    {{ unique_key }} as dbt__unique_key,
    {{ dest_columns | map(attribute='name') | join(', ') }}
    from {{ target_relation.schema }}.{{ target_relation.table }}
  ),
  new as (
    select
    {{ unique_key }} as dbt__unique_key,
    {{ dest_columns | map(attribute='name') | join(', ') }}
    from {{ tmp_relation.schema }}.{{ tmp_relation.table }}
  )
  select
  {%- set col_updates = [] -%}
  {% for col in dest_columns -%}
    {%- do col_updates.append('existing.' ~ col.name) -%}
  {%- endfor %}
  {{ col_updates | join(', ') }}
  from existing left join new on existing.dbt__unique_key=new.dbt__unique_key
  where new.dbt__unique_key is null
{% endmacro %}

{% macro merge_delete_all(target_relation) %}
  delete from {{ target_relation.schema }}.{{ target_relation.table }}
{% endmacro %}
