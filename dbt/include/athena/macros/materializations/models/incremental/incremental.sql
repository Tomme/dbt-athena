{% materialization incremental, adapter='athena' -%}

  {% set raw_strategy = config.get('incremental_strategy', default='insert_overwrite') %}
  {% set format = config.get('format') %}
  {% set strategy = validate_get_incremental_strategy(raw_strategy, format) %}

  {% set unique_key = config.get('unique_key') %}
  {% set overwrite_msg -%}
    Athena adapter does not support 'unique_key'
  {%- endset %}
  {% if unique_key is not none and strategy != 'merge' %}
    {% do exceptions.raise_compiler_error(overwrite_msg) %}
  {% endif %}

  {% set partitioned_by = config.get('partitioned_by', default=none) %}
  {% set external_location = config.get('external_location', default=none) %}
  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_suffix = athena__unique_suffix() %}
  {% set tmp_relation = make_temp_relation(this, tmp_suffix) %}
  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  
  -- ICEBERG CTAS is not supported by Athena, create table first
  {% if existing_relation is none and format | lower == 'iceberg' %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {%- set dest_columns = adapter.get_columns_in_relation(tmp_relation) -%}
      {%- set column_list = [] -%}
      {% for col in dest_columns %}
        {% do column_list.append(col.name ~ ' ' ~ safe_athena_type(col.data_type)) %}
      {% endfor %}

      {% if external_location is none %}
        {% set external_location = adapter.s3_staging_dir() ~ target_relation.name %}
      {% endif %}

      {% do run_query(create_iceberg_table(target_relation, column_list, partitioned_by, external_location)) %}
      {% set existing_relation = load_relation(this) %}
  {% endif %}

  {% if existing_relation is none and format | lower != 'iceberg' %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or should_full_refresh() %}
      {% do adapter.drop_relation(existing_relation) %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif partitioned_by is not none and strategy == 'insert_overwrite' %}
      {% set tmp_relation = make_temp_relation(target_relation, tmp_suffix) %}
      {% if tmp_relation is not none %}
          {% do adapter.drop_relation(tmp_relation) %}
      {% endif %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      
      {% if existing_relation is not none %}
          {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
          {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
      {% endif %}

      {% do delete_overlapping_partitions(target_relation, tmp_relation, partitioned_by, format) %}
      {% set build_sql = incremental_insert(tmp_relation, target_relation) %}
      {% do to_drop.append(tmp_relation) %}
  {% elif format | lower == 'iceberg' and strategy == 'merge' %}
      {% set tmp_relation = make_temp_relation(target_relation, tmp_suffix) %}
      {% if tmp_relation is not none %}
          {% do adapter.drop_relation(tmp_relation) %}
      {% endif %}

      -- stage new changes
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}

      {% if existing_relation is not none %}
          {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
          {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
      {% endif %}

      {% if partitioned_by is not none %}
        {% set where_clauses = [] %}
        {% for column in partitioned_by %}
          {% set partition_values = [] %}
          {% set values = run_query(get_partition_values(tmp_relation, column)) %}
          {% for value in values %}
            {% do partition_values.append(column ~ "='" ~ value[0] ~ "'") %}
          {% endfor %}
          {% do where_clauses.append('(' ~ partition_values|join(' OR ') ~ ')') %}
        {% endfor %}
        {% set partition_where_condition = where_clauses|join(' AND ') %}
      {% else %}
        {% set partition_where_condition = none %}
      {% endif %}

      {% set new_tmp_insert = merge_insert_existing(target_relation, tmp_relation, unique_key, partition_where_condition) %}

      -- save existing rows NOT being updated in stage to temp table
      {% set new_suffix = athena__unique_suffix() %}
      {% set new_tmp_relation = make_temp_relation(this, new_suffix) %}
      {% do run_query(create_table_as(True, new_tmp_relation, new_tmp_insert)) %}

      -- wipe target table
      {% if partitioned_by is not none %}
        {% do run_query(iceberg_delete_where(target_relation, partition_where_condition)) %}
      {% else %}
        {% do run_query(merge_delete_all(target_relation)) %}
      {% endif %}

      -- merge new changes and existing rows
      {% set build_sql = merge_insert(tmp_relation, new_tmp_relation, target_relation) %}
      {% do to_drop.append(tmp_relation) %}

      -- make_temp_relation isn't setting type correctly?
      {% do to_drop.append(api.Relation.create(schema=new_tmp_relation.schema, identifier=new_tmp_relation.name, type='table')) %}
  {% else %}
      {% set tmp_relation = make_temp_relation(target_relation, tmp_suffix) %}
      {% if tmp_relation is not none %}
          {% do adapter.drop_relation(tmp_relation) %}
      {% endif %}
      {% if strategy == 'insert_overwrite' and partitioned_by is none %}
        {% if format | lower == 'iceberg' %}
          {% do run_query(merge_delete_all(target_relation)) %}
        {% else %}
          {% do adapter.clean_up_table(target_relation.schema, target_relation.name) %}
        {% endif %}
      {% endif %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      
      {% if existing_relation is not none %}
          {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
          {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
      {% endif %}

      {% set build_sql = incremental_insert(tmp_relation, target_relation) %}
      {% do to_drop.append(tmp_relation) %}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  -- set table properties
  {% if not to_drop and format | lower != 'iceberg' %}
    {{ set_table_classification(target_relation, 'parquet') }}
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
