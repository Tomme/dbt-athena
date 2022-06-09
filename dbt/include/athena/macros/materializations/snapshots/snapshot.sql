{% macro athena__snapshot_hash_arguments(args) -%}
    to_hex(md5(to_utf8({%- for arg in args -%}
        coalesce(cast({{ arg }} as varchar ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})))
{%- endmacro %}

{% macro athena__snapshot_string_as_time(timestamp) -%}
    {%- set result = "date_parse('" ~ timestamp ~ "', '%Y-%m-%d %H:%i:%S.%f')" -%}
    {{ return(result) }}
{%- endmacro %}


{% macro athena__snapshot_merge_sql(target, source, insert_cols) -%}
    INSERT INTO {{ target }}
    SELECT *
      , {{ current_timestamp() }} as dbt_snapshot_at
    FROM {{ source }};
{% endmacro %}

{% macro build_snapshot_table(strategy, source_sql) %}
    SELECT *
      , {{ strategy.unique_key }} AS dbt_unique_key
      , {{ strategy.updated_at }} AS dbt_valid_from
      , {{ strategy.scd_id }} AS dbt_scd_id
      , 'insert' AS dbt_change_type
      , {{ current_timestamp() }} AS dbt_snapshot_at
    FROM ({{ source_sql }}) source;
{% endmacro %}

{% macro snapshot_staging_table(strategy, source_sql, target_relation) -%}
    WITH snapshot_query AS (
        {{ source_sql }}
    )
    , snapshotted_data_base AS (
        SELECT *
          , ROW_NUMBER() OVER (
              PARTITION BY dbt_unique_key
              ORDER BY dbt_valid_from DESC
            ) AS dbt_snapshot_rn
        FROM {{ target_relation }}
    )
    , snapshotted_data AS (
        SELECT *
        FROM snapshotted_data_base
        WHERE dbt_snapshot_rn = 1
          AND dbt_change_type != 'delete'
    )
    , source_data AS (
        SELECT *
          , {{ strategy.unique_key }} AS dbt_unique_key
          , {{ strategy.updated_at }} AS dbt_valid_from
          , {{ strategy.scd_id }} AS dbt_scd_id
        FROM snapshot_query
    )
    , upserts AS (
        SELECT source_data.*
          , CASE
              WHEN snapshotted_data.dbt_unique_key IS NULL THEN 'insert'
              ELSE 'update'
            END as dbt_change_type
        FROM source_data
        LEFT JOIN snapshotted_data
               ON snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        WHERE snapshotted_data.dbt_unique_key IS NULL
           OR (
                snapshotted_data.dbt_unique_key IS NOT NULL
            AND (
                {{ strategy.row_changed }}
            )
        )
    )
    {%- if strategy.invalidate_hard_deletes -%}
    {%- set target_columns = adapter.get_columns_in_relation(target_relation) -%}
    , deletes AS (
        SELECT
        {% for column in target_columns if not column.name == 'dbt_snapshot_at' %}
          {% if column.name == 'dbt_valid_from' %}
            {{ current_timestamp() }} AS dbt_valid_from {%- if not loop.last -%},{%- endif -%}
          {% elif column.name == 'dbt_change_type' %}
            'delete' AS dbt_change_type {%- if not loop.last -%},{%- endif -%}
          {% else %}
            snapshotted_data.{{ column.name }} {%- if not loop.last -%},{%- endif -%}
          {% endif %}
        {% endfor %}
        FROM snapshotted_data
        LEFT JOIN source_data
               ON snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        WHERE source_data.dbt_unique_key IS NULL
    )
    SELECT * FROM upserts
    UNION ALL
    SELECT * FROM deletes
    {% else %}
    SELECT * FROM upserts
    {% endif %}

{%- endmacro %}


{% macro athena__build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set tmp_identifier = target_relation.identifier ~ '__dbt_tmp' %}

    {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier,
                                                  schema=target_relation.schema,
                                                  database=none,
                                                  type='table') -%}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {# needs to be a non-temp view so that its columns can be ascertained via `describe` #}
    {% call statement('build_snapshot_staging_relation') %}
        {{ create_table_as(False, tmp_relation, select) }}
    {% endcall %}

    {% do return(tmp_relation) %}
{% endmacro %}

{% macro athena__post_snapshot(staging_relation) %}
    {% do adapter.drop_relation(staging_relation) %}
{% endmacro %}


{% materialization snapshot, adapter='athena' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}
  {%- set file_format = config.get('file_format', 'parquet') -%}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=none,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}


  {% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif %}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['compiled_sql']) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %}

      {{ adapter.valid_snapshot_target(target_relation) }}

      {% set staging_table = athena__build_snapshot_staging_table(strategy, sql, target_relation) %}

      {%- set target_columns = adapter.get_columns_in_relation(target_relation) -%}
      {%- set staging_columns = adapter.get_columns_in_relation(staging_table) -%}


      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      {% set final_sql = snapshot_merge_sql(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns
         )
      %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
