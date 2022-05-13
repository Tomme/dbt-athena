{% macro default__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {% set sql = "" %}
    -- No truncate in Athena so always drop CSV table and recreate
    {{ adapter.drop_relation(old_relation) }}
    {% set sql = create_csv_table(model, agate_table) %}

    {{ return(sql) }}
{% endmacro %}

{% macro athena__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}

  {%- set split_model_path = model.path.split('/') -%}
  {%- set domain_name = split_model_path[0] -%}
  {%- set database_name = split_model_path[1] -%}
  {%- set env_name = "dev" if database_name.endswith("_dev") else "prod" -%}
  {%- set file_name = split_model_path[-1].split('.')[0] -%}
  {%- set table_name = file_name.split('__')[-1] -%}
  {%- set run_time = run_started_at.strftime("%Y-%m-%d %H:%M:%S") -%}
  {%-
    set default_external_location = adapter.generate_s3_data_path(
      env_name,
      domain_name,
      database_name,
      table_name,
      run_time
    )
  -%}

  {% set sql %}
    create external table {{ this.render() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {% endif -%}
        {%- endfor -%}
    )
    stored as parquet
    location '{{ default_external_location }}'
    tblproperties ('classification'='parquet')
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}
