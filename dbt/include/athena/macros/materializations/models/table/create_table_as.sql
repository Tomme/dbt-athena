{% macro athena__create_table_as(temporary, relation, sql) -%}
  {%- set split_model_path = model.path.split('/') -%}
  {%- set domain_name = split_model_path[0] -%}
  {%- set database_name = split_model_path[1] -%}
  {%- set file_name = split_model_path[-1].split('.')[0] -%}
  {%- set table_name = file_name.split('__')[-1] -%}
  {%- set extraction_timestamp = run_started_at.strftime("%Y-%m-%d %H:%M:%S") -%}
  {%-
    set default_external_location = 's3://mojap-derived-tables/domain_name='
    + domain_name + '/database_name=' + database_name + '/table_name=' + table_name
    + '/extraction_timestamp=' + extraction_timestamp + '/'
  -%}
  {%-
    set external_location = config.get(
      'external_location', default=default_external_location
    )
  -%}
  {%- set partitioned_by = config.get('partitioned_by', default=none) -%}
  {%- set bucketed_by = config.get('bucketed_by', default=none) -%}
  {%- set bucket_count = config.get('bucket_count', default=none) -%}
  {%- set field_delimiter = config.get('field_delimiter', default=none) -%}
  {%- set format = config.get('format', default='parquet') -%}

  create table
    {{ relation }}

    with (
      {%- if external_location is not none and not temporary %}
        external_location='{{ external_location }}',
      {%- endif %}
      {%- if partitioned_by is not none %}
        partitioned_by=ARRAY{{ partitioned_by | tojson | replace('\"', '\'') }},
      {%- endif %}
      {%- if bucketed_by is not none %}
        bucketed_by=ARRAY{{ bucketed_by | tojson | replace('\"', '\'') }},
      {%- endif %}
      {%- if bucket_count is not none %}
        bucket_count={{ bucket_count }},
      {%- endif %}
      {%- if field_delimiter is not none %}
        field_delimiter='{{ field_delimiter }}',
      {%- endif %}
        format='{{ format }}'
    )
  as
    {{ sql }}
{% endmacro %}
