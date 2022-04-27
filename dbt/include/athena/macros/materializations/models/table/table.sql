{% materialization table, adapter='athena' -%}
  {%- set identifier = model['alias'] -%}

  {{ run_hooks(pre_hooks) }}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {% if var('table_zero_downtime', false) or 'table_zero_downtime' in config.get('tags') %}
    {%- set current_ts = (modules.datetime.datetime.utcnow() - modules.datetime.datetime.utcfromtimestamp(0)).total_seconds() * 1000 -%}
    {%- set ctas_id_str = "ctas_{0}_{1}".format(identifier, current_ts) -%}
    {%- set ctas_id = ctas_id_str[0:ctas_id_str.index('.')] -%}

    {%- set target_relation = api.Relation.create(identifier=ctas_id,
                                                schema=schema,
                                                database=database,
                                                type='table', alias=identifier, ctas_id=ctas_id, model=model, graph=graph) -%}
  {%- else -%}
    {%- if old_relation is not none -%}
      {{ adapter.drop_relation(old_relation) }}
    {%- endif -%}
    {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}
  {%- endif -%}

  -- build model
  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {% endcall -%}

  -- set table properties
  {{ set_table_classification(target_relation, 'parquet') }}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
